# Sponge–HoloStream Experiment Status Brief (historical)

> **Superseded status; annotated 2026-09-08.** This briefing records the earlier
> 164K–900K experiment and missing-invocation investigation, not the current cluster
> state. The invocation correction has since been built and exercised on c6525:
> the earlier two-reactivation lifecycle check passed, while the later 100K–375K
> run failed overall validation and exposed a separate merger-migration problem.
> The historical claim that the lifecycle mismatch explains all Q6 failures is
> therefore too broad. No real-AWS success is established by these CloudLab runs.
>
> Current references: [reactivation status](DEFERRED_WORKER_REACTIVATION.md),
> [migration re-investigation](SPONGE_Q6_MIGRATION_REINVESTIGATION_20260907.md), and
> [archived outcomes](results/cloudlab/SINE_REACTIVATION_ARCHIVE_20260908.md).
> The invocation correction applies even with the deferred guard disabled; that
> flag alone does not restore the pre-fix backend. Historical launch examples below
> are not a current runbook; the actual backend flag is `-offloading_type`.

## Executive summary

- We successfully compiled and validated the modified Sponge code and launched the full scaled sine-wave workload on CloudLab.
- Input generation completed: **61,379,940 events** were written, and the Sponge Kafka consumer group reached **zero lag**.
- The run is nevertheless **not valid for evaluation** because four offloaded Stage 2 tasks remained blocked during a repeated scale-out cycle.
- The root cause is not the Nexmark query, Kafka adapter, or original AWS Lambda implementation. It is an incomplete lifecycle emulation in our earlier **CloudLab-specific Lambda adapter**.
- The new deferred-reactivation patch prevented the previous crash and exposed this deeper problem: CloudLab acknowledged worker activation without starting the new invocation that original Sponge receives from AWS Lambda.
- We stopped the invalid application, did not generate comparison plots from it, and did not run HoloStream against it.

## Experimental objective

- Compare Sponge and HoloStream using equivalent Nexmark Query 6 pipelines and identical Kafka input.
- Exercise each system's scale-out behavior under a Figure 9b-style sine workload.
- Use the same generated records and workload timing for Sponge, HoloStream memory, and HoloStream Pebble runs.
- Avoid attributing producer, encoding, or dataset differences to the streaming systems.

## Sponge test configuration

- Workload shape: scaled Figure 9b sine pattern.
- Baseline rate: approximately **164K events/s**.
- Peak rate: **900K events/s**.
- Total generated records: **61,379,940**.
- Kafka source parallelism: **8 tasks**.
- Baseline compute parallelism: **4 target-operator tasks**.
- Baseline compute executors: **4**, with strict host placement.
- CloudLab offload capacity: **108 single-core workers**.
  - Four physical offload hosts.
  - 27 worker processes per host.
- Safe worker reactivation: enabled for this validation run.
- Sponge scaler: armed from the start.

## Validation completed before the run

- Full Java 11 Maven build passed.
- Java 8 Kafka producer and telemetry helper compilation passed.
- Deferred-reactivation unit tests passed: **8 tests, 0 failures**.
- Packaged-artifact verification passed.
- No duplicate Beam classes were present in the assembled artifacts.
- Hardware and strict-placement preflight checks passed.
- Workload configuration and total-event calculation passed.
- Protected original Sponge files were checked against the selected baseline.

## What happened during the run

1. Sponge started normally and consumed the baseline workload.
2. The first scale-out completed successfully.
3. The workload later caused another scale transition involving the same four Stage 2 workers.
4. Those workers were still in `DEACTIVATING` when the scheduler selected them again.
5. The new safety logic correctly recorded four `DEFERRED` reactivations instead of throwing the previous state-transition exception.
6. None of those transitions reached `DEFERRED_RELEASED`.
7. The affected VM-worker logs subsequently emitted repeated `No task executor Stage2-*` errors.
8. Kafka lag reached zero because all source records had been consumed, but the downstream graph was not healthy or complete.

Therefore, **zero Kafka lag was not sufficient evidence of a successful run**.

## Root cause

### Original Sponge behavior on AWS Lambda

Original Sponge invokes an AWS Lambda function every time an offload worker is activated or reused:

1. `LambdaAWSResourceRequester.activate()` calls `awsLambda.invokeAsync(...)`.
2. The Lambda invocation enters `OffloadingHandler.handleRequest()`.
3. The invocation waits for an `END` message.
4. It acknowledges `END` and returns.
5. A later activation creates a new invocation with a new thread waiting for the next `END`.

### CloudLab adaptation

Our earlier CloudLab port replaced the real Lambda invocation with a persistent VM process:

- It added a direct `ACTIVATE` message to the shared worker-control path.
- `CloudLabVMLambdaResourceRequester.activate()` was left empty.
- The direct message makes the VM worker echo an activation acknowledgement.
- It does **not** start a new `OffloadingHandler.handleRequest()` invocation.

The first worker invocation can acknowledge its first `END`. After that invocation returns, a direct `ACTIVATE` can make the persistent process accept work, but no invocation thread is waiting for the following `END`. The second deactivation therefore never completes.

## Was this caused by the latest deferred-reactivation patch?

**No—not at its root.**

- The incomplete CloudLab activation lifecycle was introduced by the earlier CloudLab backend port.
- The direct-activation shortcut and empty CloudLab activator already existed in the branch used as the base for the latest work.
- Previously, Sponge crashed when asked to activate a worker still marked `DEACTIVATING`.
- The new patch changes that crash into a deferred, acknowledgement-gated transition.
- That behavior exposed the fact that the CloudLab worker never supplies the required acknowledgement on repeated reuse.

The deferred-reactivation feature is configuration-gated and defaults to disabled, so the original behavior remains available when the feature is not selected.

## Does the original AWS implementation still exist?

Yes.

- `LambdaAWSResourceRequester` is still present.
- Initial invocation and reactivation still use `awsLambda.invokeAsync(...)`.
- AWS region and profile configuration remain present.
- `-offloadingType lambda` selects the AWS requester.
- `-offloadingType cloudlab-vm` selects the CloudLab VM emulator.
- `-ec2 true` affects network-address and CPU-normalization behavior; it does not by itself select Lambda.

Using real Lambda still requires deployed Lambda functions, IAM credentials, artifacts, concurrency quota, and network reachability. These are deployment requirements, not missing code.

## Resolution options

### Option A: Run Sponge on real AWS Lambda

- Restore the original activation path: each activation calls the AWS activator.
- Remove or backend-gate the direct CloudLab `ACTIVATE` shortcut.
- Run with `-offloadingType lambda`.
- This provides the deployment behavior closest to the original Sponge paper.

Trade-off:

- Sponge would run with AWS Lambda while HoloStream currently runs on CloudLab.
- That weakens a hardware-matched comparison unless HoloStream is also moved to comparable AWS infrastructure.

### Option B: Correct the CloudLab Lambda emulator

- Keep the original Sponge scheduler and AWS path unchanged.
- Make each CloudLab activation create a fresh VM-worker invocation, matching Lambda semantics.
- Ensure the CloudLab path emits exactly one `ACTIVATE` acknowledgement.
- Ensure every invocation waits for and acknowledges exactly one `END`.
- Keep this behavior isolated behind `-offloadingType cloudlab-vm`.

Trade-off:

- This requires a real change to the CloudLab emulation backend.
- It is not a change to the Nexmark query or scaling policy, but it must be validated carefully because it models Lambda lifecycle semantics.

## Recommended direction

- For a **hardware-matched Sponge versus HoloStream comparison**, use Option B.
- For a **faithful reproduction of the original Sponge deployment**, use Option A.
- Do not use the current 164K–900K result in graphs or performance claims.
- Add completion validation requiring:
  - every `DEFERRED` transition to have a matching `DEFERRED_RELEASED` transition;
  - every task waiting for activation to be released;
  - no VM-worker `No task executor` errors;
  - zero Kafka lag;
  - no driver or evaluator fatal errors.

## Current cluster and result status

- Invalid YARN application: stopped.
- Running YARN applications: none.
- Warm CloudLab VM workers: stopped on all four offload hosts.
- Kafka/HDFS and baseline cluster services: left intact.
- HoloStream memory run for this workload: not started.
- HoloStream Pebble run for this workload: not started.
- Comparison plots: intentionally not generated from the invalid Sponge run.

## Proposed next validation sequence

1. Select real Lambda or corrected CloudLab emulation based on the desired scientific comparison.
2. Implement only the selected backend-specific lifecycle correction.
3. Add a focused test covering two complete activate/deactivate cycles.
4. Recompile and run a short multi-cycle smoke test.
5. Confirm every deferred activation is released and no worker errors occur.
6. Repeat the full Sponge sine workload.
7. Download and inspect the Sponge metrics and plots.
8. Clean the cluster.
9. Run HoloStream memory with the identical workload.
10. Run HoloStream Pebble with the identical workload.
11. Generate the three-system comparison plots.

## Short verbal version

> The build, preflight checks, and Kafka production all succeeded, but we rejected the Sponge run because four offloaded tasks remained blocked during repeated worker reuse. The issue is in our CloudLab emulation of Lambda, not in the Nexmark query or the original AWS implementation. Original Sponge creates a new Lambda invocation for every reuse; our CloudLab shortcut acknowledges activation without creating that invocation, so the next deactivation cannot be acknowledged. The latest deferred-reactivation patch prevented the previous crash and made this lifecycle defect visible. We now need to decide whether to restore real AWS Lambda for deployment fidelity or correct the CloudLab emulator for a hardware-matched comparison. We did not run HoloStream or generate comparison plots from the invalid result.

## Decision requested from the supervisor

- Should the primary experiment prioritize:
  - original Sponge deployment fidelity using AWS Lambda; or
  - hardware matching using a corrected CloudLab Lambda emulator?
