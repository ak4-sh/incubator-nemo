# Deferred worker reactivation compatibility fix

Updated: 2026-09-08. This document separates the worker-lifecycle changes from the
remaining Q6 migration problem. It supersedes the earlier statement that the
CloudLab invocation correction had not been compiled or cluster-tested.

## Scope

- Branch: `experiment/deferred-worker-reactivation`.
- Base: `experiment/original-sponge-holostream` at `8a3e0379f`.
- Pre-fix tag: `sponge-holostream-pre-deferred-reactivation`.
- Runtime flag: `-safe_worker_reactivation true`.
- Default flag value: `false`. This disables deferred reactivation and the stricter
  task-delivery activation gate; it does **not** undo the CloudLab invocation correction.
- Validation launcher: `scripts/cloudlab/run_deferred_reactivation_validation.sh`.
- c6525 launcher: `scripts/cloudlab/run_deferred_reactivation_c6525_stepwise.sh`.
- The lifecycle implementation and tests are committed as `212891779`; the harness
  changes as `6ed2c51ea`; experiment configurations as `c8a929845`; and curated
  historical evidence as `91d60912d`. These commits do not constitute a new run.
- See the [archive index](results/cloudlab/SINE_REACTIVATION_ARCHIVE_20260908.md)
  for scoped run classifications and the raw logs deliberately retained only locally.

## Failure addressed

- Sponge may make a second native scale-out decision while VM workers from the first migration are still deactivating.
- The scheduler can immediately assign a newly ready task to one of those workers.
- The original `WorkerControlProxy.activate()` accepts only `DEACTIVATE` and throws for `DEACTIVATING`.
- The pre-fix 164K–900K events/s sine run exposed this transition during repeated
  scaling. Whole-worker activation and scaler decisions must not be counted as
  evidence that Q6's R3 partial-state offloading completed successfully.

## Optional deferred-reactivation guard

```text
DEACTIVATING + activation request
    -> record one pending reactivation
    -> wait for worker END acknowledgement
    -> DEACTIVATE
    -> ACTIVATING (one backend activation request)
    -> wait for worker ACTIVATE acknowledgement
    -> ACTIVATE
    -> release the pending task
```

- Duplicate activation requests during `DEACTIVATING` are coalesced.
- No backend activation is issued before the `END` acknowledgement.
- A reassigned task is not transmitted while the worker is `ACTIVATING`.
- These guards are controlled by `-safe_worker_reactivation true` and are in the
  shared worker-control code, not exclusively in the CloudLab requester.
- Lifecycle events use the `SPONGE_WORKER_REACTIVATION` log prefix.
- The guard cannot repair a missing `END` acknowledgement; it waits for that
  acknowledgement rather than starting an overlapping invocation.

## Backend invocation correction, independent of the flag

- A persistent `VMWorker` represents a warm execution environment, not an active invocation.
- `WorkerControlProxy` delegates activation to the selected backend. The earlier
  direct `ACTIVATE` request/echo shortcut has been removed.
- The CloudLab activator sends a fresh `SEND_ADDRESS` request for initial creation
  and every later activation, submitting a new `OffloadingHandler.handleRequest()` invocation.
- Each handler invocation acknowledges activation, waits for one `END`, acknowledges it, and
  returns to the VMWorker handler pool.
- `VMWorker` emits `SPONGE_WORKER_INVOCATION` records when an invocation starts and finishes.
- The AWS activator retains its `awsLambda.invokeAsync(...)` call. The AWS requester
  also has a test-only constructor; no real-AWS validation is established by the
  CloudLab results below.
- The CloudLab invocation correction and removal of the shortcut apply even when
  `safe_worker_reactivation=false`. Disabling that flag is therefore **not** an
  exact replay of the pre-fix CloudLab backend; use the pre-fix tag for that comparison.

## Build and validation status

- The corrected invocation path has been built and exercised on the c6525 cluster:
  the archived 2026-08-28 runs contain fresh-invocation and reactivation logs.
- This documentation update does not perform a new build or test run. The archives
  do not independently certify every historical deployed JAR against the newly
  recorded source commits.
- Three focused Java test files cover deferred/coalesced activation, task-delivery
  gating, fresh CloudLab invocations, and AWS invocation calls. Their presence is
  not a claim that the current checkout has just passed the complete test suite.

### Earlier c6525 lifecycle check: passed within its scope

Archive: [deferred-reactivation cap124 run](results/cloudlab/sponge-q6-c6525-deferred-reactivation-cap124-20260828T174230Z/worker_lifecycle_validation.json).

- Lifecycle validator: `passed: true`; required reuse depth: two reactivations
  after initial warm-up for at least one worker.
- Workers 36, 37, 88, and 120 reached two reactivations.
- 36 activation requests matched 36 acknowledgements.
- Worker 88 recorded one `DEFERRED` and one `DEFERRED_RELEASED` transition.
- All 148 logged `END` sends had acknowledgements; the validator reported no errors.
- This supports the deferred-transition and invocation-reuse fix in that captured
  test. It is not an end-to-end Q6 correctness or paper-policy validation.

### Later 100K–375K c6525 run: failed overall validation

Archive: [repeated-scale lifecycle check](results/cloudlab/sponge-q6-c6525-repeated-scale-100k375k-20260828T191034Z/worker_lifecycle_validation.json)
and [final validation](results/cloudlab/sponge-q6-c6525-repeated-scale-100k375k-20260828T191034Z/final_validation.json).

- Lifecycle validator: `passed: false`; overall result: `validated: false`.
- Required reuse depth was three reactivations; maximum observed depth was one.
- Invocation-request/start sets or counts differed, including inconsistent records
  for workers 87 and 120. The cause of those discrepancies remains unresolved.
- 26 activation requests matched 26 acknowledgements; all 144 logged `END` sends
  had acknowledgements. No deferred transitions were recorded in this run.
- Kafka consumed all 150 million records and consumer lag reached zero. Neither
  that nor the reported YARN `RUNNING` state establishes downstream completion.
- The later log investigation found merger tasks moved to offload workers followed
  by pipe-open failures. This run does not establish two completed global scale-outs.

## Separate unresolved Q6 controller/protocol problem

- The harness uses `StreamingR1R2R3Policy` but sends `start-scaler`, activating the
  default controller's `sendMigrationAllStages()` whole-task migration path.
- That path selected merger Stage7/13, creating offload-worker-to-offload-worker
  connections unsupported by the current transport path. Both destination tasks
  had started; these failures are not explained by an unreleased activation gate.
- The public source separately provides `redirection-r2`, which reaches the R3
  partial-state protocol. The failed run did not invoke that route.
- Neither `safe_worker_reactivation` nor the unused controller-selection field
  `scaling_type` switches the default scaler onto that redirection path.
- The lifecycle patch does not fix this controller/protocol incompatibility.
  Simply excluding mergers from migration is also insufficient to demonstrate
  activation of Q6's transient partial aggregations.
- See the [2026-09-07 investigation](SPONGE_Q6_MIGRATION_REINVESTIGATION_20260907.md)
  for source references, timing evidence, and remaining alternative explanations.

## Deliberately unchanged

The lifecycle code changes do not alter:

- Queue-delay threshold.
- Decision slack period.
- Migration-ratio calculation.
- R1/R2/R3 graph-rewriting passes.
- Task/merger selection policy (activation-gated task delivery does change).
- Q6 operators or HoloStream event decoding.

Worker counts, hardware allocation, rates, and phase timing are separately recorded
in each experiment configuration; they did change between experiment batches.

## Remaining validation requirements

1. Preserve build/test reports and JAR hashes for the exact source revision under test.
2. Repeat the lifecycle check at the intended reuse depth with complete, run-scoped
   logs. Reconcile invocation identities/counts and every `END` pair; verify deferred
   requests and waiting tasks are released. Resolve the later archive's discrepancies.
3. Exercise the existing R3 redirection route on a small known Q6 dataset with
   verified stage roles. Confirm mergers remain on Compute, transient partial tasks
   process records, protocol acknowledgements finish, and Q6 outputs are correct.
4. Establish the intended automatic-controller-to-R3 integration before claiming
   a native autoscaling benchmark. A manually triggered redirection test validates
   the protocol, not the automatic scaling policy.
5. Then rerun baseline, single-scale, and repeated-scale workloads with explicit
   completion criteria for both global scaling operations and downstream processing.

The harness now checks application health and, when requested, lifecycle validation
in addition to Kafka offsets. These checks remain necessary but insufficient for Q6
correctness: inspect worker pipe errors, R3 progress, and downstream results as well.

## Reporting

- Runs using both changes must be labelled **Sponge with CloudLab invocation-lifecycle
  correction and deferred worker-reactivation compatibility fix**.
- Report the earlier lifecycle-check pass separately from the later failed Q6 run.
  Do not present either lifecycle counters or zero Kafka lag as proof of a successful
  repeated-scale Q6 benchmark.
- The failed pre-fix 164K–900K run remains archived as diagnostic evidence.
- Record the backend, flag, source revision, artifact hashes, and workload configuration
  with each result. Use the pre-fix tag, not only a disabled flag, for exact rollback.
