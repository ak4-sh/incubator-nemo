# Sponge repeated scale-out failure on CloudLab (historical)

> **Historical lifecycle report; annotated 2026-09-08.** This report describes the
> earlier missing-invocation defect and its proposed correction. Its cluster-state,
> uncompiled-source, and "CloudLab is the only cause" statements are not current.
> The correction has since run on c6525, including a scoped two-reactivation pass;
> a later Q6 run failed for additional reasons involving merger migration and
> unsupported worker-to-worker connections. The optional deferred guard and the
> unconditional invocation correction must be reported separately. AWS invocation
> code remains present, but AWS execution has not been validated by these runs.
>
> Use [reactivation status](DEFERRED_WORKER_REACTIVATION.md), the
> [migration re-investigation](SPONGE_Q6_MIGRATION_REINVESTIGATION_20260907.md), and
> the [archive outcome index](results/cloudlab/SINE_REACTIVATION_ARCHIVE_20260908.md)
> for current conclusions. The body is retained as investigation history, not a
> current runbook or certification of native R3 autoscaling. The actual backend
> flag is `-offloading_type`, not the camel-case spelling in historical examples.

## Purpose

This report explains:

- how Sponge's scale-out path works;
- why the first scale-out appeared healthy;
- why a later scale-out/reconfiguration stalled;
- why the failure is specific to the CloudLab VM emulation path;
- why the original AWS Lambda path does not have the same lifecycle defect;
- what must be fixed and validated before using repeated-scale results.

## Executive summary

- Sponge treats an offload worker as a **reusable logical worker**, but each active period is a separate **Lambda invocation**.
- Every activation must start an invocation that:
  1. acknowledges activation;
  2. processes assigned work;
  3. waits for `END`;
  4. acknowledges `END`; and
  5. returns.
- The original AWS backend starts a fresh invocation on every activation with `awsLambda.invokeAsync(...)`.
- The pre-fix CloudLab backend keeps a persistent `VMWorker` process, but only the initial `SEND_ADDRESS` request starts `OffloadingHandler.handleRequest()`.
- Pre-fix CloudLab activations send a direct `ACTIVATE` message. This can acknowledge activation and allow processing, but it does **not** start a new `handleRequest()` invocation waiting for the next `END`.
- Consequently, the first scale-out can process data successfully. The defect becomes visible only when Sponge later stops/deactivates and reuses the same worker.
- During the failed repeated-scale run, `END` was sent but never acknowledged. The master remained in `DEACTIVATING`, while the stopped task was made `READY` and selected again.
- Without the deferred-reactivation patch, that overlap throws an illegal state-transition exception. With the patch, reactivation waits safely, but it waits forever because the missing CloudLab invocation never acknowledges `END`.
- This is not a Nexmark, Kafka, producer, network-throughput, or HoloStream problem. It is a lifecycle mismatch in the `cloudlab-vm` offloading adapter.

## Experiment context

The objective is to compare equivalent Nexmark Query 6 pipelines in Sponge and HoloStream while using:

- the same HoloStream Kafka producer and encoded records;
- equivalent source and target-operator parallelism;
- the same CloudLab hardware allocation;
- Sponge's native queue-delay scaler and graph-rewriting path.

The repeated-scale failure was observed in the Sponge scaled sine-wave experiment:

- baseline input: approximately **164K events/s**;
- peak input: **900K events/s**;
- produced records: **61,379,940**;
- Kafka source tasks: **8**;
- baseline target compute tasks: **4**;
- offload pool: **108 one-core VM workers** across four physical hosts.

Production completed and Kafka consumer lag reached zero, but the run was rejected because four Stage 2 tasks did not complete the repeated offload-worker lifecycle.

## Implementation status

- The CloudLab lifecycle correction described later in this report is now implemented in source.
- `WorkerControlProxy` delegates activation only to the selected backend.
- The CloudLab backend sends `SEND_ADDRESS` for initial creation and every reuse.
- `VMWorker` records the start and finish of each handler invocation.
- Lifecycle validation now checks invocation, activation, `END`, and deferred-reactivation pairs.
- The source has **not yet been compiled or cluster-tested** because cluster access is currently unavailable.

## Terminology

The following distinction is essential:

- **Logical offload worker:** Sponge's long-lived master-side identity, such as `Lambda-3` or `VM-3`.
- **Worker process/container:** the environment that can remain warm and retain initialized objects or network connections.
- **Active invocation:** one execution of `OffloadingHandler.handleRequest()` inside that environment.

AWS may reuse a warm Lambda container, but every `invokeAsync` call still enters `handleRequest()` again. The container can persist while the previous invocation has already returned.

CloudLab similarly keeps a persistent Java process. To emulate Lambda correctly, that process must start a fresh `handleRequest()` execution for every logical activation. Keeping the process alive is not, by itself, equivalent to having an active invocation.

## Sponge worker state machine

The intended whole-worker state sequence is:

```text
DEACTIVATE
    -> activation request
ACTIVATING
    -> worker ACTIVATE acknowledgement
ACTIVATE
    -> master sends END
DEACTIVATING
    -> worker END acknowledgement
DEACTIVATE
```

The important rule is that the master must not begin another activation while the worker is still `DEACTIVATING`. It first needs the previous invocation's `END` acknowledgement.

The whole-worker state is separate from task state. A persistent worker can exist while deactivated, and stopped tasks can be returned to `READY` for rescheduling.

## End-to-end scale-out process

### 1. The scaler observes source pressure

`InputAndCpuBasedScaler` runs once per second after its initial delay. In the active code path it estimates source queue delay as:

```text
estimated queue = cumulative reported input - cumulative source-processed events
processing rate = mean of recent source-processing deltas
estimated queue delay = estimated queue / processing rate
```

When estimated delay exceeds `scaler_trigger_queue_delay`—2 seconds in our experiments—and the cooldown and previous-migration checks pass, the scaler calculates a migration ratio and calls `ScaleInOutManager.sendMigrationAllStages(...)`.

This is Sponge's internal estimated input delay. It is distinct from the Kafka record residence-time metric used in our result plots.

Relevant code:

- `runtime/master/.../scaler/InputAndCpuBasedScaler.java`
  - `queueSizeBasedScalingRatio()`
  - `scalingWithRatio()`

### 2. Sponge selects tasks to offload

`ScaleInOutManager` selects tasks from baseline compute executors according to the calculated ratio. It asks the scheduler to stop/migrate those tasks toward the `LAMBDA` resource type and records their IDs in `prevSelectedTasksToMoveLambda`.

Relevant code:

- `runtime/master/.../ScaleInOutManager.java`
  - `sendMigrationAllStages()`

### 3. The scheduler activates offload workers

When an offload worker is required, `WorkerControlProxy.activate()` transitions it from `DEACTIVATE` to `ACTIVATING` and invokes the backend-specific `LambdaActivator`.

The worker must send an `ACTIVATE` acknowledgement before it is considered active and before activation-gated task delivery is released.

### 4. Tasks execute on the offload workers

After task installation and activation, records are processed by the offloaded task. In the CloudLab adapter, the worker's persistent control/data channels can continue functioning even if no new `handleRequest()` invocation was started. This is why activation and data processing alone did not reveal the defect.

### 5. A later scaling decision revisits the migrated tasks

On a subsequent scaling call, `ScaleInOutManager` first checks `prevSelectedTasksToMoveLambda`. If it is non-empty, Sponge issues `stopTask()` for those previously selected tasks and returns from that migration call.

This means the later decision is not simply "add more offload workers." It first stops and reconfigures the tasks selected during the previous decision.

### 6. Stopped tasks are returned to the scheduler

When the runtime receives `StopTaskDone`, it:

1. removes the task from the worker's running set;
2. invokes `checkAndDeactivate()` for an offload worker;
3. removes the task from the scheduled-task map;
4. changes the task state back to `READY`; and
5. places it in the pending collection for immediate rescheduling.

Relevant code:

- `runtime/master/.../RuntimeMaster.java`, `StopTaskDone` handling
- `runtime/master/.../DefaultExecutorRepresenterImpl.java`, `onTaskExecutionStop()`

### 7. An empty active worker is deactivated

Under the R2 policy, `checkAndDeactivate()` sends whole-worker `END` when the worker has no:

- activated tasks;
- pending task activations;
- tasks being stopped; or
- relevant running tasks.

The worker transitions to `DEACTIVATING` until its invocation acknowledges `END`.

Because the stopped task is already `READY`, the scheduler may select the same logical worker again. Correct behavior therefore depends on finishing the old invocation before starting the next one.

## Expected AWS Lambda lifecycle

The original AWS requester performs the following lifecycle.

### Initial registration/warm-up

```text
master createRequest()
    -> awsLambda.invokeAsync(request)
    -> invocation A enters handleRequest()
    -> invocation A sends ACTIVATE
    -> master sends initial END to leave a warm, inactive worker
    -> invocation A sends END acknowledgement
    -> invocation A returns
```

### First operational scale-out

```text
master activate()
    -> awsLambda.invokeAsync(request)
    -> invocation B enters handleRequest()
    -> invocation B sends ACTIVATE
    -> tasks execute
    -> invocation B waits for END
```

### Later stop and reuse

```text
master sends END
    -> invocation B consumes END
    -> invocation B acknowledges END and returns

master activates logical worker again
    -> awsLambda.invokeAsync(request)
    -> invocation C enters handleRequest()
    -> invocation C sends ACTIVATE and waits for its own END
```

The logical worker and warm container may be reused, but invocations B and C are separate executions. Every active period has a thread blocked on `endBlockingQueue.take()` and therefore has something capable of acknowledging its corresponding `END`.

Relevant code:

- `LambdaAWSResourceRequester.createRequest()` invokes Lambda initially.
- The returned `LambdaActivator.activate()` also calls `awsLambda.invokeAsync(...)` on every reuse.
- `OffloadingHandler.handleRequest()` waits on `endBlockingQueue.take()`, acknowledges `END`, and returns.

## Pre-fix CloudLab VM lifecycle

The CloudLab adapter performs a different sequence.

### Initial registration/warm-up

```text
master createRequest()
    -> connect to persistent VMWorker
    -> send SEND_ADDRESS
    -> VMWorker submits handleRequest(): invocation A
    -> invocation A sends ACTIVATE
    -> master sends initial END
    -> invocation A acknowledges END and returns
```

This first lifecycle is valid because `SEND_ADDRESS` actually caused `VMWorker` to submit `handleRequest()`.

### First operational scale-out

```text
master activate()
    -> shared WorkerControlProxy sends direct ACTIVATE
    -> CloudLab LambdaActivator.activate() does nothing
    -> persistent worker echoes/handles ACTIVATE
    -> tasks can execute through persistent channels
    -> no invocation B was started
```

At this point the master believes the worker is active, and data processing can appear normal. However, there is no invocation B blocked in `handleRequest()` waiting for the next `END`.

### Later stop/deactivation

```text
master sends END
    -> worker control handler places an END token in endBlockingQueue
    -> no handleRequest() invocation is waiting to consume it
    -> no END acknowledgement is sent to the master
    -> master remains in DEACTIVATING
```

The CloudLab `LambdaActivator.activate()` is empty. The direct `ACTIVATE` shortcut therefore acknowledges a state transition without recreating the Lambda invocation whose lifetime that state is supposed to represent.

Relevant code:

- `CloudLabVMLambdaResourceRequester.createRequest()` sends `SEND_ADDRESS` only during initial creation.
- Its returned `LambdaActivator.activate()` is empty.
- `VMWorker.start()` submits `OffloadingHandler.handleRequest()` only for `SEND_ADDRESS`.
- The CloudLab port added a direct `ACTIVATE` message in shared `WorkerControlProxy` logic.

## Why single-scale-out tests appeared successful

A single scale-out test exercised only the visible first half of the broken lifecycle:

1. The initial warm-up invocation existed and completed its initial `END` normally.
2. The first operational scale-out sent direct `ACTIVATE`.
3. The persistent CloudLab process and existing channels acknowledged activation.
4. Tasks were installed and processed records.
5. The run either ended or was externally cleaned up before Sponge needed a complete **deactivate then reactivate** cycle for those workers.

Therefore, a successful first activation demonstrated:

- worker connectivity;
- task delivery;
- data processing; and
- the activation acknowledgement path.

It did **not** demonstrate:

- that a post-scale-out `END` could be consumed;
- that the master would receive the corresponding `END` acknowledgement;
- that the worker could return to `DEACTIVATE`; or
- that another fresh invocation could be started for reuse.

This is why the earlier step workloads, which produced only one scaler decision during their useful measurement interval, could appear healthy. A repeated-scale sine workload exercised the missing second half of the lifecycle.

## What happened in the failed repeated-scale run

The observed sequence was:

1. The first native scale-out activated offload workers and moved Stage 2 tasks successfully.
2. A later native scaler decision revisited the tasks saved in `prevSelectedTasksToMoveLambda`.
3. Sponge stopped those offloaded tasks.
4. The workers became empty, so the master sent `END` and marked them `DEACTIVATING`.
5. `RuntimeMaster` changed the stopped tasks back to `READY` and made them available for immediate scheduling.
6. The scheduler selected the same logical workers while their old state was still `DEACTIVATING`.

Two different master-side behaviors expose the same underlying CloudLab defect:

- **Original behavior:** `activate()` while `DEACTIVATING` throws an illegal state-transition exception.
- **Deferred-reactivation behavior:** activation is recorded as `DEFERRED` until the previous `END` acknowledgement arrives.

In the validation run, four transitions reached `DEFERRED` but never reached `DEFERRED_RELEASED`. The relevant `END` acknowledgement never arrived because the CloudLab worker had not started a new invocation during its prior activation. Subsequent worker logs reported missing Stage 2 task executors.

Kafka lag still reached zero because the Kafka source had consumed all records. That did not prove that downstream stateful processing or worker lifecycles completed correctly.

## Why this is specific to the CloudLab VM path

Within the current Sponge experiment code, this exact defect is confined to `-offloadingType cloudlab-vm`:

| Component | Activation mechanism | Fresh `handleRequest()` per activation? | Can later `END` be consumed? |
|---|---|---:|---:|
| AWS Lambda backend | `awsLambda.invokeAsync(...)` | Yes | Yes |
| Pre-fix CloudLab VM backend | Direct `ACTIVATE`; empty backend activator | No | No, after the initial invocation returns |
| Corrected CloudLab VM backend | Backend sends `SEND_ADDRESS` on every activation | Yes | Yes, pending build/run validation |
| Baseline YARN compute | Long-running executor; not Lambda invocation lifecycle | Not applicable | Not applicable |
| HoloStream workers | HoloStream's own scaling/migration model | Not applicable | Not applicable |

The problem is not that CloudLab VMs are persistent. A persistent process can emulate Lambda correctly. The problem is that reuse of that process does not submit a fresh invocation of `handleRequest()`.

It is also not primarily a slow-network race. Slower communication can widen the period in which reassignment overlaps with `DEACTIVATING`, but even with instantaneous networking, the current CloudLab path has no invocation waiting to consume the second `END`. The missing invocation is deterministic lifecycle behavior.

Other persistent-worker backends would have the same risk only if they used the same direct-activation shortcut without recreating an invocation. The original AWS backend does not use that shortcut as its activation mechanism.

## Change attribution

### Original Sponge

- `WorkerControlProxy.activate()` calls the backend `LambdaActivator`.
- AWS `LambdaActivator.activate()` invokes Lambda again.
- The state machine assumes each activation creates an invocation that will eventually acknowledge `END`.

### Earlier CloudLab port

The CloudLab port introduced the lifecycle mismatch by combining:

- a direct `ACTIVATE` message in shared worker-control code; and
- an empty `CloudLabVMLambdaResourceRequester.LambdaActivator.activate()`.

This was sufficient for first activation and processing, but not for repeated invocation lifecycles.

### Deferred-reactivation patch

The later patch did not create the missing lifecycle. It:

- prevents an immediate crash when activation is requested during `DEACTIVATING`;
- coalesces duplicate activation requests;
- waits for `END` before releasing the deferred activation; and
- gates task delivery on activation acknowledgement.

The patch exposed the deeper CloudLab issue because its correct wait could never finish without an `END` acknowledgement. It should not be described as the root cause.

## Implemented fix for hardware-matched CloudLab experiments

The source now reproduces the original Lambda invocation boundary without changing Sponge's scaler or migration policy:

1. `WorkerControlProxy` requests activation through `LambdaActivator` rather than relying on a global direct `ACTIVATE` shortcut.
2. `CloudLabVMLambdaResourceRequester.LambdaActivator.activate()` sends a new `SEND_ADDRESS` request for every reuse, causing `VMWorker` to submit `OffloadingHandler.handleRequest()` again.
3. The obsolete direct `ACTIVATE` handler was removed, leaving exactly one mechanism to produce the `ACTIVATE` acknowledgement.
4. Preserve the original lifecycle contract: one activation, one invocation, one `END`, one `END` acknowledgement, then return.
5. Retain deferred reactivation only as protection for legitimate overlap between asynchronous deactivation and scheduler reassignment.
6. Keep all CloudLab-specific behavior behind `-offloadingType cloudlab-vm`; do not change the AWS Lambda path.

This fix changes the CloudLab **emulator**, not the queue-delay threshold, task-selection ratio, R1/R2/R3 graph rewriting, Nexmark query, or HoloStream implementation.

## Alternative: use real AWS Lambda

Running with the original AWS backend avoids this specific emulation defect because every activation invokes Lambda again. However, it requires:

- deployed Lambda functions and artifacts;
- IAM permissions and AWS credentials;
- sufficient Lambda concurrency quota;
- compatible region and networking configuration; and
- reachability between Lambda and the Sponge control/data services.

It also changes the infrastructure comparison: Sponge would use AWS Lambda while HoloStream currently runs on CloudLab. That is appropriate for reproducing Sponge's original deployment model, but it is not a strictly hardware-matched systems comparison.

## Required validation before accepting the CloudLab fix

A successful first scale-out is no longer sufficient. Validation must cover at least two complete worker lifecycles:

```text
activation 1 -> ACTIVATE ACK -> task work -> END -> END ACK
activation 2 -> ACTIVATE ACK -> task work -> END -> END ACK
```

The test should verify:

- one invocation starts for every activation;
- every `ACTIVATE` has exactly one acknowledgement;
- every `END` has exactly one acknowledgement;
- every `DEFERRED` transition reaches `DEFERRED_RELEASED`;
- every activation-gated task is eventually released;
- no task is sent while its worker is `ACTIVATING` or `DEACTIVATING`;
- no `No task executor Stage2-*` errors occur;
- the YARN application remains healthy;
- Kafka terminal offsets and zero consumer lag are reached;
- downstream completion/terminal conditions are reached, not merely source completion.

The full sine workload should be rerun only after a focused two-cycle smoke test passes.

## Scientific interpretation

- Results from the failed repeated-scale run must not be used in performance graphs.
- Successful earlier single-scale results remain evidence that first activation and offloaded processing worked, but not evidence that repeated worker reuse worked.
- A corrected CloudLab-emulation run should be labelled clearly as using a CloudLab Lambda-lifecycle emulator.
- The fix should be reported as deployment compatibility, while confirming that Sponge's scaling decision and graph-rewriting logic remain unchanged.

## Supervisor-ready summary

> Sponge's original design reuses a logical worker but starts a fresh AWS Lambda invocation for every active period. Each invocation acknowledges activation, processes tasks, waits for `END`, acknowledges it, and returns. Our CloudLab port kept a persistent VM process and could acknowledge the first operational activation, so single-scale-out tests processed data successfully. However, reuse did not start a fresh `handleRequest()` invocation. When a later scaler decision stopped the offloaded tasks and sent `END`, no invocation was waiting to consume and acknowledge it. The worker remained `DEACTIVATING` while the scheduler made the task ready and attempted to reuse the worker. This is why the issue appears only in repeated-scale tests and only in the CloudLab VM adapter. The correct hardware-matched fix is to make each CloudLab activation start a new handler invocation, preserving the same activation/END contract as AWS without changing Sponge's scaler or query.

## Relevant source files

- `runtime/master/src/main/java/org/apache/nemo/runtime/master/scaler/InputAndCpuBasedScaler.java`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/ScaleInOutManager.java`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/RuntimeMaster.java`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/DefaultExecutorRepresenterImpl.java`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/WorkerControlProxy.java`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/lambda/LambdaAWSResourceRequester.java`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/lambda/CloudLabVMLambdaResourceRequester.java`
- `runtime/lambda-executor/src/main/java/org/apache/nemo/runtime/lambdaexecutor/OffloadingHandler.java`
- `offloading/workers/vm/src/main/java/org/apache/nemo/offloading/workers/vm/VMWorker.java`
