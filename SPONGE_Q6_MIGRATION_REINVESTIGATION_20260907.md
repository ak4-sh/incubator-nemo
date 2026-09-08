# Sponge Q6 migration: re-investigation

Date: 2026-09-07. Scope: source inspection, Git history, the paper, and archived run logs. No compilation, cluster experiment, or runtime change was performed.

## Main finding

The failed run does move merger tasks to offload workers and then cannot establish the required worker-to-worker pipes. That diagnosis remains supported. However, describing the entire problem as a forgotten merger filter was incomplete: our launcher activates a whole-task migration controller on a graph prepared for R1/R2/R3 redirection. The public source also contains a separate redirection path that invokes the partial-state protocol. The failed run did not use that path.

Removing merger tasks from the migration list would prevent this particular placement, but would not by itself activate Q6's stateful transient operators. We must establish the intended controller-to-protocol integration before claiming to reproduce the paper's scaling behaviour.

## Evidence from the failed run

Run directory: `results/cloudlab/sponge-q6-c6525-repeated-scale-100k375k-20260828T191034Z`.

- Optimizer policy: `StreamingR1R2R3Policy`.
- Beam application: Query 6, using the HoloStream MUS Kafka source.
- Nemo `queryId`: 0.
- Backend: `cloudlab-vm`; `safeWorkerReactivation: true`; `ec2: true`.
- Four-way compute stages, eight physical Kafka source tasks.
- Compile-time plan: Stage5/11 are original partial-aggregation tasks; Stage6/12 are their transient counterparts; Stage7/13 are merger tasks initially assigned to Compute.
- At 19:24:58, `sendMigrationAllStages()` selected Stage2, Stage7, and Stage13 on all four compute executors.
- At 19:27:51, Stage7-2-0 was assigned to VM-123 and Stage13-2-0 to VM-106.
- At 19:27:53, the master received executing acknowledgements for both tasks. This was not simply a failure to start those workers.
- Existing transient worker VM-76 tried to reopen Stage6-2-0 -> Stage7-2-0. VM-108 tried to reopen Stage12-2-0 -> Stage13-2-0. Their new peers were also offload workers.
- Around 19:28:08, these pipe-open operations failed with `Cannot connect to task Stage7-2-0` and `Cannot connect to task Stage13-2-0`. Adjacent transient consumers also failed while reconnecting to these merger tasks.

Driver evidence: `remote_tmp_metrics/yarn_logs/driver.stderr`, especially lines 16508-16559 and 18270-18346.

Worker evidence: `remote_tmp_metrics/vmworker_logs/node7/vmworker-25334.log`, line 13495 onwards; `remote_tmp_metrics/vmworker_logs/node8/vmworker-25335.log`, line 7607 onwards.

### What was actually activated

Across all 124 archived worker logs:

- Transient Stage3, Stage6, Stage9, Stage12, and Stage15 report zero processed input in their captured metric samples (over 3,300 samples per stage).
- The driver records one `Waiting for scale out decision`, and no `End of waiting for scale out decision`.
- It records no `Redirection to lambda start`, `Activation lambda task`, `Send R3PairInputOutputStart`, or `Send R3OptSignalFinalCombine` messages.
- The master records 26 worker activation requests and 26 acknowledgements. These are process/invocation transitions, not evidence of the R3 task-redirection protocol.

Thus this run did not demonstrate successful Q6 partial-state offloading, nor two completed global scaler decisions. Kafka consumed all 150 million inputs, but that is not proof of downstream Q6 completion.

## The two control paths in the public source

### Path used by our launcher

`run_autoscaler_smoke.sh` writes `start-scaler`. `NemoDriver` calls `Scaler.start()`. The default `InputAndCpuBasedScaler` calls:

```text
queue-delay decision
-> sendMigrationAllStages(..., LAMBDA)
-> stopTask(task, LAMBDA)
-> ordinary StopTask control message
-> checkpoint / reschedule the same task identity
```

The candidate filter excludes routers, stream tasks, original VM partial-combine tasks, and certain descendants of mergers. It does not exclude the mergers themselves. It also replaces the supplied migration ratio with 1.0 for every selected stage.

Relevant files:

- `runtime/master/src/main/java/org/apache/nemo/runtime/master/scaler/InputAndCpuBasedScaler.java:156` and `:219`.
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/ScaleInOutManager.java:182`.
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/TaskScheduledMapMaster.java:162`.
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/DefaultExecutorRepresenterImpl.java:540`.

### Existing redirection path

The driver also accepts `redirection-r2`. Despite the name, this path reaches R3 for partial aggregations under the R1/R2/R3 policy:

```text
redirection-r2 command with original task stages/counts
-> RuntimeMaster.redirectionToLambda()
-> find each original task's pre-created transient partner
-> LambdaContainerManager.redirectionToLambda()
-> activateLambdaTask() / RoutingDataToLambda
-> R3_INVOKE_REDIRECTION_FOR_CR_BY_MASTER for partial-combine tasks
-> coordinate router, partial tasks, and merger
```

This path activates the partner tasks without requesting that the merger be rescheduled to Lambda. It exists in the original commit. Its successful execution on our adapter/backend has not been established by this inspection.

Relevant files:

- `runtime/driver/src/main/java/org/apache/nemo/driver/NemoDriver.java:315`.
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/RuntimeMaster.java:777`.
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/lambda/LambdaContainerManager.java:209`.
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/Executor.java:1233` and `:1257`.

There is no active use of `EvalConf.scalingType` to choose these paths: searches find its declaration, binding, construction, and logging only. Changing `-scaling_type` therefore does not switch the default scaler to redirection.

## What the original commit establishes

`ScaleInOutManager`, `TaskScheduledMapMaster`, `InputAndCpuBasedScaler`, `StagePartitioner`, and `StreamingR1R2R3Policy` have no diff from baseline `4c976182e` in this checkout.

The SHA-256 of `ScaleInOutManager.java` is identical in baseline and working tree:

`ff26978fcfa44addf7504ba58964a32d58196497883e954cf2eb8339cda1ebb8`.

The original `MergerTaskExecutorImpl` explicitly contains the comment `THIS TASK SHOULD NOT BE MOVED` at lines 115-117. The original affinity pass keeps mergers on Compute and stops traversal into them when marking transient operators for Lambda.

Git history also contains earlier commits named `merger migration` (`e02cb1c85`) and `move partial later and wait for merger first` (`d7894c753`). This shows experimentation with merger migration in the general migration machinery; it does not establish that the authors intended this combination of controller and R3 graph for the final paper. The ratio changed to a constant 1.0 during `4beb69cba` when the migration API changed from a single ratio to a list.

Consequently, the observable incompatibility is established; describing its origin as an accidental forgotten line, or asserting that the paper used an unreleased version, goes beyond the evidence.

## Other possibilities and their status

| Possibility | Finding |
| --- | --- |
| Wrong original runtime control path for the selected optimization policy | Strong additional explanation. The run invokes migration; the separate R3 redirection route is present but was not invoked. This may be a reproduction/configuration mismatch involving legacy or experimental code. |
| Incorrect Q6 stage partitioning | Real configuration difference: Nemo queryId is 0. Its Q6-specific checks only split at vertex14/15; in this run these are union-tagging transforms. Setting queryId does not alter the migration filter or select the redirection route. It needs a plan comparison, not an assumption that it fixes the crash. |
| HoloStream-specific decode transforms broke Q6 | The MUS decode already happens once in the Kafka source. The two downstream transforms unwrap Kafka metadata/key. Standard Beam Kafka input in this same launcher has the same two wrapper operations. The difference from a direct synthetic source is not uniquely caused by HoloStream decoding. |
| CloudLab worker identity classification causes the blocked connection | Our port extends original `contains("Lambda")` checks to recognize `VM-*` workers. This is a real networking-path change, but it applies the original Lambda connection restriction to the emulated workers. It explains why these peers are rejected, not why merger stages were selected. Disabling the check would require a supported transport and would change the deployment model. |
| Slow network, delayed registration, or insufficient 15-second timeout | Possible sources of other pipe timeouts, but weak as the sole explanation here: both destinations were acknowledged as executing, and required connections are explicitly disallowed. Raising the timeout cannot make those connections available. |
| Long state transfer or stop/drain delay | A separate problem is visible. Stage7-2/13-2 took 173 seconds to stop; input acknowledgements arrived after about 106 seconds. Their merger checkpoints then took about 54 ms and encoded only 68 bytes each. The long delay precedes serialization and requires a separate control/drain investigation. |
| Worker lifecycle fix remains the immediate blocker | Not supported for these two targets: their activation gates released and tasks started. All 144 logged END sends have acknowledgements and no DEFERRED transitions occurred. The lifecycle validator still reports invocation-log mismatches and insufficient reuse depth; this run cannot certify the full lifecycle fix. |
| Stale or mixed artifacts | No positive evidence. The archive records client and Nexmark hashes and the logs follow the examined code, but this review did not independently verify every deployed worker JAR against the source. |

## Paper comparison and remaining uncertainty

The paper's Sections 4.1, 4.2, and 4.5 describe transient tasks receiving redirected work and merger operators on VMs. Section 4.3 describes per-executor CPU/input observations and resource calculation. The inspected default scaler currently invokes only its queue-based decision function; its CPU-based ratio method is defined but not called from that decision loop. Being present in the `sponge` branch therefore does not establish that a controller is the exact paper evaluation controller.

The official artifact link points to the public `sponge` branch. The artifact evaluation table awards Sponge `AVAILABLE` only, not `FUNCTIONAL` or `REPRODUCED`. This does not establish that the paper is wrong or that its code never worked; it means that entry does not provide independent confirmation of this exact reproduction path.

Sources: [paper](https://www.usenix.org/conference/atc23/presentation/song), [public artifact](https://github.com/apache/incubator-nemo/tree/sponge), [artifact evaluation results](https://sysartifacts.github.io/atc2023/results). Local paper inspected: `/Users/akash/Downloads/atc23-song.pdf`, printed pages 304-307.

## Recommended next diagnostic

1. Preserve the current runtime and create an isolated diagnostic configuration.
2. Compare direct-source and HoloStream-Kafka Q6 plans with the same optimizer and Nemo query ID, matching operators and task roles rather than assuming vertex numbers correspond.
3. Exercise the existing `redirection-r2` route on a tiny known Q6 dataset with explicit original aggregation stages. Confirm that original/merger tasks remain on Compute, transient partial tasks process events, the R3 acknowledgements finish, and Q6 results remain correct.
4. Repeat return-to-VM and reactivation to test the existing protocol and CloudLab lifecycle together.
5. Separately trace the 106-second input-stop / 173-second total-stop delay with control logging and queue/drain counters.
6. Ask the authors which controller and commands connected the published policy to R3 redirection. Manual redirection can test the native protocol but cannot be presented as an autonomous scaling-policy reproduction.

Do not accept a merger-exclusion patch as sufficient evidence of full Q6 scaling: in this plan it leaves Stage2 as the migration candidate while Stage5/11 remain excluded, and there is no call that would activate their Stage6/12 transient partners.
