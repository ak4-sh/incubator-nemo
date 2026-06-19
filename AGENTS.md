Current Cluster Context
=======================

Topology
--------

- **node0**: control node, YARN ResourceManager, HDFS NameNode, build/scripts/log collection, job submission point.
- **node1, node2, node3**: Kafka brokers. Bootstrap servers: `node1:9092,node2:9092,node3:9092`.
- **node5, node9, node10, node11, node12**: baseline Sponge/Nemo workers, regular JVM executors, YARN NodeManagers should run only here.
- **node4, node6, node7, node8, node13**: JVM function pool / offloading workers for Lambda-like/serverless execution path. Do not run YARN NodeManagers here for baseline runs.

Filesystem Note
---------------
- `/users/akash01/hadoop` and `/users/akash01/incubator-nemo` are on **local filesystem** (`/dev/sda3`) on each node — NOT NFS.
- Config files edited on node0 are invisible to worker nodes until explicitly copied via `scp`.
- JARs built on node0 must be distributed (e.g., via HDFS or YARN distributed cache; the client uploads them automatically).

Current YARN State
------------------
- 5 NodeManager registrations on baseline workers: node5, node9, node10, node11, node12.
- NMs now register with **internal hostnames** (`node5-link-1` → `10.10.1.6`) instead of external FQDNs (`c220g2-011125.wisc.cloudlab.us` → `128.105.145.128`).
- NMs killed and restarted fresh with distributed config files.
- RM also restarted fresh.
- Latest clean restart after Q8 cap4 benchmark: `application_1781894495142_0001` was killed, local subscriber/producer/collector processes were stopped, warm VMWorker pools on offload nodes were killed, RM and all five baseline NMs were restarted, and YARN shows all five baseline NMs `RUNNING` with `0` containers and no active applications.
- `/users/akash01/hadoop/etc/hadoop/slaves` on all nodes contains only: `node5`, `node9`, `node10`, `node11`, `node12`.

Configuration Changes Applied
-----------------------------

### Hadoop/YARN Config (distributed to all baseline nodes via scp)

- `/users/akash01/hadoop/etc/hadoop/hadoop-env.sh`: Java 11, explicit Hadoop component homes:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/users/akash01/hadoop
export HADOOP_COMMON_HOME=/users/akash01/hadoop
export HADOOP_HDFS_HOME=/users/akash01/hadoop
export HADOOP_YARN_HOME=/users/akash01/hadoop
export HADOOP_MAPRED_HOME=/users/akash01/hadoop
```

- `/users/akash01/hadoop/etc/hadoop/yarn-env.sh`: sets `NM_HOST` env var and `YARN_NODEMANAGER_OPTS`:
```bash
export NM_HOST=$(hostname -s)
export YARN_NODEMANAGER_OPTS="-Dyarn.nodemanager.hostname=${NM_HOST} -DNM_HOST=${NM_HOST} $YARN_NODEMANAGER_OPTS"
```

- `/users/akash01/hadoop/etc/hadoop/yarn-site.xml`:
  - Explicit `yarn.application.classpath` with absolute paths (fixes missing YARN classes in AM).
  - `yarn.nodemanager.hostname` → `${NM_HOST}` (resolved from env var at startup; avoids circular reference that occurred with `${yarn.nodemanager.hostname}`).
  - Removed `yarn.nodemanager.admin-env` override (caused invalid `JAVA_HOME` expansion).
  - RM hostname set to `node0`.

- `/users/akash01/hadoop/etc/hadoop/slaves`: 5 baseline nodes only.

### Nemo Config

- `bin/run_nexmark.sh`: includes Beam vendor gRPC jar in client classpath.
- `/users/akash01/deps/beam-vendor-grpc-1_21_0-0.1.jar` → symlink to Maven local copy.
- Executor JSON configs in `configs/cloudlab/`:
  - `nemo-yarn-1source-5compute-small.json`: 1 Source + 5 Compute, `capacity: 1`.
  - `nemo-yarn-kafka-1source-8compute.json`: 1 Source + 8 Compute with Source `slot: 1` (insufficient for Q8 with 4 Kafka source tasks).
  - `nemo-yarn-kafka-1source-cap4-8compute.json`: 1 Source + 8 Compute with Source `slot: 4` (required for Q8 autoscaler runs).
  - `nemo-yarn-kafka-1source-5compute.json`, `nemo-yarn-kafka-1source-4compute.json`.

Issues Found And Resolved
-------------------------

### Java Mismatch
- YARN containers used Java 8 while Nemo jars compiled for Java 11. Fixed by switching Hadoop/YARN env to Java 11.

### Missing Beam gRPC Class
- `org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ProtocolMessageEnum` missing from classpath. Fixed by adding Beam vendor gRPC jar to launcher classpath.

### Missing Hadoop YARN Class in AM
- `AMRMClientAsync$CallbackHandler` not found. Fixed by setting explicit `yarn.application.classpath` in `yarn-site.xml`.

### Stale/Stuck Processes
- Stale NodeManagers on offload nodes, duplicate NM registrations. Fixed by terminating NMs everywhere, restarting RM, and starting NMs only on baseline workers.
- Stale `JobLauncher`/`REEFLauncher`/`run_q8_subscriber_yarn.sh` processes can survive killed/timed-out runs and cause confusing YARN state. Clean them up before restarting RM/NMs.
- Latest cleanup also removed stale `/tmp/source.log` from node0 before the next benchmark run.

### "No address VM-1" Error (New)
- **Root cause:** `CloudLabVMLambdaResourceRequester` creates executor IDs with `VM-` prefix (e.g., `VM-1`), but codebase checked only `executorId.contains("Lambda")`. When `DefaultByteTransportImpl.connectTo("VM-1")` is called, it invokes `nameResolver.lookup("VM-1")`, which fails because `LambdaByteTransport` never registers VM executor addresses with `NemoNameServer`.
- **Fix:** Added `RuntimeIdManager.isLambdaExecutorId(String)` helper that checks for both `"Lambda"` and `"VM-"` prefixes. Updated all 6 files:
  - `ExecutorChannelManagerMap.java` (2 checks)
  - `DefaultByteTransportImpl.java` (1 check)
  - `LambdaByteTransport.java` (1 check)
  - `DefaultExecutorRepresenterImpl.java` (3 checks)
  - `ContainerManager.java` (1 check)
  - `RuntimeMaster.java` (3 checks)

### Address Plane External IP (New)
- `NemoNameServer` and `LambdaContainerManager` use `localAddressProvider.getLocalAddress()` which returns external IP (e.g., `128.105.145.65`). Created `ShortHostnameLocalAddressProvider` that returns short hostname (e.g., `node0`) by stripping domain from `InetAddress.getLocalHost().getHostName()`.
- Bound in `JobLauncher.java` via `Configurations.merge()`.

### Hadoop YARN External Hostname (New)
- NodeManagers registered with external FQDN (`c220g2-011125.wisc.cloudlab.us`), causing RM to connect via external IP (`128.105.145.128`). Fixed by:
  - Adding `yarn.nodemanager.hostname` property to `yarn-site.xml` with value `${NM_HOST}`.
  - Setting `NM_HOST=$(hostname -s)` in `yarn-env.sh`.
  - Distributing both files to all 5 baseline nodes via `scp`.
  - After restart, NMs register as `node5-link-1:...` (internal hostname → `10.10.1.6`).

### Hadoop Config Distribution (New)
- `/users/akash01/hadoop` is local filesystem — config changes on node0 invisible to workers. All config file edits (`yarn-env.sh`, `yarn-site.xml`, `slaves`) must be explicitly `scp`'d to each baseline node.

### Q8 Migration Schedulability (New)
- **Root cause:** `ContainerTypeAwareSchedulingConstraint.testSchedulability()` required strict equality between task placement and executor container type. Scale-out via `sendMigrationAllStages()` tags stopped tasks with `ResourcePriorityProperty.LAMBDA`, but VM executors have `containerType = "VM"`, so `"VM".equals("Lambda")` was false. The scheduler rejected all 179 VM executors and left migrated Q8 tasks in the `couldNotSchedule` retry loop forever.
- **Fix:** `ContainerTypeAwareSchedulingConstraint.java` now allows VM executors to schedule Lambda-tagged tasks: `task placement == LAMBDA && executor container type == VM`.
- **Result:** Q8 with `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json` successfully migrated Stage1 and Stage2 tasks to VM executors. AM logs showed `Stage1-*` and `Stage2-*` scheduled to `VM-76`, `VM-77`, `VM-78`, `VM-79`, `VM-110`, `VM-111`, `VM-112`, and `VM-116`.
- **Important:** Q8 also requires Source `slot: 4`; the older `nemo-yarn-kafka-1source-8compute.json` has Source `slot: 1` and leaves `Stage0-1/2/3` unscheduled.

Current Nexmark Status
----------------------

### Q0 Direct Runs
- `application_1781740448420_0004` (10 events): processed all 10, killed manually (streaming).
- `application_1781742418407_0001` (50k events): processed all 50000, killed manually.
- Final app status: `State=KILLED`, `Final-State=KILLED`, `Progress=100%`.

### Kafka Q8 Streaming
- Current autoscaler Q8 runs use 12 tasks: 4 Source tasks + 4 Stage1 (transform) + 4 Stage2 (GBK final). The Source executor must expose 4 slots.
- Consumer: `auto.offset.reset=earliest`, `enable.auto.commit=false`, `group.id=` (empty).
- `run_q8_subscriber_yarn.sh` uses `StreamingPolicy` + `StreamingScheduler`.

#### Pado Streaming Scheduler Deadlock
- `StreamingScheduler` dispatches Stage2 tasks before Stage1 tasks. With fewer slots than total tasks, Stage2 fills all slots and Stage1 never dispatches.
- 3-compute (4 slots): 4/9 tasks dispatched, 5 stuck, zero input.
- 5-compute (6 slots): 6/9 tasks dispatched, 3 stuck, zero input.
- **8-compute with Source `slot: 4` (12 total task slots): current minimum viable Q8 autoscaler config** — all 4 Source, 4 Stage1, and 4 Stage2 tasks can fit.

#### Q8 Finite Window Output
- Stage2 consistently shows `output=0` even though Stage1 outputs are shuffled to Stage2. Root cause: unbounded Kafka source watermark does not advance, so streaming windows never close/flush.

### Kafka Q0 with Offloading
- Full bursty run (100 prefill + 100000 live = 100100 total) succeeded with 200 VMWorkers.
- Source and KAFKA results topic offsets both confirmed at 100100.

### Autoscaler Smoke Test (Latest)
- `run_q8_subscriber_yarn.sh` now launches the Java subscriber with `nohup ... > "$LOG_FILE" 2>&1 &` and writes `/tmp/nemo-subscriber-${TOPIC}.pid`, so the harness returns after writing `scaling.txt` and producing live events.
- `run_autoscaler_smoke.sh` default total events is now 500 (100 prefill + 400 live) and reads the subscriber PID file.
- `run_autoscaler_smoke.sh` no longer starts the redundant background `source.log` sync loop. The client reads `source.log` locally on node0 and sends input updates to the AM via RPC, so syncing the file to the AM host is unnecessary.
- `run_autoscaler_smoke.sh` now clears AM-side fallback metric files (`/tmp/scaling_decisions.csv`, `/tmp/source_metrics.csv`, `/tmp/task_metrics.csv`) on the captured AM host after the YARN app reaches RUNNING and before metrics collection starts.
- Latest Q8 autoscaling rerun with wrong Source capacity: topic `nexmark-auto-133500`, app `application_1781894049447_0001`.
  - Command used `QUERY=8 EXECUTOR_JSON=configs/cloudlab/nemo-yarn-kafka-1source-8compute.json` with 8M events, 8 Kafka partitions, and producer parallelism 8.
  - Producer sent all `8,000,000` events, but this config has only Source `slot: 1`; Q8 had four Source tasks, so `Stage0-1/2/3` never scheduled.
  - Scale-out was blocked at the `ScaleInOutManager` wait-for-all-tasks scheduling gate. The app was killed and rerun with the cap4 config.
- Latest Q8 autoscaling rerun with Source capacity 4: topic `nexmark-auto-134204`, app `application_1781894495142_0001`, AM host `node12`.
  - Command used: `TOTAL_EVENTS=8000000 PREFILL_EVENTS=100 FIRST_RATE=50000 NEXT_RATE=200000 RATE_PERIOD_SEC=50 CPU_DELAY_MS=2 QUERY=8 EXECUTOR_JSON=configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json BASELINE_NODES='node5 node9 node10 node11 node12' EXPECTED_NM_COUNT=5 KAFKA_PARTITIONS=8 PRODUCER_PARALLELISM=8 scripts/cloudlab/run_autoscaler_smoke.sh`.
  - Producer sent `7,999,900` live records plus 100 prefill = `8,000,000` total at ~74.0K ev/s average.
  - Kafka offsets confirmed `8,000,000` across 8 partitions.
  - Scale-out triggered once: `1781894710766,SCALE_OUT,0.0648,146500.0000,50884.6000,44977.0000,44977,0.7621,179`.
  - VM task migration succeeded. AM logs scheduled Stage1/Stage2 tasks to `VM-76`, `VM-77`, `VM-78`, `VM-79`, `VM-110`, `VM-111`, `VM-112`, and `VM-116`.
  - Offload node task metrics confirmed current-run VM rows: `node6` had 508 rows for `VM-78/VM-79`, and `node7` had 510 rows for `VM-111/VM-112`. Note these rows are written on offload nodes, not the AM host.
  - Scale-in triggered once too early: `1781894728769,SCALE_IN,0.0195,0.0000,0.0000,315884.0000,315884,1.0000,179`.
  - Root cause of early scale-in: `scaleInIfIdle()` checked idle input/process rates and low CPU but did not require backlog to be drained. `source_metrics.csv` plateaued at `7,684,016` while Kafka offset was `8,000,000`, leaving ~315K queue backlog.
  - Fix added after this run: `scaleInIfIdle()` now requires `queue = aggInput - currSourceEvent <= 0` before scale-in.
  - Cleanup: app killed, local subscriber/producer/collector stopped, warm VMWorker pools killed, RM and five baseline NMs restarted; YARN returned to five `RUNNING` NMs with `0` containers.
- Recent Q0 regression checkpoints remain valid:
  - Sustained Q0 `nexmark-auto-123054`, app `application_1781890212133_0001`: source consumed all 8M records, scale-out fired once, scale-in fired once, no repeat scale-in loop.
  - Earlier Q0 `nexmark-auto-193044`: 500k events consumed with zero loss, scale-out/scale-in fired once each, metrics pipeline worked end-to-end.

### Full Sponge Q0 Benchmark (Latest)
- New wrapper `scripts/cloudlab/run_sponge_q0_benchmark.sh` created with full Sponge defaults: `TOTAL_EVENTS=23850000`, `RATE_PERIOD_SEC=450`, `FIRST_RATE=50000`, `NEXT_RATE=200000`.
- Run `sponge-q0-20260619-164645`, app `application_1781901266080_0002`, AM host `node9`.
  - Producer sent `23,849,900` live records plus 100 prefill = `23,850,000` total at ~58.3K ev/s average.
  - Kafka offsets confirmed `23,850,000` across 8 partitions.
  - Source metrics confirmed `23,850,000` consumed.
  - Result topic `nexmark-sponge-q0-20260619-164645-results` confirmed `23,850,000`.
  - Scale-out triggered once: `1781906080547,SCALE_OUT,0.0965,46426.0000,21618.4000,27808.0000,27808,0.6658,179`.
  - VM task migration succeeded: 132 VM task metric rows in `task_metrics.csv`.
  - Scale-in triggered once after backlog drained: `1781906116551,SCALE_IN,0.0200,0.0000,0.0000,-100.0000,-100,1.0000,179`.
  - No phantom scale-in loop observed (exactly 1 scale-in row).
  - Artifacts saved to `results/cloudlab/sponge-q0-20260619-164645/`.

### Custom Burst Mode (Latest)
- **Problem:** Legacy `BURSTY` mode produced only 1-second micro-bursts at 200K ev/s, which was barely distinguishable from the 50K ev/s steady rate due to the producer's ~66K ev/s max. The scaler only triggered at ~380s of a 408s run.
- **Solution:** Implemented `CUSTOM_BURST` mode in `StandaloneNexmarkKafkaProducer.java` with sustained multi-second bursts.
- **Pattern:** Ramp-up (60s at 50K) + N cycles of (steady 60s at 50K + burst 45s at 200K).
- **Implementation:** Time-based per-second batch loops with `RateShape.SQUARE` and `isRateLimited=false`. Events generated per second = `ratePerGen`, then sleep remainder of second. Respects `maxEvents` cap for prefill/controlled runs.
- **Default config:** `STEADY_DURATION_SEC=60`, `BURST_DURATION_SEC=45`, `NUM_BURSTS=3`, `RAMP_UP_SEC=60`, `FIRST_RATE=50000`, `NEXT_RATE=200000`.
- **Run `sponge-q0-20260619-174241`, app `application_1781901266080_0007`, AM host `node11`:**
  - Producer correctly generated full burst pattern: 60s ramp-up + 3 cycles of (60s steady + 45s burst) = 375s total, 39M events per phase (78M total).
  - Scale-out triggered early: `1781909586802,SCALE_OUT,0.3355,200000.0000,142812.8000,228542.0000,228542,0.3033,179`.
  - Queue built up to 228K during first burst (not 27K at 380s like legacy mode).
  - **Issue:** Prefill generated 39M instead of 100 because custom burst ignored `events` parameter.
  - **Fix:** Added optional `maxEvents` parameter (11th arg). When `maxEvents > 0`, caps total events. `run_autoscaler_smoke.sh` passes `events` as `maxEvents`.
  - **Recompiled:** `StandaloneNexmarkKafkaProducer.class` in `build/cloudlab-producer/` (was stale from old compilation).
- **Run `sponge-q0-20260619-180648`, app `application_1781901266080_0008`, AM host initially `node11`:**
  - Producer fix worked: prefill generated exactly 100 records, live producer generated `23,849,900`, input topic reached `23,850,000`.
  - Scale-out fired once: `1781910629888,SCALE_OUT,0.1438,200000.0000,51583.0000,127442.0000,127442,0.8528,179`.
  - Run failed after scale-out: source/result plateaued around `8,215,025` / `7,640,472`, REEF logged `FailedRuntime`, app fell back to `ACCEPTED` with `AM Host: N/A`, and YARN showed only one RUNNING NM.
  - `/tmp/source_metrics.csv` on AM alternated between current `8215025` and stale `78000000`, confirming stale fallback metric contamination; Kafka result offset was the authoritative incomplete result.
  - Aggregated YARN logs also showed `HDFSUtils` warning `java.net.UnknownHostException: hdfs-master` during setup; this was not confirmed as the root cause because the job processed ~8.2M records before the runtime failure.
  - Cleanup performed: killed `application_1781901266080_0008`, killed stale submit-side `JobLauncher`, restarted RM and all five baseline NMs; YARN returned to 5 RUNNING NMs with 0 containers and no active apps.
- `run_sponge_q0_benchmark.sh` now fails fast during monitoring if the YARN app leaves `RUNNING`, AM host becomes `N/A`, RUNNING NM count drops below `EXPECTED_NM_COUNT`, or source/result progress stalls after producer completion. It saves artifacts and runs cleanup on monitor failure.

### Scale-In Loop Fix
- **Root cause:** `scaleInIfIdle()` used `executorRegistry.getLambdaExecutors().size() > 0` as its only guard. After scale-in moved all tasks back, lambda executors remained registered forever, so every 1s idle tick re-triggered scale-in. `writeScalingDecision("SCALE_IN")` was called before checking whether lambda executors actually had eligible tasks, so no-op attempts were recorded as real scale-in rows.
- **Fix (3 changes in `InputAndCpuBasedScaler.java`):**
  1. `lastActionWasScaleOut` flag: set `true` after scale-out completes; `scaleInIfIdle()` returns immediately unless a scale-out happened first. Cleared after any scale-in outcome (empty lambda, no stages, or successful migration).
  2. `hasLambdaTasksToScaleIn()` helper: checks at least one lambda executor has non-CR running tasks. Replaces bare `getLambdaExecutors().size() > 0` check.
  3. `writeScalingDecision("SCALE_IN")` moved inside the future, after confirming lambda executors and stages are non-empty.
- **Result:** Before fix: 62+ SCALE_IN rows (one per second). After fix: exactly 1 SCALE_IN row.

### Scale-In Idle Input Fix
- **Root cause:** In sustained Q0 run `nexmark-auto-115819`, scale-out succeeded but scale-in did not trigger because `avgInputRate` stayed at the final positive delta (`13450`) after producer input stopped. `InputAndCpuBasedScaler` uses `DescriptiveStatistics(1)`, so if no later zero-delta input update reaches `addCurrentInput()`, the last positive input average remains forever and blocks `avgInput <= 0`.
- **Fix in `InputAndCpuBasedScaler.java`:**
  1. Added `lastInputUpdateTime` and a 30s `INPUT_IDLE_TIMEOUT_MS` stale-input guard.
  2. `scaleInIfIdle()` now treats input as idle when `avgInput <= 0 || isInputStale()` while still requiring `avgProcess <= 0` and lambda tasks to scale in.
  3. `addCurrentInput()` now records zero/non-positive updates as `avgInputRate=0` instead of returning silently.
  4. Negative cumulative deltas are guarded: they log a warning, reset current input rate to zero, and do not subtract from `aggInput`.
- Rebuilt `client/target/nemo-client-0.2-SNAPSHOT-shaded.jar` after this fix; latest timestamp observed: 2026-06-19 12:12:22 -0500.

### Scale-In Backlog Guard (Latest)
- **Root cause:** In Q8 run `nexmark-auto-134204`, scale-in fired while `queue = aggInput - currSourceEvent` was still `315884`. Input and source-processing rates were both zero because the producer finished and source metrics plateaued, but the backlog was not drained.
- **Fix in `InputAndCpuBasedScaler.java`:** `scaleInIfIdle()` now computes `queue = aggInput.get() - currSourceEvent` and requires `queue <= 0` before calling `scaleIn()`.
- **Behavior:** If input/process rates are idle but `queue > 0`, scale-in is skipped and logs `Input and processing are idle, but queue remains ...; skipping scale-in`.
- **Verification:** `mvn -pl runtime/master -am -DskipTests compile` passed after this change. The shaded client jar has not yet been rebuilt after the queue-guard fix.

### Metrics Pipeline (Latest)
- `StandaloneNexmarkKafkaProducer.java`: writes `producer_metrics.csv` every 5s with `timestamp,outputRate,totalSent,elapsedMs`.
- `StandaloneNexmarkKafkaProducer.java`: source-log writes are cumulative and append-mode; the final write uses the final total.
- `UnboundedSourceReadable.java`: tracks `idleTimeNs` (waiting for advance/pollRecord), samples `queueTimeNs` every 1000 msgs (time since message timestamp), reports input rate.
- `TaskEventRateCalculator.java`: writes `task_metrics.csv` with per-task inputRate, outputRate, processingTime, deserTime, inbytes, etc.
- `SourceEventAggregator.java`: writes `source_metrics.csv` with per-executor source counts every 1s.
- `InputAndCpuBasedScaler.java`: writes `scaling_decisions.csv` with timestamp, action, avgCpu, avgInput, avgProcess, queue, ratio, numExecutors.
- `metrics_collector.py` (new): background collector running on submit node (node0). Polls every 5s: Kafka latest offset via SSH to node1, source counts/CPU/scaling data via SSH to AM host. Writes `combined_metrics.csv`.
- `plot_metrics.py` (new): post-run plotting script that reads CSV files.
- All CSV writers fall back to `/tmp` when `nemo.work.dir` system property is unset (since YARN containers don't inherit client env vars).
- `metrics_collector.py` accepts optional 4th arg `am_host` for SSH-based reads; `run_autoscaler_smoke.sh` captures AM host from `yarn application -status`.

Active Scaler Implementation
---------------------------

`InputAndCpuBasedScaler.java`:
- 80-second initial delay before first decision.
- Skips first 10 observations and first 30 seconds of source handling.
- Checks every 1 second after initial delay.
- Scale-out triggers when: `queue / processingRate > scalerTriggerQueueDelay` AND `ratio > 0.1`.
- Scale-out path: `scalingWithRatio()` → `sendMigrationAllStages(ratio, vmComputeExecutors, LAMBDA)`.
- Scale-in triggers when: `(avgInput <= 0 || isInputStale()) && avgProcess <= 0 && queue <= 0 && hasLambdaTasksToScaleIn() && avgCpu < scalerScaleoutTriggerCPU`.
- `queue` is `aggInput - currSourceEvent`; non-empty backlog blocks scale-in even when input and processing rates are idle.
- Scale-in path: `scaleInIfIdle()` → `scaleIn()` → `sendMigration(ratios, lambdaExecutors, slist, COMPUTE)`.
- `queueSizeBasedScalingRatio` guards `avgInput <= 0`, clamps ratio to `[0.0, 0.95]`, and now handles `processingRate <= 0 && queue > 0` by triggering conservative queue-based scale-out instead of returning empty.
- `cpuBasedScalingRatio` clamps ratio.
- `hasLambdaTasksToScaleIn()`: checks lambda executors have non-CR running tasks (prevents phantom scale-in).
- `lastActionWasScaleOut` guard: `scaleInIfIdle()` returns immediately unless a scale-out happened first (prevents repeat scale-in loop).
- `writeScalingDecision("SCALE_IN")` moved inside future after empty checks (no phantom CSV rows).
- All metrics CSV writers (source_metrics, task_metrics, scaling_decisions) fall back to `/tmp` when `nemo.work.dir` is unset.

`InputAndQueueSizeBasedBackpressure.java`:
- `sendBackpressureWrapper()` is active and sends the in-memory `backpressureRate` to source executors after the 5s scaling-hint guard.
- `backpressureRate` starts from `policyConf.bpMinEvent` (`5000`) because the constructor overwrites the field initializer.
- `addCurrentInput` ignores non-positive heartbeat values and derives deltas from cumulative source-log totals.
- `queueBasedBackpressure()` exists but the active scheduling loop uses `cpuBasedBackpressure()`.

`ScaleInOutManager.java`:
- `sendMigrationAllStages` passes actual ratio instead of `1.0`.
- `prevSelectedTasksToMoveLambda` cleared on scale-in (`resourceType == COMPUTE`).

`MasterUtils.java`:
- `getMaxMigrationCntPerStage` uses `Math.max(1, ceil(count * ratio))` for positive ratios.

`JobScaler.java` (commented loop):
- Divide-by-zero guards: `workers.isEmpty()`, `tasksPerWorker == 0`, `ratio <= 0`, `input_rate == 0`, `numWorkers == 0`, `divide == 0`, `executorCpuUseMap.isEmpty()`.

Relevant Files
--------------

### Source (Nemo)
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/scaler/InputAndCpuBasedScaler.java`: active scaler
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/ScaleInOutManager.java`: migration manager
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/MasterUtils.java`: task count math
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/JobScaler.java`: old commented loop
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/TaskScheduledMapMaster.java`: task scheduling/migration state
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/scheduler/ContainerTypeAwareSchedulingConstraint.java`: VM-compatible schedulability; allows `VM` executors for `Lambda`-tagged migrated tasks
- `common/src/main/java/org/apache/nemo/common/RuntimeIdManager.java`: `isLambdaExecutorId()` helper
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/ExecutorChannelManagerMap.java`: VM- prefix fix
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/datatransfer/DefaultByteTransportImpl.java`: VM- prefix fix
- `runtime/lambda-executor/src/main/java/org/apache/nemo/runtime/lambdaexecutor/datatransfer/LambdaByteTransport.java`: VM- prefix fix
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/DefaultExecutorRepresenterImpl.java`: VM- prefix fix
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/resource/ContainerManager.java`: VM- prefix fix
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/RuntimeMaster.java`: VM- prefix fix
- `runtime/driver/src/main/java/org/apache/nemo/driver/ShortHostnameLocalAddressProvider.java`: returns short hostname
- `client/src/main/java/org/apache/nemo/client/JobLauncher.java`: binds `ShortHostnameLocalAddressProvider`, scaling service, source.log reader
- `common/src/main/java/org/apache/nemo/common/NetworkUtils.java`: internal `10.10.*` address selection for Netty server binding
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/SourceEventAggregator.java`: writes `source_metrics.csv`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/backpressure/InputAndQueueSizeBasedBackpressure.java`: backpressure with aggInput tracking
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/monitoring/TaskEventRateCalculator.java`: writes `task_metrics.csv`
- `compiler/frontend/beam/src/main/java/org/apache/nemo/compiler/frontend/beam/source/UnboundedSourceReadable.java`: tracks idle time, queue time (sampled 1000 msgs), input rate

### Scripts
- `scripts/cloudlab/run_autoscaler_smoke.sh`: autoscaler smoke test harness; exports `NEMO_WORK_DIR`, captures AM host for metrics, no longer syncs `source.log` to AM host
- `scripts/cloudlab/run_q8_subscriber_yarn.sh`: subscriber launcher; passes `-Dnemo.work.dir`
- `scripts/cloudlab/run_live_bursty_combined.sh`: live bursty producer pattern
- `scripts/cloudlab/start_warm_pool.sh`: VMWorker pool starter (defaults to Java 11)
- `scripts/cloudlab/generate_vm_addresses.py`: VM address generator (handles comma-separated nodes)
- `scripts/cloudlab/force_cloudlab_vm_offload_smoke.sh`: manual scaling.txt smoke test
- `scripts/cloudlab/cloudlab_env.sh`: env defaults
- `scripts/cloudlab/metrics_collector.py` (new): background collector, polls Kafka offsets via SSH to node1, reads driver CSVs via SSH to AM host, writes `combined_metrics.csv`
- `scripts/cloudlab/plot_metrics.py` (new): post-run plotting from CSV data
- `scripts/cloudlab/StandaloneNexmarkKafkaProducer.java`: producer with `producer_metrics.csv` output every 5s

### Config
- `/users/akash01/hadoop/etc/hadoop/yarn-env.sh` (distributed to all baseline nodes)
- `/users/akash01/hadoop/etc/hadoop/yarn-site.xml` (distributed)
- `/users/akash01/hadoop/etc/hadoop/hadoop-env.sh` (distributed)
- `/users/akash01/hadoop/etc/hadoop/slaves` (distributed)
- `configs/cloudlab/nemo-yarn-kafka-1source-8compute.json`
- `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`: Q8 autoscaler benchmark config with Source `slot: 4`
- `configs/cloudlab/nemo-yarn-kafka-1source-4compute.json`
- `configs/cloudlab/nemo-yarn-1source-5compute-small.json`

### Built JARs
- `client/target/nemo-client-0.2-SNAPSHOT-shaded.jar` (rebuilt after Q8 schedulability fix; latest timestamp observed: 2026-06-19 13:31:33 -0500; not yet rebuilt after scale-in backlog guard)
- `examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar`
- `offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar`

Key Decisions
-------------
- Use only 5 baseline workers as YARN NodeManagers.
- Do not run YARN NodeManagers on offload nodes (node4, node6, node7, node8, node13) or Kafka nodes (node1, node2, node3).
- Use Java 11 for Hadoop/YARN and Nemo throughout.
- Q0 with KAFKA sink for complete end-to-end verification via result topic offsets.
- 8-compute plus one Source executor with `slot: 4` is the current minimum viable Q8 autoscaler config for 4 Source + 4 Stage1 + 4 Stage2 tasks.
- Active scaler (`InputAndCpuBasedScaler`) preferred over old commented `JobScaler` loop.
- Config files must be manually distributed (`scp`) to all nodes; no shared filesystem.
- Default executor JSON for autoscaler smoke tests: `nemo-yarn-kafka-1source-8compute.json`.
- Q8 autoscaler benchmarks should use `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`; the older `1source-8compute` config has insufficient Source slots for Q8.
- `yarn.nodemanager.hostname` resolved from environment variable `${NM_HOST}` (not system property) to avoid circular reference in Hadoop Configuration.
- VM executor IDs use `VM-` prefix; treat same as `Lambda` prefix everywhere via `isLambdaExecutorId()`.
- VM executors must be schedulable for `Lambda`-tagged migrated tasks via `ContainerTypeAwareSchedulingConstraint`.
- Scale-in requires backlog drained (`aggInput - currSourceEvent <= 0`) before moving tasks back from VM executors.
- Address plane uses short hostnames via `ShortHostnameLocalAddressProvider`.

Next Steps
----------
1. If future scale-out does not trigger, debug:
   - Is `avgInputRate` populated correctly?
   - Is `avgSrcProcessingRate` populated correctly?
   - Are executor metrics arriving at `ExecutorMetricMap`?
   - Is `scaleInOutManager.sendMigrationAllStages` called with correct ratio?
2. Rerun Q8 sustained benchmark with the cap4 config after rebuilding the shaded client jar with the scale-in backlog guard; verify scale-out → VM task migration → no scale-in until `queue <= 0`.
3. Investigate why Q8 source metrics plateaued at `7,684,016` of `8,000,000` in `nexmark-auto-134204` after VM migration.
4. For the Pado scheduler deadlock, investigate `StreamingScheduler`/`TaskDispatcher` to prefer upstream tasks over downstream tasks.
5. Consider additional benchmark harness improvements: collect YARN logs, aggregate METRICLOG counters, auto-kill stale processes (timeout/watchdog already present in `run_sponge_q0_benchmark.sh`).

Critical Context Reminders
-------------------------
- `run_q8_subscriber_yarn.sh` is now non-blocking via `nohup ... > "$LOG_FILE" 2>&1 &`; do not reintroduce `| tee` for the long-running Java command because it keeps the harness attached to subscriber output.
- `NEMO_WORK_DIR` env var must be set so `JobLauncher` finds `scaling.txt` in the correct directory.
- The autoscaler harness must not reintroduce a background `source.log` scp/sync loop. `JobLauncher` reads `source.log` locally on node0 and forwards input updates to the AM via RPC.
- Scaling commands in `scaling.txt` must be in `nemo.work.dir` (from system property `nemo.work.dir`, env var `NEMO_WORK_DIR`, or `user.dir`).
- VM- prefix executor IDs will trigger "No address VM-1" error without the `isLambdaExecutorId()` fix.
- Q8 scale-out migration requires `ContainerTypeAwareSchedulingConstraint` to allow `VM` executors for `Lambda`-tagged tasks; otherwise migrated Stage1/Stage2 tasks will loop in `couldNotSchedule`.
- Q8 requires Source `slot: 4`; use `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`. The older `nemo-yarn-kafka-1source-8compute.json` has Source `slot: 1` and leaves `Stage0-1/2/3` unscheduled.
- `yarn.nodemanager.hostname` in `yarn-site.xml` must NOT reference itself (`${yarn.nodemanager.hostname}` creates circular ref and resolves to empty). Use `${NM_HOST}` referencing an env var.
- Hadoop config files are per-node (local filesystem). Always `scp` after editing.
- Old `run_q8_subscriber_yarn.sh` processes can linger and interfere with new runs.
- If `yarn node -list` shows duplicate or stale NMs, verify actual processes with `ps` on each node. `yarn-daemon.sh stop` can miss processes if pidfiles are stale; use careful process-level cleanup and then restart one RM plus one NM per baseline node.
- `InputAndCpuBasedScaler` scale-in loop is fixed: `lastActionWasScaleOut` guards re-entry, `hasLambdaTasksToScaleIn()` prevents phantom scale-in.
- `InputAndCpuBasedScaler` scale-in now requires `queue <= 0`; a non-empty backlog blocks scale-in even when input and processing rates are idle.
- All Java metrics CSV writers (`SourceEventAggregator`, `TaskEventRateCalculator`, `InputAndCpuBasedScaler`) fall back to `/tmp` when `nemo.work.dir` is null; YARN containers do not inherit client env vars.
- `metrics_collector.py` needs `am_host` as 4th argument to read driver-side CSVs via SSH; `run_autoscaler_smoke.sh` now captures `AM_HOST` from `yarn application -status` after the app reaches RUNNING.
- VM executor task metrics are written to offload nodes' local `/tmp/task_metrics.csv`, not the AM host. Check `node4,node6,node7,node8,node13` for current-run `VM-*` rows.
- Before the next benchmark, the workspace is clean for YARN runtime state: latest app was killed, local subscriber/producer/collector are stopped, warm VMWorker pools were killed, and YARN showed exactly five RUNNING baseline NMs with zero containers. The harness now removes AM-side fallback metric files automatically after it discovers the AM host.
- Full Sponge Q0 benchmark completed successfully: `sponge-q0-20260619-164645`, app `application_1781901266080_0002`, AM host `node9`. Input/source/result all reached `23,850,000`. Scale-out and scale-in fired exactly once each with no phantom loop. VM task metrics confirmed 132 rows. Artifacts saved to `results/cloudlab/sponge-q0-20260619-164645/`.
