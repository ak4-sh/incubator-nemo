package org.apache.nemo.runtime.master.scaler;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.conf.PolicyConf;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.nemo.runtime.master.ClientRPC;
import org.apache.nemo.runtime.master.ExecutorRepresenter;
import org.apache.nemo.runtime.master.ScaleInOutManager;
import org.apache.nemo.runtime.master.backpressure.Backpressure;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public final class InputAndCpuBasedScaler implements Scaler {
  private static final Logger LOG = LoggerFactory.getLogger(InputAndCpuBasedScaler.class.getName());
  private static final long INPUT_IDLE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  private final AtomicLong aggInput = new AtomicLong(0);
  private long currEmitInput = 0;
  private volatile long lastInputUpdateTime = System.currentTimeMillis();

  private final ExecutorMetricMap executorMetricMap;
  private final PolicyConf policyConf;

  private final ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor();

  private long currRate = Long.MAX_VALUE;

  private final DescriptiveStatistics avgCpuUse;
  private final DescriptiveStatistics avgInputRate;
  private long currInputRate;
  private final DescriptiveStatistics avgSrcProcessingRate;
  private final DescriptiveStatistics avgExpectedCpu;
  private final DescriptiveStatistics avgMaxCpuUse;
  private long currSourceEvent = 0;
  private final int windowSize = 5;

  private final ScaleInOutManager scaleInOutManager;
  private final ExecutorRegistry executorRegistry;

  private final AtomicBoolean prevFutureCompleted = new AtomicBoolean(true);
  private long prevFutureCompleteTime = System.currentTimeMillis();

  private final ExecutorService prevFutureChecker = Executors.newSingleThreadExecutor();

  private int observation = 0;

  private boolean started = false;

  private final Backpressure backpressure;
  private final boolean autoscaling;

  private final ClientRPC clientRPC;
  private final String jobId;

  private static final class ScalingDecisionSnapshot {
    private final String action;
    private final String trigger;
    private final double avgCpu;
    private final double cpuThreshold;
    private final double expectedCpu;
    private final double upperCpuThreshold;
    private final double targetCpu;
    private final double avgInput;
    private final double avgProcess;
    private final long queue;
    private final double queueDelay;
    private final double queueThreshold;
    private final double relayOverhead;
    private final double ratio;
    private final int numExecutors;
    private final int numComputeExecutors;
    private final int numLambdaExecutors;

    private ScalingDecisionSnapshot(final String action,
                                    final String trigger,
                                    final double avgCpu,
                                    final double cpuThreshold,
                                    final double expectedCpu,
                                    final double upperCpuThreshold,
                                    final double targetCpu,
                                    final double avgInput,
                                    final double avgProcess,
                                    final long queue,
                                    final double queueDelay,
                                    final double queueThreshold,
                                    final double relayOverhead,
                                    final double ratio,
                                    final int numExecutors,
                                    final int numComputeExecutors,
                                    final int numLambdaExecutors) {
      this.action = action;
      this.trigger = trigger;
      this.avgCpu = avgCpu;
      this.cpuThreshold = cpuThreshold;
      this.expectedCpu = expectedCpu;
      this.upperCpuThreshold = upperCpuThreshold;
      this.targetCpu = targetCpu;
      this.avgInput = avgInput;
      this.avgProcess = avgProcess;
      this.queue = queue;
      this.queueDelay = queueDelay;
      this.queueThreshold = queueThreshold;
      this.relayOverhead = relayOverhead;
      this.ratio = ratio;
      this.numExecutors = numExecutors;
      this.numComputeExecutors = numComputeExecutors;
      this.numLambdaExecutors = numLambdaExecutors;
    }
  }

  @Inject
  private InputAndCpuBasedScaler(final ExecutorMetricMap executorMetricMap,
                                 final ScaleInOutManager scaleInOutManager,
                                  final ExecutorRegistry executorRegistry,
                                  final Backpressure backpressure,
                                  final ClientRPC clientRPC,
                                  final PolicyConf policyConf,
                                  @Parameter(EvalConf.Autoscaling.class) final boolean autoscaling,
                                  @Parameter(JobConf.JobId.class) final String jobId) {
    this.executorMetricMap = executorMetricMap;
    this.policyConf = policyConf;
    this.scaleInOutManager = scaleInOutManager;
    this.executorRegistry = executorRegistry;
    this.avgCpuUse = new DescriptiveStatistics(windowSize);
    this.avgMaxCpuUse = new DescriptiveStatistics(windowSize);
    this.avgInputRate = new DescriptiveStatistics(1);
    this.avgSrcProcessingRate = new DescriptiveStatistics(windowSize);
    this.avgExpectedCpu = new DescriptiveStatistics(windowSize);
    this.currRate = policyConf.bpMinEvent;
    this.backpressure = backpressure;
    this.autoscaling = autoscaling;
    this.clientRPC = clientRPC;
    this.jobId = jobId;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        final ExecutorMetricInfo info = executorMetricMap.getAggregated();

        if (info.numExecutor > 0) {
          avgCpuUse.addValue(info.cpuUse / info.numExecutor);
          avgMaxCpuUse.addValue(info.maxCpuUse);
        }

        final double avgCpu = avgCpuUse.getMean();
        final double avgProcess = avgSrcProcessingRate.getMean();
        final double avgInput = avgInputRate.getMean();
        final long queue = aggInput.get() - currSourceEvent;

        clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
          .setType(ControlMessage.DriverToClientMessageType.PrintLog)
          .setPrintStr(String.format("Avg cpu: %f " +
            "Max cpu: %f " +
              "Avg input: %f, Avg process input: %f Curr input: %d, NumExecutor: %d",
            avgCpu,
            avgMaxCpuUse.getMean(),
            avgInput,
            avgProcess,
            currInputRate,
          info.numExecutor)).build());

        writeScalerMetrics(avgCpu, avgInput, avgProcess, queue, info.numExecutor);

        final double avgExpectedCpuVal;
        if (avgProcess > 0 && avgInput > 0 && info.numExecutor > 0) {
          avgExpectedCpu.addValue((avgInput * avgCpu) / avgProcess);
          avgExpectedCpuVal = avgExpectedCpu.getMean();
        } else {
          avgExpectedCpuVal = 0.0;
        }

        LOG.info("Scaler avg cpu: {}, avg expected cpu: {}, target cpu: {}, " +
            "avg input: {}, avg process input: {}, numExecutor: {}",
          avgCpu,
          avgExpectedCpuVal,
          policyConf.scalerTargetCpu,
          avgInput,
          avgProcess,
          info.numExecutor);

        if (info.numExecutor == 0) {
          return;
        }

        if (!started) {
          return;
        }

        observation += 1;

        // Skip in initial
        if (observation < 10) {
          return;
        }

        if (System.currentTimeMillis() - sourceHandlingStartTime
          <= TimeUnit.SECONDS.toMillis(30)) {
          return;
        }

        if (!autoscaling) {
          return;
        }

        if (!prevFutureCompleted.get()) {
          LOG.info("Prev future is not finished ... skip current decision");
          return;
        }

        if (System.currentTimeMillis() - prevFutureCompleteTime < TimeUnit.SECONDS
          .toMillis(policyConf.scalerSlackTime)) {
          LOG.info("Elapsed time is less than slack time... skip current decision {}/ {}",
            System.currentTimeMillis() - prevFutureCompleteTime, policyConf.scalerSlackTime);
          return;
        }

        synchronized (this) {
          boolean scaled = false;

          final Optional<ScalingDecisionSnapshot> queueDecision =
            queueSizeBasedScalingDecision(avgCpu, avgExpectedCpuVal, info.numExecutor);
          if (queueDecision.isPresent() && queueDecision.get().ratio > 0.1) {
            if (hasEligibleTasksToMigrate()) {
              scalingWithDecision(queueDecision.get());
              scaled = true;
            } else {
              LOG.info("No eligible tasks on compute executors to migrate; skipping scale-out");
            }
          }

          if (!scaled) {
            final Optional<ScalingDecisionSnapshot> cpuDecision =
              cpuBasedScalingDecision(avgCpu, avgExpectedCpuVal, info.numExecutor);
            if (cpuDecision.isPresent() && cpuDecision.get().ratio > 0.1) {
              scalingWithDecision(cpuDecision.get());
              scaled = true;
            }
          }

          // Minimal automatic scale-in
          if (!scaled && executorRegistry.getLambdaExecutors().size() > 0) {
            scaleInIfIdle();
          }
        }

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, 80, 1, TimeUnit.SECONDS);
  }

  private boolean hasEligibleTasksToMigrate() {
    return executorRegistry.getVMComputeExecutors().stream()
      .anyMatch(exec -> exec.getRunningTasks().stream()
        .anyMatch(task -> !task.isCrTask()));
  }

  private Optional<ScalingDecisionSnapshot> queueSizeBasedScalingDecision(final double avgCpu,
                                                                          final double expectedCpu,
                                                                          final int numExecutors) {
    final long queue = aggInput.get() - currSourceEvent;
    final double processingRate = avgSrcProcessingRate.getMean();
    final double avgInput = avgInputRate.getMean();
    final double queueDelay = processingRate > 0 ? queue / processingRate : -1.0;

    if (processingRate > 0) {
      LOG.info("Scaler queue: {}, processingRate: {}, avgInputRate: {}, delay: {}",
        queue, processingRate, avgInput, queueDelay);
    } else {
      LOG.info("Scaler queue: {}, processingRate: {}, avgInputRate: {}, delay: N/A",
        queue, processingRate, avgInput);
    }

    if (queue < 0) {
      return Optional.empty();
    }

    if (processingRate > 0) {
      if (queueDelay > policyConf.scalerTriggerQueueDelay) {
        if (avgInput <= 0) {
          LOG.warn("avgInputRate is zero or negative; cannot compute ratio");
          return Optional.empty();
        }

        final double rawRatio = 1 - (processingRate / avgInput) + policyConf.scalerRelayOverhead;
        final double ratioToScaleout = Math.max(0.0, Math.min(0.95, rawRatio));

        return Optional.of(newDecision("SCALE_OUT", "QUEUE", avgCpu, expectedCpu, avgInput,
          processingRate, queue, queueDelay, ratioToScaleout, numExecutors));
      }
    } else if (queue > 0) {
      // Source is stalled (processingRate = 0) but queue is positive;
      // scale out with a conservative queue-based ratio.
      LOG.info("Source stalled with queue={}, triggering queue-based scale-out", queue);
      final double rawRatio = Math.min(0.95, queue / (double) Math.max(aggInput.get(), 1));
      return Optional.of(newDecision("SCALE_OUT", "QUEUE_STALLED", avgCpu, expectedCpu, avgInput,
        processingRate, queue, queueDelay, Math.max(0.1, rawRatio), numExecutors));
    }

    return Optional.empty();
  }

  private Optional<ScalingDecisionSnapshot> cpuBasedScalingDecision(final double avgCpu,
                                                                    final double avgExpectedCpuVal,
                                                                    final int numExecutors) {
    if (avgCpu > policyConf.scalerScaleoutTriggerCPU
      && avgExpectedCpuVal > policyConf.scalerUpperCpu) {
      // Scale out !!
      // ex) expected cpu val: 2.0, target cpu: 0.6
      // then, we should reduce the current load of cluster down to 0.3 (2.0 * 0.3 = 0.6),
      // which means that we should scale out 70 % of tasks to Lambda (1 - 0.3)
      final double rawRatio = 1 - policyConf.scalerTargetCpu / avgExpectedCpuVal;
      final double ratioToScaleout = Math.max(0.0, Math.min(0.95, rawRatio));
      // move ratioToScaleout % of computations to Lambda
      final long queue = aggInput.get() - currSourceEvent;
      final double processingRate = avgSrcProcessingRate.getMean();
      final double avgInput = avgInputRate.getMean();
      final double queueDelay = processingRate > 0 ? queue / processingRate : -1.0;
      return Optional.of(newDecision("SCALE_OUT", "CPU", avgCpu, avgExpectedCpuVal, avgInput,
        processingRate, queue, queueDelay, ratioToScaleout, numExecutors));
    }

    return Optional.empty();
  }

  private ScalingDecisionSnapshot newDecision(final String action,
                                              final String trigger,
                                              final double avgCpu,
                                              final double expectedCpu,
                                              final double avgInput,
                                              final double avgProcess,
                                              final long queue,
                                              final double queueDelay,
                                              final double ratio,
                                              final int numExecutors) {
    return new ScalingDecisionSnapshot(action, trigger, avgCpu, policyConf.scalerScaleoutTriggerCPU,
      expectedCpu, policyConf.scalerUpperCpu, policyConf.scalerTargetCpu, avgInput, avgProcess,
      queue, queueDelay, policyConf.scalerTriggerQueueDelay, policyConf.scalerRelayOverhead, ratio,
      numExecutors, executorRegistry.getVMComputeExecutors().size(),
      executorRegistry.getLambdaExecutors().size());
  }

  private void scalingWithDecision(final ScalingDecisionSnapshot decision) {
    // move ratioToScaleout % of computations to Lambda
    LOG.info("Move {} percent of tasks in all vm executors", decision.ratio);
    emitDecisionLog(decision);
    writeScalingDecision(decision);

    prevFutureCompleted.set(false);

    prevFutureChecker.execute(() -> {
      final long st = System.currentTimeMillis();
      LOG.info("Waiting for scale out decision");
      scaleInOutManager.sendMigrationAllStages(
        decision.ratio,
        executorRegistry.getVMComputeExecutors(),
        ResourcePriorityProperty.LAMBDA).forEach(future -> {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      });
      scaleInOutManager.clearPrevSelectedTasksToMoveLambda();
      final long et = System.currentTimeMillis();
      prevFutureCompleted.set(true);
      prevFutureCompleteTime = et;
      lastActionWasScaleOut = true;

      // send hints to the backpressure
      backpressure.setHintForScaling(decision.ratio);

      LOG.info("End of waiting for scale out decision {}", et - st);
    });
  }

  private long sourceHandlingStartTime = 0;
  private boolean lastActionWasScaleOut = false;

  @Override
  public void start() {
    LOG.info("Start scaler");
    started = true;
  }

  private boolean hasLambdaTasksToScaleIn() {
    return executorRegistry.getLambdaExecutors().stream()
      .anyMatch(exec -> exec.getRunningTasks().stream()
        .anyMatch(task -> !task.isCrTask()));
  }

  private void scaleInIfIdle() {
    final double avgInput = avgInputRate.getMean();
    final double avgProcess = avgSrcProcessingRate.getMean();
    final double avgCpu = avgCpuUse.getMean();
    final boolean inputIdle = avgInput <= 0 || isInputStale();
    final long queue = aggInput.get() - currSourceEvent;

    // Only scale in after a scale-out has happened and there are actually
    // eligible tasks on lambda executors to move back.
    if (!lastActionWasScaleOut) {
      return;
    }

    if (inputIdle && avgProcess <= 0 && queue <= 0 && hasLambdaTasksToScaleIn()) {
      LOG.info("Queue is cleared (input: {}, inputStale: {}, process: {}, queue: {}); considering scale-in",
        avgInput, isInputStale(), avgProcess, queue);
      if (avgCpu < policyConf.scalerScaleoutTriggerCPU) {
        LOG.info("CPU is low ({}) and queue is cleared; scaling in", avgCpu);
        scaleIn();
      }
    } else if (inputIdle && avgProcess <= 0 && queue > 0) {
      LOG.info("Input and processing are idle, but queue remains {}; skipping scale-in", queue);
    }
  }

  private boolean isInputStale() {
    return System.currentTimeMillis() - lastInputUpdateTime > INPUT_IDLE_TIMEOUT_MS;
  }

  private void scaleIn() {
    prevFutureCompleted.set(false);

    prevFutureChecker.execute(() -> {
      final long st = System.currentTimeMillis();
      LOG.info("Waiting for scale-in decision");

      final Set<ExecutorRepresenter> lambdaExecutors = executorRegistry.getLambdaExecutors();
      if (lambdaExecutors.isEmpty()) {
        LOG.info("No Lambda executors to scale in");
        lastActionWasScaleOut = false;
        prevFutureCompleted.set(true);
        prevFutureCompleteTime = System.currentTimeMillis();
        return;
      }

      // Get stages from Lambda executors
      final Set<String> stages = lambdaExecutors.stream()
        .map(executor -> executor.getRunningTasks())
        .flatMap(l -> l.stream()
          .filter(task -> !task.isCrTask())
          .map(t -> t.getStageId()))
        .collect(Collectors.toSet());

      if (stages.isEmpty()) {
        LOG.info("No eligible stages to scale in");
        lastActionWasScaleOut = false;
        prevFutureCompleted.set(true);
        prevFutureCompleteTime = System.currentTimeMillis();
        return;
      }

      final ScalingDecisionSnapshot decision = newDecision("SCALE_IN", "IDLE", avgCpuUse.getMean(),
        avgExpectedCpu.getMean(), avgInputRate.getMean(), avgSrcProcessingRate.getMean(),
        aggInput.get() - currSourceEvent,
        avgSrcProcessingRate.getMean() > 0
          ? (aggInput.get() - currSourceEvent) / avgSrcProcessingRate.getMean() : -1.0,
        1.0, executorRegistry.getRunningExecutors().size());
      emitDecisionLog(decision);
      writeScalingDecision(decision);

      final List<String> slist = new ArrayList<>(stages);
      final List<Double> ratios = slist.stream().map(s -> 1.0).collect(Collectors.toList());

      scaleInOutManager.sendMigration(ratios, lambdaExecutors, slist, ResourcePriorityProperty.COMPUTE)
        .forEach(future -> {
          try {
            future.get();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (ExecutionException e) {
            e.printStackTrace();
          }
        });

      final long et = System.currentTimeMillis();
      lastActionWasScaleOut = false;
      prevFutureCompleted.set(true);
      prevFutureCompleteTime = et;

      LOG.info("End of waiting for scale-in decision {}", et - st);
    });
  }

  @Override
  public void addSourceEvent(final long sourceEvent) {
    avgSrcProcessingRate.addValue(sourceEvent - currSourceEvent);
    currInputRate = sourceEvent - currSourceEvent;
    currSourceEvent = sourceEvent;

    if (sourceHandlingStartTime == 0) {
      sourceHandlingStartTime = System.currentTimeMillis();
    }
  }

  @Override
  public void addCurrentInput(final long rate) {
    lastInputUpdateTime = System.currentTimeMillis();
    if (rate <= 0) {
      currInputRate = 0;
      avgInputRate.addValue(0);
      return;
    }
    final long delta = rate - currEmitInput;
    if (delta < 0) {
      LOG.warn("Cumulative input decreased from {} to {}; ignoring negative delta", currEmitInput, rate);
      currEmitInput = rate;
      currInputRate = 0;
      avgInputRate.addValue(0);
      return;
    }
    currEmitInput = rate;
    currInputRate = delta;
    avgInputRate.addValue(delta);
    aggInput.getAndAdd(delta);
  }

  private void emitDecisionLog(final ScalingDecisionSnapshot decision) {
    final String msg = String.format("Scale decision action=%s trigger=%s avgCpu=%.4f "
        + "cpuThreshold=%.4f expectedCpu=%.4f upperCpuThreshold=%.4f avgInput=%.4f "
        + "avgProcess=%.4f queue=%d queueDelay=%.4f queueThreshold=%.4f ratio=%.4f "
        + "numExecutors=%d numComputeExecutors=%d numLambdaExecutors=%d",
      decision.action, decision.trigger, decision.avgCpu, decision.cpuThreshold,
      decision.expectedCpu, decision.upperCpuThreshold, decision.avgInput, decision.avgProcess,
      decision.queue, decision.queueDelay, decision.queueThreshold, decision.ratio,
      decision.numExecutors, decision.numComputeExecutors, decision.numLambdaExecutors);
    LOG.info(msg);
    clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
      .setType(ControlMessage.DriverToClientMessageType.PrintLog)
      .setPrintStr(msg).build());
  }

  private void writeScalingDecision(final ScalingDecisionSnapshot decision) {
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    final File outFile = new File(outDir, "scaling_decisions.csv");
    final File parent = outFile.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      LOG.warn("Failed to create scaling decision metrics directory {}", parent);
    }
    try (PrintWriter writer = new PrintWriter(new FileWriter(outFile, true))) {
      final long now = System.currentTimeMillis();
      writer.printf("%d,%s,%.4f,%.4f,%.4f,%.4f,%d,%.4f,%d,"
          + "%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%d,%d%n",
        now, decision.action, safeMetric(decision.avgCpu), safeMetric(decision.avgInput),
        safeMetric(decision.avgProcess), (double) decision.queue, decision.queue,
        safeMetric(decision.ratio), decision.numExecutors, decision.trigger,
        safeMetric(decision.cpuThreshold), safeMetric(decision.expectedCpu),
        safeMetric(decision.upperCpuThreshold), safeMetric(decision.targetCpu),
        safeMetric(decision.queueDelay), safeMetric(decision.queueThreshold),
        decision.numComputeExecutors, decision.numLambdaExecutors);
    } catch (IOException e) {
      LOG.warn("Failed to write scaling decision", e);
    }
  }

  private double safeMetric(final double value) {
    return Double.isFinite(value) ? value : -1.0;
  }

  private void writeScalerMetrics(final double avgCpu,
                                   final double avgInput,
                                   final double avgProcess,
                                   final long queue,
                                   final int numExecutors) {
    final double safeAvgCpu = Double.isFinite(avgCpu) ? avgCpu : -1.0;
    final double safeAvgInput = Double.isFinite(avgInput) ? avgInput : -1.0;
    final double safeAvgProcess = Double.isFinite(avgProcess) ? avgProcess : -1.0;
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    final File outFile = new File(outDir, "scaler_metrics.csv");
    final File parent = outFile.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      LOG.warn("Failed to create scaler metrics directory {}", parent);
    }
    final boolean writeHeader = !outFile.exists() || outFile.length() == 0;
    try (PrintWriter writer = new PrintWriter(new FileWriter(outFile, true))) {
      if (writeHeader) {
        writer.println("timestamp,jobId,avgCpu,avgInput,avgProcess,queue,numExecutors,numLambdaExecutors,lastActionWasScaleOut,prevFutureCompleted");
      }
      writer.printf("%d,%s,%.4f,%.4f,%.4f,%d,%d,%d,%s,%s%n",
        System.currentTimeMillis(), jobId, safeAvgCpu, safeAvgInput, safeAvgProcess, queue, numExecutors,
        executorRegistry.getLambdaExecutors().size(), lastActionWasScaleOut, prevFutureCompleted.get());
    } catch (IOException e) {
      LOG.warn("Failed to write scaler metrics", e);
    }
  }
}
