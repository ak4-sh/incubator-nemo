package org.apache.nemo.runtime.executor.common.monitoring;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.reef.tang.annotations.Parameter;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public final class TaskEventRateCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(TaskEventRateCalculator.class.getName());

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private final String executorId;

  @Inject
  private TaskEventRateCalculator(final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                  @Parameter(JobConf.ExecutorId.class) final String executorId) {
    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
    this.executorId = executorId;
  }

  public Pair<Integer, Integer> calculateProcessedEvent() {
    int sum = 0;
    int sum2 = 0;

    final List<TaskExecutor> tasks = new ArrayList<>(taskExecutorMap.keySet());
    tasks.sort((t1, t2) -> t1.getId().compareTo(t2.getId()));

    final StringBuilder sb = new StringBuilder("---- Start of task processed event (# tasks: "
      + tasks.size() + " in executor " + executorId + ")----\n");

    // CSV writer
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    PrintWriter csvWriter = null;
    try {
      csvWriter = new PrintWriter(new FileWriter(outDir + "/task_metrics.csv", true));
    } catch (IOException e) {
      LOG.warn("Failed to create task metrics CSV", e);
    }
    final long now = System.currentTimeMillis();

    for (final TaskExecutor taskExecutor : tasks) {
      // final AtomicInteger count = taskExecutor.getProcessedCnt();
      //final AtomicInteger count2 = taskExecutor.getOffloadedCnt();
      final TaskMetrics.RetrievedMetrics taskMetrics = taskExecutor.getTaskMetrics().retrieve();
      sum += taskMetrics.inputElement;
      sb.append("METRICLOG/" + executorId + "\t");
      sb.append(taskExecutor.getId());
      sb.append("\t");
      sb.append("inputR: ");
      sb.append(taskMetrics.inputReceiveElement);
      sb.append("\tinputP:");
      sb.append(taskMetrics.inputElement);
      sb.append("\toutput:");
      sb.append(taskMetrics.outputElement);
      sb.append("\tinW:");
      sb.append(new Instant(taskMetrics.watermark));
      sb.append("\tioutW:");
      sb.append(new Instant(taskMetrics.outWatermark));
      sb.append("\tptime:");
      sb.append(taskMetrics.computation);
      sb.append("\tdsertime:");
      sb.append(taskMetrics.deserTime);
      sb.append("\tinbytes:");
      sb.append(taskMetrics.inbytes);
      sb.append("\tsertime:");
      sb.append(taskMetrics.serializedTime);
      sb.append("\toutbytes:");
      sb.append(taskMetrics.outbytes);
      sb.append("\tnumKey:");
      sb.append(taskExecutor.getNumKeys());
      sb.append("\towc:");
      sb.append(taskMetrics.outWatermarkCount);
      sb.append("\n");

      if (csvWriter != null) {
        csvWriter.printf("%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d%n",
          now, executorId, taskExecutor.getId(),
          taskMetrics.inputReceiveElement, taskMetrics.inputElement,
          taskMetrics.outputElement, taskMetrics.computation,
          taskMetrics.deserTime, taskMetrics.inbytes,
          taskMetrics.serializedTime, taskMetrics.outbytes);
      }
    }

    sb.append("----- End of taks processed event ----\n");

    LOG.info(sb.toString());
    if (csvWriter != null) {
      csvWriter.close();
    }

    return Pair.of(sum, sum2);
  }
}
