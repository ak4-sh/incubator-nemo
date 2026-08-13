package org.apache.nemo.runtime.master;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.nemo.runtime.master.backpressure.Backpressure;
import org.apache.nemo.runtime.master.scaler.Scaler;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class SourceEventAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(SourceEventAggregator.class.getName());

  private final Backpressure backpressure;
  private final Scaler scaler;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Map<String, Long> sourceEventMap;
  private final String jobId;

  @Inject
  private SourceEventAggregator(final Backpressure backpressure,
                                final Scaler scaler,
                                final MessageEnvironment messageEnvironment,
                                @Parameter(JobConf.JobId.class) final String jobId) {
    messageEnvironment.setupListener(MessageEnvironment.ListenerType.SOURCE_EVENT_HANDLER_ID,
      new MessageReceiver());

    this.backpressure = backpressure;
    this.scaler = scaler;
    this.jobId = jobId;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.sourceEventMap = new ConcurrentHashMap<>();

    scheduledExecutorService.scheduleAtFixedRate(() -> {

      synchronized (sourceEventMap) {
        final long count = sourceEventMap.values().stream().reduce((x, y) -> x + y).orElse(0L);

        if (count > 0) {
          LOG.info("Set source count {} / {}", count, sourceEventMap);
          backpressure.addSourceEvent(count);
          scaler.addSourceEvent(count);
          writeSourceMetrics(count);
        }
      }

    }, 1, 1, TimeUnit.SECONDS);
  }

  private void writeSourceMetrics(final long totalSourceCount) {
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final File outFile = new File(workDir != null ? workDir : "/tmp", "source_aggregate_metrics.csv");
    final File parent = outFile.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      LOG.warn("Failed to create source aggregate telemetry directory {}", parent);
    }
    final boolean writeHeader = !outFile.exists() || outFile.length() == 0;
    try (PrintWriter writer = new PrintWriter(new FileWriter(outFile, true))) {
      if (writeHeader) {
        writer.println("timestamp,jobId,totalSourceCount,executorId,executorSourceCount");
      }
      final long now = System.currentTimeMillis();
      for (final Map.Entry<String, Long> entry : sourceEventMap.entrySet()) {
        writer.printf("%d,%s,%d,%s,%d%n",
          now, jobId, totalSourceCount, entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      LOG.warn("Failed to write passive source aggregate telemetry", e);
    }
  }


  public final class MessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case SourceEvent: {
          synchronized (sourceEventMap) {
            sourceEventMap.put(message.getRegisteredExecutor(), message.getSetNum());
          }
          break;
        }
        default: {
          throw new RuntimeException("not supported " + message.getType());
        }
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
        throw new RuntimeException("Not supported " + message);
    }
  }
}
