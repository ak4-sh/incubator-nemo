package org.apache.nemo.compiler.frontend.beam.source;

import io.netty.buffer.ByteBufOutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UnboundedSourceReadable class.
 * @param <O> output type.
 * @param <M> checkpoint mark type.
 */
public final class UnboundedSourceReadable<O, M extends UnboundedSource.CheckpointMark> implements Readable<Object> {
  private final UnboundedSource<O, M> unboundedSource;
  private UnboundedSource.UnboundedReader<O> reader;
  private boolean isStarted = false;
  private volatile boolean isCurrentAvailable = false;
  private volatile boolean isKafkaPolled = false;
  private volatile boolean isKafkaPolling = false;
  private boolean isFinished = false;

  private final PipelineOptions pipelineOptions;
  private M checkpointMark;

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceReadable.class.getName());

  private KafkaUnboundedReader kafkaReader;

  private ExecutorService readableService;

  private ReadableContext readableContext;
  private StateStore stateStore;
  private String taskId;

  // Kafka metrics
  private PrintWriter metricsWriter;
  private final AtomicLong recordsRead = new AtomicLong(0);
  private final AtomicLong samplingCounter = new AtomicLong(0);
  private long lastMetricTime = 0;
  private long idleTimeNs = 0;
  private long totalIdleTimeNs = 0;

  /**
   * Constructor.
   * @param unboundedSource unbounded source.
   */
  public UnboundedSourceReadable(final UnboundedSource<O, M> unboundedSource) {
    this(unboundedSource, null, null);
  }

  public UnboundedSourceReadable(final UnboundedSource<O, M> unboundedSource,
                                 final PipelineOptions options,
                                 final M checkpointMark) {
    this.unboundedSource = unboundedSource;
    this.pipelineOptions = options;
    this.checkpointMark = checkpointMark;
  }

  public UnboundedSource.UnboundedReader<O> getReader() {
    return reader;
  }

  public UnboundedSource getUnboundedSource() {
    return unboundedSource;
  }

  @Override
  public void checkpoint() {
    final UnboundedSource.CheckpointMark checkpointMark = getReader().getCheckpointMark();
    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = getUnboundedSource().getCheckpointMarkCoder();

    final StateStore stateStore = readableContext.getStateStore();
    final String taskId = readableContext.getTaskId();

    final ByteArrayOutputStream bos = new ByteArrayOutputStream(100);

    LOG.info("Store checkpointmark of task {}/ {}", taskId, checkpointMark);
    try {
      checkpointMarkCoder.encode(checkpointMark, bos);
      bos.close();
      stateStore.put(taskId, bos.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void restore() {
    if (stateStore.containsState(taskId)) {
      LOG.info("Task " + taskId + " has checkpointMark state... we should deserialize it.");
      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = (Coder<UnboundedSource.CheckpointMark>)
        unboundedSource.getCheckpointMarkCoder();

      final long st = System.currentTimeMillis();

      try {
        final InputStream is = stateStore.getStateStream(taskId);
        checkpointMark = (M) checkpointMarkCoder
          .decode(is);
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      final long et = System.currentTimeMillis();

      LOG.info("Task {} checkpoint deserialize time {}", taskId, et - st);

      LOG.info("Task " + taskId + " checkpoint mark " + checkpointMark);
    }

    try {

      final long et = System.currentTimeMillis();

      readableService = ReadableService.getInstance();
      reader = unboundedSource.createReader(pipelineOptions, checkpointMark);
      kafkaReader = reader instanceof KafkaUnboundedReader ? (KafkaUnboundedReader) reader : null;

      final long et2 = System.currentTimeMillis();

      LOG.info("Task {} reader create time {}", taskId, et2 - et);

      isCurrentAvailable = reader.start();

      LOG.info("Task {} reader start time {}", taskId, System.currentTimeMillis() - et2);

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void prepare(ReadableContext readableContext) {
    LOG.info("Prepare unbounded sources!! {}, {}", unboundedSource, unboundedSource.toString());
    this.readableContext = readableContext;
    taskId = readableContext.getTaskId();
    stateStore = readableContext.getStateStore();

    // Initialize Kafka metrics writer
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    try {
      metricsWriter = new PrintWriter(new FileWriter(outDir + "/source_metrics.csv", true));
      metricsWriter.println("timestamp,taskId,idleTimeNs,queueTimeNs,inputRate,recordsRead");
      metricsWriter.flush();
    } catch (IOException e) {
      LOG.warn("Failed to create metrics writer", e);
    }
    lastMetricTime = System.currentTimeMillis();

    if (stateStore.containsState(taskId)) {
      LOG.info("Task " + taskId + " has checkpointMark state... we should deserialize it.");
      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = (Coder<UnboundedSource.CheckpointMark>)
        unboundedSource.getCheckpointMarkCoder();

      final long st = System.currentTimeMillis();

      try {
        final InputStream is = stateStore.getStateStream(taskId);
        checkpointMark = (M) checkpointMarkCoder
          .decode(is);
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      final long et = System.currentTimeMillis();

      LOG.info("Task {} checkpoint deserialize time {}", taskId, et - st);

      LOG.info("Task " + taskId + " checkpoint mark " + checkpointMark);
    }

    try {

      final long et = System.currentTimeMillis();

      readableService = ReadableService.getInstance();
      reader = unboundedSource.createReader(pipelineOptions, checkpointMark);
      kafkaReader = reader instanceof KafkaUnboundedReader ? (KafkaUnboundedReader) reader : null;

      final long et2 = System.currentTimeMillis();

      LOG.info("Task {} reader create time {}", taskId, et2 - et);

      isCurrentAvailable = reader.start();

      LOG.info("Task {} reader start time {}", taskId, System.currentTimeMillis() - et2);

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized boolean isAvailable() {
    if (reader == null) {
      return false;
    }

    if (isCurrentAvailable) {
      return true;
    } else {
      final long idleStart = System.nanoTime();
      try {
        // poll kafka only when this source is Kafka-backed
        if (kafkaReader != null) {
          kafkaReader.pollRecord(5);
        }
        isCurrentAvailable =  reader.advance();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      idleTimeNs += System.nanoTime() - idleStart;
      return isCurrentAvailable;
    }
  }


  @Override
  public synchronized Object readCurrent() {

    if (isCurrentAvailable) {
      final O elem = reader.getCurrent();
      Instant currTs = reader.getCurrentTimestamp();
      if (currTs == null) {
        currTs = Instant.now();
      }

      // Track record read
      final long now = recordsRead.incrementAndGet();
      final long currentTimeMs = System.currentTimeMillis();

      // Sample queue time every 1000 messages
      long queueTimeNs = 0;
      if (kafkaReader != null && samplingCounter.incrementAndGet() >= 1000) {
        samplingCounter.set(0);
        // Kafka message timestamp is available via getCurrentTimestamp
        // Queue time = current time - message timestamp
        queueTimeNs = (currentTimeMs - currTs.getMillis()) * 1_000_000L;
      }

      // Report metrics every 5 seconds
      if (metricsWriter != null && currentTimeMs - lastMetricTime >= 5000) {
        final long totalRecords = recordsRead.get();
        final double inputRate = totalRecords * 1000.0 / Math.max(1, currentTimeMs - lastMetricTime);
        metricsWriter.printf("%d,%s,%d,%d,%.2f,%d%n",
          currentTimeMs, taskId, totalIdleTimeNs, queueTimeNs, inputRate, totalRecords);
        metricsWriter.flush();
        totalIdleTimeNs = 0;
        lastMetricTime = currentTimeMs;
      }

      try {
        isCurrentAvailable =  reader.advance();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return new TimestampAndValue<>(currTs.getMillis(),
        WindowedValue.timestampedValueInGlobalWindow(elem, currTs));
    } else {
      // poll kafka
      final long idleStart = System.nanoTime();
      if (kafkaReader != null) {
        kafkaReader.pollRecord(5);
      }
      // set current available
      try{
        isCurrentAvailable = reader.advance();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      idleTimeNs += System.nanoTime() - idleStart;
      isKafkaPolling = false;

      return EmptyElement.getInstance();
    }

    /*
    if (isFetchTime) {
      isFetchTime = false;
      readableService.execute(() -> {
        try {
          isCurrentAvailable = reader.advance();
          isFetchTime = !isCurrentAvailable;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    }

    if (isCurrentAvailable) {
      final O elem = reader.getCurrent();
      Instant currTs = reader.getCurrentTimestamp();
      if (currTs == null) {
        currTs = Instant.now();
      }
      //LOG.info("Curr timestamp: {}", currTs);

      isCurrentAvailable = false;
      readableService.execute(() -> {
        try {
          isCurrentAvailable = reader.advance();
          isFetchTime = !isCurrentAvailable;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });

      return new TimestampAndValue<>(currTs.getMillis(),
        WindowedValue.timestampedValueInGlobalWindow(elem, currTs));
    }

    return EmptyElement.getInstance();
    */
  }

  @Override
  public synchronized long readWatermark() {
    final Instant watermark = reader.getWatermark();
    // Finish if the watermark == TIMESTAMP_MAX_VALUE
    isFinished = (watermark.getMillis() >= GlobalWindow.TIMESTAMP_MAX_VALUE.getMillis());
    return watermark.getMillis();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public List<String> getLocations() throws Exception {
    return new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    // reader.close();
  }
}
