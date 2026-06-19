import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils.RateShape;
import org.apache.beam.sdk.nexmark.NexmarkUtils.RateUnit;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TimestampedValue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class StandaloneNexmarkKafkaProducer {
  private StandaloneNexmarkKafkaProducer() {
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 6) {
      System.err.println("Usage two-phase:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <steadyEvents> <steadyRate> <burstEvents> <burstRate> [isRateLimited] [numGenerators]");
      System.err.println("Usage legacy BURSTY:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <numEvents> <firstRate> <nextRate> <periodSec> <isRateLimited> [numGenerators]");
      System.exit(1);
    }

    final String bootstrapServers = args[0];
    final String topic = args[1];

    if (args.length == 7 || args.length == 8) {
      final int numEvents = Integer.parseInt(args[2]);
      final int firstEventRate = Integer.parseInt(args[3]);
      final int nextEventRate = Integer.parseInt(args[4]);
      final int ratePeriodSec = Integer.parseInt(args[5]);
      final boolean isRateLimited = Boolean.parseBoolean(args[6]);
      final int numGenerators = args.length == 8 ? Integer.parseInt(args[7]) : 1;

      System.out.println("=== NEXMark Legacy Bursty Mode ===");
      System.out.println("  " + (ratePeriodSec - 1) + "s at " + firstEventRate + " ev/s");
      System.out.println("  1s at " + nextEventRate + " ev/s");
      System.out.println("  total " + numEvents + " events");
      System.out.println("  generators: " + numGenerators);

      runBurstyPhase(bootstrapServers, topic, numEvents, firstEventRate, nextEventRate,
        ratePeriodSec, isRateLimited, numGenerators);
    } else {
      final int steadyEvents = Integer.parseInt(args[2]);
      final int steadyRate = Integer.parseInt(args[3]);
      final int burstEvents = Integer.parseInt(args[4]);
      final int burstRate = Integer.parseInt(args[5]);
      final boolean isRateLimited = args.length >= 7 ? Boolean.parseBoolean(args[6]) : true;

      System.out.println("=== NEXMark Two-Phase Kafka Producer ===");
      System.out.println("Topic: " + topic);
      System.out.println("Phase 1: " + steadyEvents + " events @ " + steadyRate + " ev/s");
      System.out.println("Phase 2: " + burstEvents + " events @ " + burstRate + " ev/s");

      if (steadyEvents > 0) {
        runSquarePhase(bootstrapServers, topic, steadyEvents, steadyRate, isRateLimited, 0L, 1);
      }
      if (burstEvents > 0) {
        runSquarePhase(bootstrapServers, topic, burstEvents, burstRate, isRateLimited,
          (long) steadyEvents * 1_000_000L, 1);
      }
      System.out.println("Done! Sent " + ((long) steadyEvents + burstEvents) + " events.");
    }
  }

  private static void runSquarePhase(final String bootstrapServers,
                                     final String topic,
                                     final int numEvents,
                                     final int rate,
                                     final boolean isRateLimited,
                                     final long firstEventId,
                                     final int numGenerators) throws Exception {
    final NexmarkConfiguration config = new NexmarkConfiguration();
    config.numEvents = numEvents;
    config.rateShape = RateShape.SQUARE;
    config.firstEventRate = rate;
    config.nextEventRate = rate;
    config.rateUnit = RateUnit.PER_SECOND;
    config.ratePeriodSec = 1;
    config.isRateLimited = isRateLimited;
    config.numEventGenerators = 1;

    final Generator generator = new Generator(new GeneratorConfig(
      config, System.currentTimeMillis(), firstEventId, config.numEvents, firstEventId));
    runGenerator(bootstrapServers, topic, generator, numEvents, isRateLimited);
  }

  private static void runBurstyPhase(final String bootstrapServers,
                                     final String topic,
                                     final int numEvents,
                                     final int firstEventRate,
                                     final int nextEventRate,
                                     final int ratePeriodSec,
                                     final boolean isRateLimited,
                                     final int numGenerators) throws Exception {
    final int clampedGenerators = Math.min(numGenerators, numEvents);
    final long startTime = System.currentTimeMillis();

    // Shared metrics state
    final AtomicLong totalSent = new AtomicLong(0);
    final AtomicBoolean anyFailed = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(clampedGenerators);

    // Metrics CSV writer + source.log writer
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    final String metricsFile = outDir + "/producer_metrics.csv";
    final PrintWriter metricsWriter = new PrintWriter(new FileWriter(metricsFile, true));
    metricsWriter.println("timestamp,outputRate,totalSent,elapsedMs");
    final String sourceLogFile = outDir + "/source.log";
    final PrintWriter sourceLogWriter = new PrintWriter(new FileWriter(sourceLogFile, true));

    // Split events and rates across generators
    final long baseEventsPerGen = numEvents / clampedGenerators;
    final int remainder = numEvents % clampedGenerators;
    final long totalEventsPerGen = baseEventsPerGen + (remainder > 0 ? 1 : 0);
    final int firstRatePerGen = firstEventRate / clampedGenerators;
    final int nextRatePerGen = nextEventRate / clampedGenerators;

    System.out.println("  per-generator: " + firstRatePerGen + "/" + nextRatePerGen
      + " ev/s, ~" + baseEventsPerGen + " events each, " + clampedGenerators + " generators");

    // Create and start generator workers
    for (int i = 0; i < clampedGenerators; i++) {
      final long eventsForThisGen = (i < remainder) ? baseEventsPerGen + 1 : baseEventsPerGen;
      final long firstEventId = (long) i * baseEventsPerGen + Math.min(i, remainder);
      final int genIndex = i;

      final Thread worker = new Thread(() -> {
        try {
          runGeneratorWorker(bootstrapServers, topic, eventsForThisGen,
            firstRatePerGen, nextRatePerGen, ratePeriodSec, isRateLimited,
            firstEventId, totalSent, genIndex);
        } catch (Exception e) {
          anyFailed.set(true);
          System.err.println("Generator " + genIndex + " failed: " + e.getMessage());
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
      worker.setName("gen-" + i);
      worker.start();
    }

    // Metrics reporter + source.log writer thread
    final Thread metricsReporter = new Thread(() -> {
      long lastMetricsWrite = startTime;
      long lastSourceLogWrite = startTime;
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        final long now = System.currentTimeMillis();
        final long sent = totalSent.get();
        final long elapsed = now - startTime;
        if (elapsed <= 0) continue;

        // Write producer metrics CSV every 5s
        if (now - lastMetricsWrite >= 5000) {
          final double rate = sent * 1000.0 / elapsed;
          synchronized (metricsWriter) {
            metricsWriter.printf("%d,%.2f,%d,%d%n", now, rate, sent, elapsed);
            metricsWriter.flush();
          }
          lastMetricsWrite = now;
        }

        // Write source.log every 1s with actual rate
        if (now - lastSourceLogWrite >= 1000) {
          // Compute rate over last second
          final long currentRate = sent * 1000 / elapsed;
          synchronized (sourceLogWriter) {
            sourceLogWriter.printf("%d events%n", sent);
            sourceLogWriter.flush();
          }
          lastSourceLogWrite = now;
        }

        if (latch.getCount() == 0) {
          break;
        }
      }
    });
    metricsReporter.setDaemon(true);
    metricsReporter.start();

    // Wait for all workers
    latch.await();

    // Workers closed their own producers; do final writes
    metricsReporter.interrupt();
    try { metricsReporter.join(2000); } catch (InterruptedException ignored) {}

    final long total = totalSent.get();
    final long elapsed = System.currentTimeMillis() - startTime;
    final double avgRate = total * 1000.0 / Math.max(1, elapsed);
    System.out.printf("  All generators done: %d events in %d ms (%.1f ev/s avg)%n",
      total, elapsed, avgRate);
    System.out.printf("KAFKA_PRODUCER_DONE topic=%s totalSent=%d elapsedMs=%d avgRate=%.2f%n",
      topic, total, elapsed, avgRate);
    synchronized (metricsWriter) {
      metricsWriter.printf("%d,%.2f,%d,%d%n", System.currentTimeMillis(), avgRate, total, elapsed);
      metricsWriter.flush();
      metricsWriter.close();
    }
    synchronized (sourceLogWriter) {
      sourceLogWriter.printf("%d events%n", total);
      sourceLogWriter.flush();
      sourceLogWriter.close();
    }

    if (anyFailed.get()) {
      throw new RuntimeException("One or more generators failed");
    }
  }

  private static void runGeneratorWorker(final String bootstrapServers,
                                         final String topic,
                                         final long numEvents,
                                         final int firstEventRate,
                                         final int nextEventRate,
                                         final int ratePeriodSec,
                                         final boolean isRateLimited,
                                         final long firstEventId,
                                         final AtomicLong totalSent,
                                         final int genIndex) throws Exception {
    final NexmarkConfiguration config = new NexmarkConfiguration();
    config.numEvents = (int) numEvents;
    config.rateShape = RateShape.BURSTY;
    config.firstEventRate = firstEventRate;
    config.nextEventRate = nextEventRate;
    config.rateUnit = RateUnit.PER_SECOND;
    config.ratePeriodSec = ratePeriodSec;
    config.isRateLimited = isRateLimited;
    config.numEventGenerators = 1;

    final Generator generator = new Generator(new GeneratorConfig(
      config, System.currentTimeMillis(), firstEventId, config.numEvents, firstEventId));

    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
    long localSent = 0;
    final int BATCH_SIZE = 500;

    while (generator.hasNext()) {
      long batchDelayUs = 0;
      int producedInBatch = 0;

      while (producedInBatch < BATCH_SIZE && generator.hasNext()) {
        final TimestampedValue<Event> tv = generator.next();
        final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
        producer.send(new ProducerRecord<>(topic, bytes));
        localSent++;
        totalSent.incrementAndGet();
        batchDelayUs += generator.currentInterEventDelayUs();
        producedInBatch++;
      }

      // Pace to match target rate using accumulated inter-event delay
      if (batchDelayUs > 2000) {
        Thread.sleep(batchDelayUs / 1000, (int) (batchDelayUs % 1000) * 1000);
      } else if (batchDelayUs > 0) {
        // Spin-wait for sub-millisecond accuracy
        final long deadline = System.nanoTime() + batchDelayUs * 1000;
        while (System.nanoTime() < deadline) {
          Thread.yield();
        }
      }
    }

    producer.flush();
    producer.close();

    System.out.printf("  [gen-%d] done: %d events%n", genIndex, localSent);
  }

  private static void runGenerator(final String bootstrapServers,
                                   final String topic,
                                   final Generator generator,
                                   final int numEvents,
                                   final boolean isRateLimited) throws Exception {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
    long totalSent = 0;
    final long startTime = System.currentTimeMillis();
    long lastLogTime = startTime;
    long lastMetricTime = startTime;

    // Metrics CSV writer
    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    final String metricsFile = outDir + "/producer_metrics.csv";
    final PrintWriter metricsWriter = new PrintWriter(new FileWriter(metricsFile, true));
    metricsWriter.println("timestamp,outputRate,totalSent,elapsedMs");

    while (generator.hasNext()) {
      final TimestampedValue<Event> tv = generator.next();
      final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
      producer.send(new ProducerRecord<>(topic, bytes));
      totalSent++;

      final long now = System.currentTimeMillis();
      if (now - lastLogTime >= 1000) {
        final long elapsed = now - startTime;
        final double rate = totalSent * 1000.0 / elapsed;
        System.out.printf("  [%5ds] sent %6d/%6d events (%-8.0f ev/s)%n",
          elapsed / 1000, totalSent, numEvents, rate);
        lastLogTime = now;
      }

      if (now - lastMetricTime >= 5000) {
        final long elapsed = now - startTime;
        final double rate = totalSent * 1000.0 / elapsed;
        metricsWriter.printf("%d,%.2f,%d,%d%n", now, rate, totalSent, elapsed);
        metricsWriter.flush();
        lastMetricTime = now;
      }

      if (isRateLimited) {
        final long delayUs = generator.currentInterEventDelayUs();
        Thread.sleep(delayUs / 1000, (int) (delayUs % 1000) * 1000);
      }
    }

    producer.flush();
    final long elapsed = System.currentTimeMillis() - startTime;
    System.out.printf("  Phase done: %d events in %d ms (%.1f ev/s avg)%n",
      totalSent, elapsed, totalSent * 1000.0 / Math.max(1, elapsed));
    System.out.printf("KAFKA_PRODUCER_DONE topic=%s totalSent=%d elapsedMs=%d avgRate=%.2f%n",
      topic, totalSent, elapsed, totalSent * 1000.0 / Math.max(1, elapsed));
    metricsWriter.printf("%d,%.2f,%d,%d%n", System.currentTimeMillis(),
      totalSent * 1000.0 / Math.max(1, elapsed), totalSent, elapsed);
    metricsWriter.close();
    producer.close();
  }
}
