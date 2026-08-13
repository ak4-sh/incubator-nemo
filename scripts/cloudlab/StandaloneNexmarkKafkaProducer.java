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
      System.err.println("Usage custom burst (default):");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <steadyRate> <burstRate> <steadySec> <burstSec> <numBursts> <isRateLimited> [numGenerators] [rampUpSec] [maxEvents]");
      System.err.println("Usage step sweep:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> STEP <startRate> <stepRate> <stepSec> <numSteps> <isRateLimited> [numGenerators] [maxEvents]");
      System.err.println("Usage explicit phases:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> PHASES <ratesCsv> <durationsCsv> <isRateLimited> [numGenerators] [maxEvents]");
      System.err.println("Usage legacy BURSTY:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <numEvents> <firstRate> <nextRate> <periodSec> <isRateLimited> [numGenerators]");
      System.err.println("Usage two-phase:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <steadyEvents> <steadyRate> <burstEvents> <burstRate> [isRateLimited]");
      System.exit(1);
    }

    final String bootstrapServers = args[0];
    final String topic = args[1];

    if ("STEP".equalsIgnoreCase(args[2])) {
      if (args.length < 8) {
        throw new IllegalArgumentException("STEP mode requires at least 8 arguments");
      }
      final int startRate = Integer.parseInt(args[3]);
      final int stepRate = Integer.parseInt(args[4]);
      final int stepDurationSec = Integer.parseInt(args[5]);
      final int numSteps = Integer.parseInt(args[6]);
      final boolean isRateLimited = Boolean.parseBoolean(args[7]);
      final int numGenerators = args.length >= 9 ? Integer.parseInt(args[8]) : 8;
      final long maxEvents = args.length >= 10 ? Long.parseLong(args[9]) : 0L;

      System.out.println("=== NEXMark Step Sweep Mode ===");
      System.out.println("  startRate: " + startRate + " ev/s");
      System.out.println("  stepRate: " + stepRate + " ev/s");
      System.out.println("  stepDuration: " + stepDurationSec + "s");
      System.out.println("  numSteps: " + numSteps);
      System.out.println("  generators: " + numGenerators);
      if (maxEvents > 0) {
        System.out.println("  maxEvents: " + maxEvents);
      }

      runStepSweepPhase(bootstrapServers, topic, startRate, stepRate,
        stepDurationSec, numSteps, isRateLimited, numGenerators, maxEvents);
      return;
    }

    if ("PHASES".equalsIgnoreCase(args[2])) {
      if (args.length < 6) {
        throw new IllegalArgumentException("PHASES mode requires at least 6 arguments");
      }
      final int[] rates = parsePositiveIntCsv(args[3], "ratesCsv");
      final int[] durationsSec = parsePositiveIntCsv(args[4], "durationsCsv");
      if (rates.length != durationsSec.length) {
        throw new IllegalArgumentException("ratesCsv and durationsCsv must have the same length");
      }
      final boolean isRateLimited = Boolean.parseBoolean(args[5]);
      final int numGenerators = args.length >= 7 ? Integer.parseInt(args[6]) : 8;
      final long maxEvents = args.length >= 8 ? Long.parseLong(args[7]) : 0L;

      System.out.println("=== NEXMark Explicit Phases Mode ===");
      for (int i = 0; i < rates.length; i++) {
        System.out.println("  phase " + (i + 1) + ": " + durationsSec[i]
          + "s at " + rates[i] + " ev/s");
      }
      System.out.println("  generators: " + numGenerators);
      if (maxEvents > 0) {
        System.out.println("  maxEvents: " + maxEvents);
      }

      runPhasesPhase(bootstrapServers, topic, rates, durationsSec, isRateLimited,
        numGenerators, maxEvents);
      return;
    }

    // Detect mode: custom burst if args[6] (numBursts) is an integer > 0 and args[5] (burstSec) is also an integer
    // Distinguish from legacy BURSTY (7-8 args) where args[6] is boolean string
    // Custom burst: args[6] is integer numBursts
    boolean isCustomBurst = false;
    if (args.length >= 8) {
      try {
        int numBursts = Integer.parseInt(args[6]);
        int burstSec = Integer.parseInt(args[5]);
        // If numBursts > 0 and burstSec is reasonable, treat as custom burst
        if (numBursts > 0 && burstSec > 0) {
          isCustomBurst = true;
        }
      } catch (NumberFormatException e) {
        // args[6] is not an integer, so it's legacy BURSTY (isRateLimited boolean)
        isCustomBurst = false;
      }
    }

    if (isCustomBurst) {
      // Custom burst mode
      final int steadyRate = Integer.parseInt(args[2]);
      final int burstRate = Integer.parseInt(args[3]);
      final int steadyDurationSec = Integer.parseInt(args[4]);
      final int burstDurationSec = Integer.parseInt(args[5]);
      final int numBursts = Integer.parseInt(args[6]);
      final boolean isRateLimited = Boolean.parseBoolean(args[7]);
      final int numGenerators = args.length >= 9 ? Integer.parseInt(args[8]) : 8;
      final int rampUpSec = args.length >= 10 ? Integer.parseInt(args[9]) : 60;
      final long maxEvents = args.length >= 11 ? Long.parseLong(args[10]) : 0L;

      System.out.println("=== NEXMark Custom Burst Mode ===");
      System.out.println("  steadyRate: " + steadyRate + " ev/s");
      System.out.println("  burstRate: " + burstRate + " ev/s");
      System.out.println("  steadyDuration: " + steadyDurationSec + "s");
      System.out.println("  burstDuration: " + burstDurationSec + "s");
      System.out.println("  numBursts: " + numBursts);
      System.out.println("  rampUpSec: " + rampUpSec + "s");
      System.out.println("  generators: " + numGenerators);
      if (maxEvents > 0) {
        System.out.println("  maxEvents: " + maxEvents);
      }

      runCustomBurstPhase(bootstrapServers, topic, steadyRate, burstRate,
        steadyDurationSec, burstDurationSec, numBursts, rampUpSec,
        isRateLimited, numGenerators, maxEvents);
    } else if (args.length == 7 || args.length == 8) {
      // Legacy BURSTY mode
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
      // Two-phase mode
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

  // ─────────────────────────────────────────────────────────────
  // Step Sweep Phase
  // ─────────────────────────────────────────────────────────────
  private static int[] parsePositiveIntCsv(final String csv, final String name) {
    final String[] parts = csv.split(",");
    final int[] values = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      values[i] = Integer.parseInt(parts[i].trim());
      if (values[i] <= 0) {
        throw new IllegalArgumentException(name + " must contain positive integers");
      }
    }
    return values;
  }

  private static void runStepSweepPhase(final String bootstrapServers,
                                         final String topic,
                                         final int startRate,
                                         final int stepRate,
                                         final int stepDurationSec,
                                         final int numSteps,
                                         final boolean isRateLimited,
                                         final int numGenerators,
                                         final long maxEvents) throws Exception {
    if (startRate <= 0 || stepRate < 0 || stepDurationSec <= 0 || numSteps <= 0) {
      throw new IllegalArgumentException("Invalid step sweep parameters");
    }

    final int clampedGenerators = Math.max(1, Math.min(numGenerators, 64));
    final long startTime = System.currentTimeMillis();
    long plannedEvents = 0;
    for (int step = 0; step < numSteps; step++) {
      plannedEvents += (long) (startRate + step * stepRate) * stepDurationSec;
    }
    final long totalEvents = maxEvents > 0 ? Math.min(plannedEvents, maxEvents) : plannedEvents;

    final AtomicLong totalSent = new AtomicLong(0);
    final AtomicBoolean anyFailed = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(clampedGenerators);

    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    final PrintWriter metricsWriter = new PrintWriter(new FileWriter(outDir + "/producer_metrics.csv", true));
    metricsWriter.println("timestamp,outputRate,totalSent,elapsedMs,targetRate");
    final PrintWriter sourceLogWriter = new PrintWriter(new FileWriter(outDir + "/source.log", true));

    final long baseEventsPerGen = totalEvents / clampedGenerators;
    final int remainder = (int) (totalEvents % clampedGenerators);
    System.out.println("  plannedEvents: " + plannedEvents);
    System.out.println("  totalEvents: " + totalEvents + " (~" + baseEventsPerGen + " per gen)");

    for (int i = 0; i < clampedGenerators; i++) {
      final long eventsForThisGen = (i < remainder) ? baseEventsPerGen + 1 : baseEventsPerGen;
      final long firstEventId = (long) i * baseEventsPerGen + Math.min(i, remainder);
      final int genIndex = i;
      final Thread worker = new Thread(() -> {
        try {
          runStepSweepWorker(bootstrapServers, topic, eventsForThisGen,
            startRate, stepRate, stepDurationSec, numSteps, clampedGenerators,
            isRateLimited, firstEventId, totalSent, genIndex);
        } catch (Exception e) {
          anyFailed.set(true);
          System.err.println("Generator " + genIndex + " failed: " + e.getMessage());
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
      worker.setName("step-gen-" + i);
      worker.start();
    }

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
        if (elapsed <= 0) {
          continue;
        }

        if (now - lastMetricsWrite >= 5000) {
          final double rate = sent * 1000.0 / elapsed;
          final long elapsedSec = elapsed / 1000;
          final int stepIndex = (int) Math.min(numSteps - 1, elapsedSec / stepDurationSec);
          final int targetRate = startRate + stepIndex * stepRate;
          synchronized (metricsWriter) {
            metricsWriter.printf("%d,%.2f,%d,%d,%d%n", now, rate, sent, elapsed, targetRate);
            metricsWriter.flush();
          }
          lastMetricsWrite = now;
        }

        if (now - lastSourceLogWrite >= 1000) {
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

    latch.await();

    metricsReporter.interrupt();
    try { metricsReporter.join(2000); } catch (InterruptedException ignored) {}

    final long total = totalSent.get();
    final long elapsed = System.currentTimeMillis() - startTime;
    final double avgRate = total * 1000.0 / Math.max(1, elapsed);
    System.out.printf("  Step sweep done: %d events in %d ms (%.1f ev/s avg)%n",
      total, elapsed, avgRate);
    System.out.printf("KAFKA_PRODUCER_DONE topic=%s totalSent=%d elapsedMs=%d avgRate=%.2f%n",
      topic, total, elapsed, avgRate);
    synchronized (metricsWriter) {
      metricsWriter.printf("%d,%.2f,%d,%d,%d%n", System.currentTimeMillis(), avgRate,
        total, elapsed, startRate + (numSteps - 1) * stepRate);
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

  private static void runStepSweepWorker(final String bootstrapServers,
                                          final String topic,
                                          final long numEvents,
                                          final int startRate,
                                          final int stepRate,
                                          final int stepDurationSec,
                                          final int numSteps,
                                          final int numGenerators,
                                          final boolean isRateLimited,
                                          final long firstEventId,
                                          final AtomicLong totalSent,
                                          final int genIndex) throws Exception {
    final NexmarkConfiguration config = new NexmarkConfiguration();
    config.numEvents = (int) Math.min(numEvents, Integer.MAX_VALUE);
    config.rateShape = RateShape.SQUARE;
    config.firstEventRate = Math.max(startRate + (numSteps - 1) * stepRate, 1);
    config.nextEventRate = config.firstEventRate;
    config.rateUnit = RateUnit.PER_SECOND;
    config.ratePeriodSec = 1;
    config.isRateLimited = false;
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
    final long workerStart = System.currentTimeMillis();

    for (int step = 0; step < numSteps && generator.hasNext(); step++) {
      final int targetRate = startRate + step * stepRate;
      final int ratePerGen = targetRate / numGenerators
        + (genIndex < targetRate % numGenerators ? 1 : 0);
      System.out.printf("  [gen-%d] step %d/%d %ds at %d ev/s%n",
        genIndex, step + 1, numSteps, stepDurationSec, ratePerGen);
      for (int sec = 0; sec < stepDurationSec && generator.hasNext(); sec++) {
        final long batchStart = System.currentTimeMillis();
        long sentThisSec = 0;
        while (sentThisSec < ratePerGen && generator.hasNext()) {
          final TimestampedValue<Event> tv = generator.next();
          final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
          producer.send(new ProducerRecord<>(topic, bytes));
          sentThisSec++;
          localSent++;
          totalSent.incrementAndGet();
        }
        if (isRateLimited) {
          final long elapsed = System.currentTimeMillis() - batchStart;
          if (elapsed < 1000) {
            Thread.sleep(1000 - elapsed);
          }
        }
      }
    }

    producer.flush();
    producer.close();

    final long elapsed = System.currentTimeMillis() - workerStart;
    System.out.printf("  [gen-%d] done: %d events in %d ms%n", genIndex, localSent, elapsed);
  }

  // ─────────────────────────────────────────────────────────────
  // Explicit Phases Mode
  // ─────────────────────────────────────────────────────────────
  private static void runPhasesPhase(final String bootstrapServers,
                                      final String topic,
                                      final int[] rates,
                                      final int[] durationsSec,
                                      final boolean isRateLimited,
                                      final int numGenerators,
                                      final long maxEvents) throws Exception {
    final int clampedGenerators = Math.max(1, Math.min(numGenerators, 64));
    final long startTime = System.currentTimeMillis();
    long plannedEvents = 0;
    for (int i = 0; i < rates.length; i++) {
      plannedEvents += (long) rates[i] * durationsSec[i];
    }
    final long totalEvents = maxEvents > 0 ? Math.min(plannedEvents, maxEvents) : plannedEvents;

    final AtomicLong totalSent = new AtomicLong(0);
    final AtomicBoolean anyFailed = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(clampedGenerators);

    final String workDir = System.getProperty("nemo.work.dir", System.getenv("NEMO_WORK_DIR"));
    final String outDir = workDir != null ? workDir : "/tmp";
    final PrintWriter metricsWriter = new PrintWriter(new FileWriter(outDir + "/producer_metrics.csv", true));
    metricsWriter.println("timestamp,outputRate,totalSent,elapsedMs,targetRate,phase");
    final PrintWriter sourceLogWriter = new PrintWriter(new FileWriter(outDir + "/source.log", true));
    final PrintWriter phaseWriter = new PrintWriter(new FileWriter(outDir + "/producer_phases.csv", true));
    phaseWriter.println("timestamp,event,phase,targetRate,durationSec,totalSent,elapsedMs");

    final long baseEventsPerGen = totalEvents / clampedGenerators;
    final int remainder = (int) (totalEvents % clampedGenerators);
    System.out.println("  plannedEvents: " + plannedEvents);
    System.out.println("  totalEvents: " + totalEvents + " (~" + baseEventsPerGen + " per gen)");

    for (int i = 0; i < clampedGenerators; i++) {
      final long eventsForThisGen = (i < remainder) ? baseEventsPerGen + 1 : baseEventsPerGen;
      final long firstEventId = (long) i * baseEventsPerGen + Math.min(i, remainder);
      final int genIndex = i;
      final Thread worker = new Thread(() -> {
        try {
          runPhasesWorker(bootstrapServers, topic, eventsForThisGen, rates, durationsSec,
            clampedGenerators, isRateLimited, firstEventId, totalSent, genIndex, phaseWriter,
            startTime);
        } catch (Exception e) {
          anyFailed.set(true);
          System.err.println("Generator " + genIndex + " failed: " + e.getMessage());
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
      worker.setName("phase-gen-" + i);
      worker.start();
    }

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
        if (elapsed <= 0) {
          continue;
        }

        if (now - lastMetricsWrite >= 5000) {
          final double rate = sent * 1000.0 / elapsed;
          final int phaseIndex = phaseIndexForElapsedMs(elapsed, durationsSec);
          synchronized (metricsWriter) {
            metricsWriter.printf("%d,%.2f,%d,%d,%d,%d%n", now, rate, sent, elapsed,
              rates[phaseIndex], phaseIndex + 1);
            metricsWriter.flush();
          }
          lastMetricsWrite = now;
        }

        if (now - lastSourceLogWrite >= 1000) {
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

    latch.await();

    metricsReporter.interrupt();
    try { metricsReporter.join(2000); } catch (InterruptedException ignored) {}

    final long total = totalSent.get();
    final long elapsed = System.currentTimeMillis() - startTime;
    final double avgRate = total * 1000.0 / Math.max(1, elapsed);
    System.out.printf("  Phases done: %d events in %d ms (%.1f ev/s avg)%n",
      total, elapsed, avgRate);
    System.out.printf("KAFKA_PRODUCER_DONE topic=%s totalSent=%d elapsedMs=%d avgRate=%.2f%n",
      topic, total, elapsed, avgRate);
    synchronized (metricsWriter) {
      final int phaseIndex = phaseIndexForElapsedMs(elapsed, durationsSec);
      metricsWriter.printf("%d,%.2f,%d,%d,%d,%d%n", System.currentTimeMillis(), avgRate,
        total, elapsed, rates[phaseIndex], phaseIndex + 1);
      metricsWriter.flush();
      metricsWriter.close();
    }
    synchronized (sourceLogWriter) {
      sourceLogWriter.printf("%d events%n", total);
      sourceLogWriter.flush();
      sourceLogWriter.close();
    }
    synchronized (phaseWriter) {
      phaseWriter.flush();
      phaseWriter.close();
    }

    if (anyFailed.get()) {
      throw new RuntimeException("One or more generators failed");
    }
  }

  private static int phaseIndexForElapsedMs(final long elapsedMs, final int[] durationsSec) {
    long elapsedSec = elapsedMs / 1000;
    for (int i = 0; i < durationsSec.length; i++) {
      if (elapsedSec < durationsSec[i]) {
        return i;
      }
      elapsedSec -= durationsSec[i];
    }
    return durationsSec.length - 1;
  }

  private static void runPhasesWorker(final String bootstrapServers,
                                       final String topic,
                                       final long numEvents,
                                       final int[] rates,
                                       final int[] durationsSec,
                                       final int numGenerators,
                                       final boolean isRateLimited,
                                       final long firstEventId,
                                       final AtomicLong totalSent,
                                       final int genIndex,
                                       final PrintWriter phaseWriter,
                                       final long globalStartTime) throws Exception {
    final NexmarkConfiguration config = new NexmarkConfiguration();
    config.numEvents = (int) Math.min(numEvents, Integer.MAX_VALUE);
    config.rateShape = RateShape.SQUARE;
    config.firstEventRate = 1;
    for (final int rate : rates) {
      config.firstEventRate = Math.max(config.firstEventRate, rate);
    }
    config.nextEventRate = config.firstEventRate;
    config.rateUnit = RateUnit.PER_SECOND;
    config.ratePeriodSec = 1;
    config.isRateLimited = false;
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
    final long workerStart = System.currentTimeMillis();

    for (int phase = 0; phase < rates.length && generator.hasNext(); phase++) {
      final int targetRate = rates[phase];
      final int ratePerGen = targetRate / numGenerators
        + (genIndex < targetRate % numGenerators ? 1 : 0);
      final long phaseStart = System.currentTimeMillis();
      if (genIndex == 0) {
        synchronized (phaseWriter) {
          phaseWriter.printf("%d,start,%d,%d,%d,%d,%d%n", phaseStart, phase + 1,
            targetRate, durationsSec[phase], totalSent.get(), phaseStart - globalStartTime);
          phaseWriter.flush();
        }
      }
      System.out.printf("  [gen-%d] phase %d/%d %ds at %d ev/s%n",
        genIndex, phase + 1, rates.length, durationsSec[phase], ratePerGen);
      for (int sec = 0; sec < durationsSec[phase] && generator.hasNext(); sec++) {
        final long batchStart = System.currentTimeMillis();
        long sentThisSec = 0;
        while (sentThisSec < ratePerGen && generator.hasNext()) {
          final TimestampedValue<Event> tv = generator.next();
          final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
          producer.send(new ProducerRecord<>(topic, bytes));
          sentThisSec++;
          localSent++;
          totalSent.incrementAndGet();
        }
        if (isRateLimited) {
          final long elapsed = System.currentTimeMillis() - batchStart;
          if (elapsed < 1000) {
            Thread.sleep(1000 - elapsed);
          }
        }
      }
      final long phaseEnd = System.currentTimeMillis();
      if (genIndex == 0) {
        synchronized (phaseWriter) {
          phaseWriter.printf("%d,end,%d,%d,%d,%d,%d%n", phaseEnd, phase + 1,
            targetRate, durationsSec[phase], totalSent.get(), phaseEnd - globalStartTime);
          phaseWriter.flush();
        }
      }
    }

    producer.flush();
    producer.close();

    final long elapsed = System.currentTimeMillis() - workerStart;
    System.out.printf("  [gen-%d] done: %d events in %d ms%n", genIndex, localSent, elapsed);
  }

  // ─────────────────────────────────────────────────────────────
  // Custom Burst Phase
  // ─────────────────────────────────────────────────────────────
  private static void runCustomBurstPhase(final String bootstrapServers,
                                           final String topic,
                                           final int steadyRate,
                                           final int burstRate,
                                           final int steadyDurationSec,
                                           final int burstDurationSec,
                                           final int numBursts,
                                           final int rampUpSec,
                                           final boolean isRateLimited,
                                           final int numGenerators,
                                           final long maxEvents) throws Exception {
    final int clampedGenerators = Math.max(1, Math.min(numGenerators, 64));
    final long startTime = System.currentTimeMillis();

    // Compute total events
    final long rampUpEvents = (long) rampUpSec * steadyRate;
    final long cycleEvents = (long) steadyDurationSec * steadyRate + (long) burstDurationSec * burstRate;
    long totalEvents = rampUpEvents + (long) numBursts * cycleEvents;
    // If maxEvents is specified, cap the total
    if (maxEvents > 0) {
      totalEvents = Math.min(totalEvents, maxEvents);
    }

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

    // Events per generator
    final long baseEventsPerGen = totalEvents / clampedGenerators;
    final int remainder = (int) (totalEvents % clampedGenerators);
    final int steadyRatePerGen = steadyRate / clampedGenerators;
    final int burstRatePerGen = burstRate / clampedGenerators;

    System.out.println("  totalEvents: " + totalEvents + " (~" + baseEventsPerGen + " per gen)");
    System.out.println("  per-generator: steady=" + steadyRatePerGen + " burst=" + burstRatePerGen + " ev/s");

    // Create and start generator workers
    for (int i = 0; i < clampedGenerators; i++) {
      final long eventsForThisGen = (i < remainder) ? baseEventsPerGen + 1 : baseEventsPerGen;
      final long firstEventId = (long) i * baseEventsPerGen + Math.min(i, remainder);
      final int genIndex = i;

      final Thread worker = new Thread(() -> {
        try {
          runCustomBurstWorker(bootstrapServers, topic, eventsForThisGen,
            steadyRatePerGen, burstRatePerGen,
            steadyDurationSec, burstDurationSec, numBursts, rampUpSec,
            isRateLimited, firstEventId, totalSent, genIndex);
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

  // ─────────────────────────────────────────────────────────────
  // Custom Burst Worker
  // ─────────────────────────────────────────────────────────────
  private static void runCustomBurstWorker(final String bootstrapServers,
                                            final String topic,
                                            final long numEvents,
                                            final int steadyRatePerGen,
                                            final int burstRatePerGen,
                                            final int steadyDurationSec,
                                            final int burstDurationSec,
                                            final int numBursts,
                                            final int rampUpSec,
                                            final boolean isRateLimited,
                                            final long firstEventId,
                                            final AtomicLong totalSent,
                                            final int genIndex) throws Exception {
    // Create generator with all events, isRateLimited=false so we control timing
    final NexmarkConfiguration config = new NexmarkConfiguration();
    config.numEvents = (int) Math.min(numEvents, Integer.MAX_VALUE);
    config.rateShape = RateShape.SQUARE;
    config.firstEventRate = Math.max(steadyRatePerGen, burstRatePerGen);
    config.nextEventRate = Math.max(steadyRatePerGen, burstRatePerGen);
    config.rateUnit = RateUnit.PER_SECOND;
    config.ratePeriodSec = 1;
    config.isRateLimited = false; // We control timing
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
    final long workerStart = System.currentTimeMillis();

    // Helper: send N events in a second
    java.util.function.IntConsumer sendSecond = (targetRate) -> {
      // Not used directly; we use inline loop below
    };

    // 1. Ramp-up phase
    if (rampUpSec > 0) {
      System.out.printf("  [gen-%d] ramp-up %ds at %d ev/s%n", genIndex, rampUpSec, steadyRatePerGen);
      for (int sec = 0; sec < rampUpSec && generator.hasNext(); sec++) {
        long batchStart = System.currentTimeMillis();
        long sentThisSec = 0;
        while (sentThisSec < steadyRatePerGen && generator.hasNext()) {
          final TimestampedValue<Event> tv = generator.next();
          final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
          producer.send(new ProducerRecord<>(topic, bytes));
          sentThisSec++;
          localSent++;
          totalSent.incrementAndGet();
        }
        long elapsed = System.currentTimeMillis() - batchStart;
        if (elapsed < 1000) {
          Thread.sleep(1000 - elapsed);
        }
      }
    }

    // 2. Burst cycles
    for (int burst = 0; burst < numBursts; burst++) {
      // Steady phase
      System.out.printf("  [gen-%d] burst %d/%d steady %ds at %d ev/s%n",
        genIndex, burst + 1, numBursts, steadyDurationSec, steadyRatePerGen);
      for (int sec = 0; sec < steadyDurationSec && generator.hasNext(); sec++) {
        long batchStart = System.currentTimeMillis();
        long sentThisSec = 0;
        while (sentThisSec < steadyRatePerGen && generator.hasNext()) {
          final TimestampedValue<Event> tv = generator.next();
          final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
          producer.send(new ProducerRecord<>(topic, bytes));
          sentThisSec++;
          localSent++;
          totalSent.incrementAndGet();
        }
        long elapsed = System.currentTimeMillis() - batchStart;
        if (elapsed < 1000) {
          Thread.sleep(1000 - elapsed);
        }
      }

      // Burst phase
      System.out.printf("  [gen-%d] burst %d/%d burst %ds at %d ev/s%n",
        genIndex, burst + 1, numBursts, burstDurationSec, burstRatePerGen);
      for (int sec = 0; sec < burstDurationSec && generator.hasNext(); sec++) {
        long batchStart = System.currentTimeMillis();
        long sentThisSec = 0;
        while (sentThisSec < burstRatePerGen && generator.hasNext()) {
          final TimestampedValue<Event> tv = generator.next();
          final byte[] bytes = CoderUtils.encodeToByteArray(Event.CODER, tv.getValue());
          producer.send(new ProducerRecord<>(topic, bytes));
          sentThisSec++;
          localSent++;
          totalSent.incrementAndGet();
        }
        long elapsed = System.currentTimeMillis() - batchStart;
        if (elapsed < 1000) {
          Thread.sleep(1000 - elapsed);
        }
      }
    }

    producer.flush();
    producer.close();

    final long elapsed = System.currentTimeMillis() - workerStart;
    System.out.printf("  [gen-%d] done: %d events in %d ms%n", genIndex, localSent, elapsed);
  }

  // ─────────────────────────────────────────────────────────────
  // Legacy Bursty Phase (unchanged)
  // ─────────────────────────────────────────────────────────────
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

  // ─────────────────────────────────────────────────────────────
  // Legacy Generator Worker (unchanged)
  // ─────────────────────────────────────────────────────────────
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

  // ─────────────────────────────────────────────────────────────
  // Square Phase (unchanged)
  // ─────────────────────────────────────────────────────────────
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

  // ─────────────────────────────────────────────────────────────
  // Single Generator Run (unchanged)
  // ─────────────────────────────────────────────────────────────
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
