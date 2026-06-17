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

import java.util.Properties;

/** Standalone NEXMark Kafka producer that bypasses Nemo/Beam PUBLISH_ONLY. */
public final class StandaloneNexmarkKafkaProducer {
  private StandaloneNexmarkKafkaProducer() {
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 6) {
      System.err.println("Usage two-phase:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <steadyEvents> <steadyRate> <burstEvents> <burstRate> [isRateLimited]");
      System.err.println("Usage legacy BURSTY:");
      System.err.println("  StandaloneNexmarkKafkaProducer <bootstrap> <topic> <numEvents> <firstRate> <nextRate> <periodSec> <isRateLimited>");
      System.exit(1);
    }

    final String bootstrapServers = args[0];
    final String topic = args[1];

    if (args.length == 7) {
      final int numEvents = Integer.parseInt(args[2]);
      final int firstEventRate = Integer.parseInt(args[3]);
      final int nextEventRate = Integer.parseInt(args[4]);
      final int ratePeriodSec = Integer.parseInt(args[5]);
      final boolean isRateLimited = Boolean.parseBoolean(args[6]);

      System.out.println("=== NEXMark Legacy Bursty Mode ===");
      System.out.println("  " + (ratePeriodSec - 1) + "s at " + firstEventRate + " ev/s");
      System.out.println("  1s at " + nextEventRate + " ev/s");
      System.out.println("  total " + numEvents + " events");

      final NexmarkConfiguration config = new NexmarkConfiguration();
      config.numEvents = numEvents;
      config.rateShape = RateShape.BURSTY;
      config.firstEventRate = firstEventRate;
      config.nextEventRate = nextEventRate;
      config.rateUnit = RateUnit.PER_SECOND;
      config.ratePeriodSec = ratePeriodSec;
      config.isRateLimited = isRateLimited;
      config.numEventGenerators = 1;

      final Generator generator = new Generator(new GeneratorConfig(
        config, System.currentTimeMillis(), 0L, config.numEvents, 0L));
      runGenerator(bootstrapServers, topic, generator, numEvents, isRateLimited);
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
        runSquarePhase(bootstrapServers, topic, steadyEvents, steadyRate, isRateLimited, 0L);
      }
      if (burstEvents > 0) {
        runSquarePhase(bootstrapServers, topic, burstEvents, burstRate, isRateLimited,
          (long) steadyEvents * 1_000_000L);
      }
      System.out.println("Done! Sent " + ((long) steadyEvents + burstEvents) + " events.");
    }
  }

  private static void runSquarePhase(final String bootstrapServers,
                                     final String topic,
                                     final int numEvents,
                                     final int rate,
                                     final boolean isRateLimited,
                                     final long firstEventId) throws Exception {
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
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

    final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
    long totalSent = 0;
    final long startTime = System.currentTimeMillis();
    long lastLogTime = startTime;

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

      if (isRateLimited) {
        final long delayUs = generator.currentInterEventDelayUs();
        Thread.sleep(delayUs / 1000, (int) (delayUs % 1000) * 1000);
      }
    }

    producer.flush();
    final long elapsed = System.currentTimeMillis() - startTime;
    System.out.printf("  Phase done: %d events in %d ms (%.1f ev/s avg)%n",
      totalSent, elapsed, totalSent * 1000.0 / Math.max(1, elapsed));
    producer.close();
  }
}
