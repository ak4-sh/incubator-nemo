/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Writes the cumulative Kafka input total expected by Nemo's source.log reader.
 *
 * <p>This process deliberately observes Kafka rather than producer stdout. It therefore provides
 * the same telemetry boundary for any producer implementation. Each output line is flushed and
 * has the legacy {@code N events} format consumed by {@code JobLauncher}.
 */
public final class KafkaInputOffsetTelemetry {
  private static final int DEFAULT_INTERVAL_MS = 1000;
  private static final int DEFAULT_TIMEOUT_SECONDS = 900;

  private KafkaInputOffsetTelemetry() {}

  private static void usage() {
    System.err.println(
        "Usage: KafkaInputOffsetTelemetry <bootstrapServers> <topicsCsv> "
            + "<expectedPartitionsPerTopic> <expectedTotal> <sourceLog> <diagnosticsCsv> "
            + "[intervalMs] [timeoutSeconds] [requireZeroStart]");
  }

  private static long parsePositiveLong(final String value, final String label) {
    final long parsed = Long.parseLong(value);
    if (parsed <= 0) {
      throw new IllegalArgumentException(label + " must be positive");
    }
    return parsed;
  }

  private static List<String> parseTopics(final String topicsCsv) {
    final List<String> topics = new ArrayList<>();
    final Set<String> unique = new HashSet<>();
    for (final String rawTopic : topicsCsv.split(",")) {
      final String topic = rawTopic.trim();
      if (topic.isEmpty() || !unique.add(topic)) {
        throw new IllegalArgumentException("Topics must be non-empty and unique: " + topicsCsv);
      }
      topics.add(topic);
    }
    if (topics.isEmpty()) {
      throw new IllegalArgumentException("At least one topic is required");
    }
    return topics;
  }

  private static void ensureParent(final File file) throws IOException {
    final File parent = file.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.isDirectory()) {
      throw new IOException("Cannot create output directory " + parent);
    }
  }

  private static List<TopicPartition> discoverPartitions(
      final KafkaConsumer<byte[], byte[]> consumer,
      final Collection<String> topics,
      final int expectedPartitionsPerTopic) {
    final List<TopicPartition> partitions = new ArrayList<>();
    for (final String topic : topics) {
      final List<PartitionInfo> topicPartitions = consumer.partitionsFor(topic);
      if (topicPartitions == null || topicPartitions.size() != expectedPartitionsPerTopic) {
        throw new IllegalStateException(
            "Topic "
                + topic
                + " has "
                + (topicPartitions == null ? 0 : topicPartitions.size())
                + " partitions; expected "
                + expectedPartitionsPerTopic);
      }
      final Set<Integer> ids = new HashSet<>();
      for (final PartitionInfo partition : topicPartitions) {
        ids.add(partition.partition());
        partitions.add(new TopicPartition(topic, partition.partition()));
      }
      for (int partition = 0; partition < expectedPartitionsPerTopic; partition++) {
        if (!ids.contains(partition)) {
          throw new IllegalStateException(
              "Topic " + topic + " is missing partition " + partition);
        }
      }
    }
    return partitions;
  }

  private static long sumOffsets(
      final Collection<TopicPartition> partitions,
      final Map<TopicPartition, Long> offsets) {
    long total = 0;
    for (final TopicPartition partition : partitions) {
      final Long offset = offsets.get(partition);
      if (offset == null || offset < 0) {
        throw new IllegalStateException("Missing or invalid end offset for " + partition);
      }
      total = Math.addExact(total, offset);
    }
    return total;
  }

  private static void validateMonotonic(
      final Collection<TopicPartition> partitions,
      final Map<TopicPartition, Long> previous,
      final Map<TopicPartition, Long> current) {
    for (final TopicPartition partition : partitions) {
      final Long previousOffset = previous.get(partition);
      final Long currentOffset = current.get(partition);
      if (previousOffset == null || currentOffset == null || currentOffset < previousOffset) {
        throw new IllegalStateException(
            "Kafka end offset decreased for "
                + partition
                + ": "
                + previousOffset
                + " -> "
                + currentOffset);
      }
    }
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 6 || args.length > 9) {
      usage();
      System.exit(2);
    }

    final String bootstrapServers = args[0];
    final List<String> topics = parseTopics(args[1]);
    final int expectedPartitions = (int) parsePositiveLong(args[2], "expectedPartitions");
    final long expectedTotal = parsePositiveLong(args[3], "expectedTotal");
    final File sourceLog = new File(args[4]);
    final File diagnosticsCsv = new File(args[5]);
    final int intervalMs =
        args.length >= 7 ? (int) parsePositiveLong(args[6], "intervalMs") : DEFAULT_INTERVAL_MS;
    final int timeoutSeconds =
        args.length >= 8
            ? (int) parsePositiveLong(args[7], "timeoutSeconds")
            : DEFAULT_TIMEOUT_SECONDS;
    final boolean requireZeroStart = args.length < 9 || Boolean.parseBoolean(args[8]);

    ensureParent(sourceLog);
    ensureParent(diagnosticsCsv);

    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "sponge-input-telemetry-" + UUID.randomUUID());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        PrintWriter sourceWriter =
            new PrintWriter(new BufferedWriter(new FileWriter(sourceLog, true)));
        PrintWriter diagnosticsWriter =
            new PrintWriter(new BufferedWriter(new FileWriter(diagnosticsCsv, false)))) {
      final List<TopicPartition> partitions =
          discoverPartitions(consumer, topics, expectedPartitions);
      final Map<TopicPartition, Long> baseline =
          new HashMap<>(consumer.endOffsets(partitions));
      final long baselineTotal = sumOffsets(partitions, baseline);
      if (requireZeroStart && baselineTotal != 0) {
        throw new IllegalStateException(
            "Telemetry requires empty input topics, but starting end offsets total "
                + baselineTotal);
      }

      diagnosticsWriter.println(
          "timestamp,totalEvents,deltaEvents,remainingEvents,baselineOffsetTotal");
      diagnosticsWriter.flush();

      Map<TopicPartition, Long> previousOffsets = baseline;
      long previousTotal = 0;
      final long startedMs = System.currentTimeMillis();

      while (true) {
        final long sampleStartedMs = System.currentTimeMillis();
        final Map<TopicPartition, Long> currentOffsets =
            new HashMap<>(consumer.endOffsets(partitions));
        validateMonotonic(partitions, previousOffsets, currentOffsets);

        final long absoluteTotal = sumOffsets(partitions, currentOffsets);
        final long total = absoluteTotal - baselineTotal;
        final long delta = total - previousTotal;
        if (total < previousTotal) {
          throw new IllegalStateException(
              "Cumulative input decreased from " + previousTotal + " to " + total);
        }
        if (total > expectedTotal) {
          throw new IllegalStateException(
              "Cumulative input " + total + " exceeds expected total " + expectedTotal);
        }

        sourceWriter.println(total + " events");
        sourceWriter.flush();
        diagnosticsWriter.printf(
            "%d,%d,%d,%d,%d%n",
            sampleStartedMs, total, delta, expectedTotal - total, baselineTotal);
        diagnosticsWriter.flush();

        if (total == expectedTotal) {
          System.out.println(
              "KAFKA_INPUT_TELEMETRY_DONE total="
                  + total
                  + " elapsedMs="
                  + (System.currentTimeMillis() - startedMs));
          return;
        }
        if (System.currentTimeMillis() - startedMs > timeoutSeconds * 1000L) {
          throw new IllegalStateException(
              "Timed out after "
                  + timeoutSeconds
                  + "s at "
                  + total
                  + "/"
                  + expectedTotal
                  + " events");
        }

        previousOffsets = currentOffsets;
        previousTotal = total;
        final long sleepMs = intervalMs - (System.currentTimeMillis() - sampleStartedMs);
        if (sleepMs > 0) {
          Thread.sleep(sleepMs);
        }
      }
    }
  }
}
