#!/usr/bin/env python3

import importlib.util
import csv
import pathlib
import tempfile
import unittest


SCRIPT_PATH = pathlib.Path(__file__).with_name("metrics_collector.py")
SPEC = importlib.util.spec_from_file_location("metrics_collector", SCRIPT_PATH)
METRICS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(METRICS)


class MetricsCollectorTest(unittest.TestCase):
    def test_parse_active_consumer_group_output(self):
        output = """\
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
group nexmark-auction 0 10 12 2 consumer /host client
group nexmark-auction 1 20 20 0 consumer /host client
group nexmark-bid 0 30 35 5 consumer /host client
group nexmark-bid 1 40 40 0 consumer /host client
"""
        parsed = METRICS.parse_consumer_group_output(
            output, ["nexmark-auction", "nexmark-bid"], 2
        )
        self.assertEqual(parsed["nexmark-auction"]["consumerCommittedOffset"], 30)
        self.assertEqual(parsed["nexmark-auction"]["consumerLag"], 2)
        self.assertEqual(parsed["nexmark-bid"]["consumerLogEndOffset"], 75)
        self.assertEqual(parsed["nexmark-bid"]["consumerLag"], 5)

    def test_parse_inactive_consumer_group_output(self):
        output = """\
TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
nexmark-auction 0 10 10 0 - - -
nexmark-auction 1 20 20 0 - - -
nexmark-bid 0 30 30 0 - - -
nexmark-bid 1 40 40 0 - - -
"""
        parsed = METRICS.parse_consumer_group_output(
            output, ["nexmark-auction", "nexmark-bid"], 2
        )
        self.assertEqual(parsed["nexmark-auction"]["consumerCommittedOffset"], 30)
        self.assertEqual(parsed["nexmark-bid"]["consumerLogEndOffset"], 70)
        self.assertEqual(parsed["nexmark-auction"]["consumerLag"], 0)
        self.assertEqual(parsed["nexmark-bid"]["consumerLag"], 0)

    def test_parse_requires_expected_partition_count(self):
        output = "nexmark-auction 0 10 10 0 - - -\n"
        self.assertEqual(
            METRICS.parse_consumer_group_output(
                output, ["nexmark-auction"], expected_partitions=2
            ),
            {},
        )

    def test_prepare_csv_refuses_truncation_and_resumes_safely(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "metrics.csv"
            METRICS.prepare_csv(path, ["timestamp", "value"], resume=False)
            METRICS.append_csv_rows(
                path,
                ["timestamp", "value"],
                [{"timestamp": 1, "value": 2}],
            )
            with self.assertRaises(RuntimeError):
                METRICS.prepare_csv(path, ["timestamp", "value"], resume=False)
            METRICS.prepare_csv(path, ["timestamp", "value"], resume=True)
            METRICS.append_csv_rows(
                path,
                ["timestamp", "value"],
                [{"timestamp": 3, "value": 4}],
            )
            with path.open(newline="") as source:
                rows = list(csv.reader(source))
        self.assertEqual(
            rows,
            [["timestamp", "value"], ["1", "2"], ["3", "4"]],
        )

    def test_prepare_csv_rejects_incompatible_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "metrics.csv"
            path.write_text("wrong,header\n", encoding="utf-8")
            with self.assertRaises(RuntimeError):
                METRICS.prepare_csv(path, ["timestamp", "value"], resume=True)

    def test_queue_time_means_ignore_invalid_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "source_task_metrics.csv"
            path.write_text(
                "\n".join([
                    "timestamp,jobId,taskId,idleTimeNs,kafkaQueueTimeNs,kafkaQueueTimeAvgNs,kafkaQueueTimeMaxNs,kafkaQueueSamples,inputRate,recordsRead",
                    "1,job,source-0,0,0,1000000,1000000,1,10,10",
                    "1,job,source-0,0,0,1000000,1000000,1,10,10",
                    "2,job,source-1,0,0,3000000,3000000,3,10,10",
                    "3,job,source-2,0,0,-1,-1,0,10,10",
                    "4,job,source-3,0,0,9000000,9000000,0,10,10",
                ]) + "\n",
                encoding="utf-8",
            )

            metrics = METRICS.read_source_queue_metrics(tmp)

        self.assertEqual(metrics["validTasks"], 2)
        self.assertEqual(metrics["totalSamples"], 4)
        self.assertAlmostEqual(metrics["unweightedMean"], 2.0)
        self.assertAlmostEqual(metrics["weightedMean"], 2.5)
        self.assertAlmostEqual(metrics["p50"], 1.0)
        self.assertAlmostEqual(metrics["p95"], 3.0)
        self.assertAlmostEqual(metrics["p99"], 3.0)

    def test_queue_time_returns_empty_without_valid_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "source_task_metrics.csv"
            path.write_text(
                "\n".join([
                    "timestamp,jobId,taskId,idleTimeNs,kafkaQueueTimeNs,kafkaQueueTimeAvgNs,kafkaQueueTimeMaxNs,kafkaQueueSamples,inputRate,recordsRead",
                    "1,job,source-0,0,0,-1,-1,0,10,10",
                ]) + "\n",
                encoding="utf-8",
            )

            metrics = METRICS.read_source_queue_metrics(tmp)

        self.assertEqual(metrics, {})


if __name__ == "__main__":
    unittest.main()
