#!/usr/bin/env python3

import importlib.util
import pathlib
import tempfile
import unittest


SCRIPT_PATH = pathlib.Path(__file__).with_name("metrics_collector.py")
SPEC = importlib.util.spec_from_file_location("metrics_collector", SCRIPT_PATH)
METRICS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(METRICS)


class MetricsCollectorTest(unittest.TestCase):
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
