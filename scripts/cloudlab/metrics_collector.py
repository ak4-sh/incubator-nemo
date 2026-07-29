#!/usr/bin/env python3
"""
Metrics collector for Nemo autoscaler tests.
Polls Kafka offsets, reads source/task metrics, and produces combined_metrics.csv.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
import subprocess
import re
import shlex
from pathlib import Path

# ── configuration ─────────────────────────────────────────────────────────
INTERVAL_S = 5
KAFKA_HOME = os.environ.get("KAFKA_HOME", "/users/akash01/kafka")
KAFKA_NODE = os.environ.get("KAFKA_NODE", "node1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "node1:9092,node2:9092,node3:9092")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "disaggregated-streaming")
KAFKA_COMMAND_MODE = os.environ.get("KAFKA_COMMAND_MODE", "ssh").lower()
METRICS_RESUME = os.environ.get("METRICS_RESUME", "false").lower() == "true"
METRICS_JOB_ID = os.environ.get("METRICS_JOB_ID", "")
try:
    METRICS_MAX_CONSECUTIVE_FAILURES = int(
        os.environ.get("METRICS_MAX_CONSECUTIVE_FAILURES", "5")
    )
except ValueError:
    METRICS_MAX_CONSECUTIVE_FAILURES = 5
try:
    KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "0"))
except ValueError:
    KAFKA_PARTITIONS = 0


def kafka_tool_command(tool: str, *args: str) -> list[str]:
    command = [f"{KAFKA_HOME}/bin/{tool}", *args]
    if KAFKA_COMMAND_MODE == "local":
        return command
    if KAFKA_COMMAND_MODE == "ssh":
        return ["ssh", KAFKA_NODE, *command]
    raise ValueError(
        f"unsupported KAFKA_COMMAND_MODE={KAFKA_COMMAND_MODE}; expected local or ssh"
    )


def offset_commands(topic: str) -> list[list[str]]:
    return [
        kafka_tool_command(
            "kafka-run-class.sh",
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            KAFKA_BOOTSTRAP,
            "--topic",
            topic,
            "--time",
            "-1",
        ),
        kafka_tool_command(
            "kafka-get-offsets.sh",
            "--bootstrap-server",
            KAFKA_BOOTSTRAP,
            "--topic",
            topic,
            "--time",
            "-1",
        ),
    ]


def consumer_group_command(group: str) -> list[str]:
    return kafka_tool_command(
        "kafka-consumer-groups.sh",
        "--bootstrap-server",
        KAFKA_BOOTSTRAP,
        "--describe",
        "--group",
        group,
    )
COMBINED_COLUMNS = [
    "timestamp",
    "inputOffset",
    "resultOffset",
    "sourceCount",
    "inputLag",
    "resultLag",
    "kafkaQueueTimeP50",
    "kafkaQueueTimeP95",
    "kafkaQueueTimeP99",
    "latencyMedian",
    "latencyP95",
    "latencyP99",
    "latencyTail",
    "avgCpu",
    "avgInput",
    "avgProcess",
    "queueSize",
    "numExecutors",
    "numLambdaExecutors",
    "consumerCommittedOffset",
    "consumerLogEndOffset",
    "consumerLag",
    "kafkaQueueTimeUnweightedMeanMs",
    "kafkaQueueTimeWeightedMeanMs",
    "kafkaQueueTimeTotalSamples",
    "kafkaQueueTimeValidTasks",
]
TOPIC_COLUMNS = [
    "timestamp",
    "topic",
    "endOffset",
    "committedOffset",
    "consumerLogEndOffset",
    "consumerLag",
]

# ── helpers ───────────────────────────────────────────────────────────────
def get_topic_offset(topic: str, expected_partitions: int | None = None) -> int:
    """Return the latest offset for a topic, or -1 on error."""
    errors = []
    for cmd in offset_commands(topic):
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, timeout=10)
            total = 0
            count = 0
            for line in out.strip().splitlines():
                parts = line.strip().split(":")
                if len(parts) == 3 and parts[0] == topic:
                    total += int(parts[2])
                    count += 1
            if expected_partitions is not None and expected_partitions > 0:
                return total if count == expected_partitions else -1
            return total if count > 0 else -1
        except Exception as e:
            errors.append(f"{cmd}: {e}")
    print(f"[metrics] offset check failed: {'; '.join(errors)}", file=sys.stderr)
    return -1


def parse_consumer_group_output(
    output: str,
    topics: list[str],
    expected_partitions: int = 0,
) -> dict[str, dict]:
    """Parse both active- and inactive-consumer Kafka CLI table layouts."""
    requested = set(topics)
    totals = {
        topic: {
            "consumerCommittedOffset": 0,
            "consumerLogEndOffset": 0,
            "consumerLag": 0,
            "partitions": 0,
        }
        for topic in topics
    }
    for line in output.strip().splitlines():
        parts = line.split()
        if not parts or parts[0] in {"GROUP", "TOPIC", "Consumer"}:
            continue
        if parts[0] in requested:
            # Inactive group: TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
            topic_index, committed_index = 0, 2
        elif len(parts) > 1 and parts[1] in requested:
            # Active group: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
            topic_index, committed_index = 1, 3
        else:
            continue
        topic = parts[topic_index]
        try:
            totals[topic]["consumerCommittedOffset"] += int(parts[committed_index])
            totals[topic]["consumerLogEndOffset"] += int(parts[committed_index + 1])
            totals[topic]["consumerLag"] += int(parts[committed_index + 2])
            totals[topic]["partitions"] += 1
        except (IndexError, ValueError):
            continue
    return {
        topic: {
            key: value
            for key, value in metrics.items()
            if key != "partitions"
        }
        for topic, metrics in totals.items()
        if metrics["partitions"] > 0
        and (
            expected_partitions <= 0
            or metrics["partitions"] == expected_partitions
        )
    }


def get_consumer_group_lags(
    group: str,
    topics: list[str],
    expected_partitions: int | None = None,
) -> dict[str, dict]:
    """Return committed offset, log-end offset, and lag grouped by input topic."""
    if expected_partitions is None:
        expected_partitions = KAFKA_PARTITIONS
    cmd = consumer_group_command(group)
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, timeout=10)
    except Exception as e:
        print(f"[metrics] consumer-group lag check failed: {e}", file=sys.stderr)
        return {}
    return parse_consumer_group_output(out, topics, expected_partitions)


def get_consumer_group_lag(group: str, topic: str) -> dict:
    """Backward-compatible single-topic consumer lag helper."""
    return get_consumer_group_lags(group, [topic]).get(topic, {})


def parse_input_topics(value: str) -> list[str]:
    topics = [topic for topic in re.split(r"[,\s]+", value) if topic]
    if not topics:
        raise ValueError("at least one input topic is required")
    if len(topics) != len(set(topics)):
        raise ValueError(f"duplicate input topics are not allowed: {topics}")
    return topics


def aggregate_complete(values: dict[str, int], topics: list[str]) -> int:
    """Sum a metric only when every requested topic has a valid value."""
    if any(topic not in values or values[topic] < 0 for topic in topics):
        return -1
    return sum(values[topic] for topic in topics)


def prepare_csv(path: Path, columns: list[str], resume: bool) -> None:
    """Create a CSV safely, or validate it before explicitly resuming."""
    if path.exists() and path.stat().st_size > 0:
        with path.open(newline="") as source:
            header = next(csv.reader(source), [])
        if header != columns:
            raise RuntimeError(f"existing CSV has incompatible header: {path}")
        if not resume:
            raise RuntimeError(
                f"refusing to truncate existing metrics file {path}; "
                "set METRICS_RESUME=true to append"
            )
        return
    with path.open("w", newline="") as output:
        csv.DictWriter(output, fieldnames=columns).writeheader()
        output.flush()
        os.fsync(output.fileno())


def append_csv_rows(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("a", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writerows(rows)
        output.flush()
        os.fsync(output.fileno())


def _ssh_read_tail(host: str, filepath: str, lines: int = 50) -> list[str]:
    """Return the tail of a remote file via SSH."""
    try:
        out = subprocess.check_output(
            ["ssh", host, "tail", "-n", str(lines), shlex.quote(filepath)],
            stderr=subprocess.DEVNULL, text=True, timeout=5
        )
        return [line.strip() for line in out.splitlines() if line.strip()]
    except Exception:
        return []


def _read_tail_local(filepath: str, lines: int = 50) -> list[str]:
    """Return the tail of a local file."""
    try:
        with open(filepath) as f:
            data = f.readlines()
            return [line.strip() for line in data[-lines:] if line.strip()]
    except Exception:
        return []


def _read_tail(host: str | None, filepath: str, lines: int = 50) -> list[str]:
    if host:
        return _ssh_read_tail(host, filepath, lines)
    return _read_tail_local(filepath, lines)


def read_source_count(work_dir: str, input_offset: int, am_host: str | None = None,
                      last_valid: int = -1) -> tuple[int, int]:
    """Read the latest valid aggregate source count."""
    path = str(Path(work_dir) / "source_aggregate_metrics.csv")
    if am_host:
        path = "/tmp/source_aggregate_metrics.csv"
    for line in reversed(_read_tail(am_host, path)):
        if line.startswith("timestamp"):
            continue
        parts = line.split(",")
        if len(parts) >= 3:
            try:
                source = int(float(parts[2]))
                if source < 0:
                    continue
                if input_offset >= 0 and source > input_offset:
                    print(
                        f"[metrics] ignoring invalid sourceCount={source} > inputOffset={input_offset}",
                        file=sys.stderr,
                    )
                    continue
                return source, source
            except ValueError:
                continue
    return last_valid, last_valid


def read_latency(log_file: str) -> dict:
    """Parse the latest COLLECT_LATENCY line from the subscriber log."""
    if not os.path.exists(log_file):
        return {}
    try:
        with open(log_file, "rb") as f:
            # seek to end and read backwards for last latency line
            f.seek(0, 2)
            buf = b""
            while f.tell() > 0:
                f.seek(-1, 1)
                ch = f.read(1)
                if ch == b"\n" and buf:
                    line = buf.decode("utf-8", errors="ignore")
                    if "COLLECT_LATENCY" in line:
                        m = re.search(
                            r"median\s+([\d.]+).*p95\s+([\d.]+).*p99\s+([\d.]+).*tail\s+([\d.]+)", line)
                        if m:
                            return {
                                "median": float(m.group(1)),
                                "p95": float(m.group(2)),
                                "p99": float(m.group(3)),
                                "tail": float(m.group(4)),
                            }
                    buf = b""
                else:
                    buf = ch + buf
                    f.seek(-1, 1)
    except Exception:
        pass
    return {}


def read_scaler_metrics(work_dir: str, am_host: str | None = None) -> dict:
    """Read the latest continuous scaler metrics row."""
    path = str(Path(work_dir) / "scaler_metrics.csv")
    if am_host:
        path = "/tmp/scaler_metrics.csv"
    for line in reversed(_read_tail(am_host, path)):
        if line.startswith("timestamp"):
            continue
        parts = line.split(",")
        if len(parts) >= 10:
            try:
                return {
                    "avgCpu": float(parts[2]),
                    "avgInput": float(parts[3]),
                    "avgProcess": float(parts[4]),
                    "queue": float(parts[5]),
                    "numExecutors": int(parts[6]),
                    "numLambdaExecutors": int(parts[7]),
                }
            except (ValueError, IndexError):
                continue
    return {}


def read_source_queue_metrics(work_dir: str, am_host: str | None = None,
                              source_hosts: list[str] | None = None) -> dict:
    """Read latest source task Kafka queue-time metrics if available."""
    paths: list[tuple[str | None, str]] = [(None, str(Path(work_dir) / "source_task_metrics.csv"))]
    if am_host:
        paths.append((am_host, "/tmp/source_task_metrics.csv"))
    for host in source_hosts or []:
        if host and host != am_host:
            paths.append((host, "/tmp/source_task_metrics.csv"))
    values = []
    weighted_sum = 0.0
    total_samples = 0
    seen_intervals: set[tuple[str, str, str]] = set()
    for host, path in paths:
        for line in _read_tail(host, path, 200):
            if line.startswith("timestamp"):
                continue
            parts = line.split(",")
            if len(parts) >= 10:
                try:
                    interval_key = (parts[0], parts[1], parts[2])
                    if interval_key in seen_intervals:
                        continue
                    avg_ns = float(parts[5])
                    samples = int(float(parts[7]))
                    if avg_ns >= 0 and samples > 0:
                        seen_intervals.add(interval_key)
                        values.append(avg_ns)
                        weighted_sum += avg_ns * samples
                        total_samples += samples
                except (ValueError, IndexError):
                    continue
    if not values:
        return {}
    values.sort()
    def pct(p: float) -> float:
        idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * p))))
        return values[idx] / 1_000_000.0
    return {
        "p50": pct(0.50),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "unweightedMean": (sum(values) / len(values)) / 1_000_000.0,
        "weightedMean": (weighted_sum / total_samples) / 1_000_000.0 if total_samples > 0 else -1,
        "totalSamples": total_samples,
        "validTasks": len(values),
    }


# ── main loop ─────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 3:
        print(
            "Usage: metrics_collector.py <input_topic[,input_topic...]> "
            "<work_dir> [subscriber_log_file] [am_host] [result_topic] [source_hosts]"
        )
        sys.exit(1)

    try:
        topics = parse_input_topics(sys.argv[1])
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(2)
    work_dir = sys.argv[2]
    log_file = (
        sys.argv[3]
        if len(sys.argv) > 3
        else "/tmp/nx-auto-sub-{}.log".format(topics[0])
    )
    am_host = sys.argv[4] if len(sys.argv) > 4 else None
    result_topic = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] else None
    source_hosts = []
    if len(sys.argv) > 6 and sys.argv[6]:
        source_hosts = [host for host in re.split(r"[ ,]+", sys.argv[6]) if host]
    last_valid_source = -1
    consecutive_failures = 0

    os.makedirs(work_dir, exist_ok=True)

    combined_csv = Path(work_dir) / "combined_metrics.csv"
    topic_csv = Path(work_dir) / "kafka_topic_metrics.csv"
    metadata_path = Path(work_dir) / "kafka_input_topics.json"
    metadata = {
        "topics": topics,
        "consumerGroup": KAFKA_CONSUMER_GROUP,
        "expectedPartitionsPerTopic": KAFKA_PARTITIONS,
        "jobId": METRICS_JOB_ID,
        "kafkaCommandMode": KAFKA_COMMAND_MODE,
        "aggregateColumnsRequireAllTopics": True,
        "queueTimeScope": "aggregate across source tasks; topic identity unavailable",
    }
    try:
        if metadata_path.exists() and metadata_path.stat().st_size > 0:
            existing_metadata = json.loads(metadata_path.read_text())
            identity_keys = ("topics", "consumerGroup", "expectedPartitionsPerTopic", "jobId")
            if any(existing_metadata.get(key) != metadata.get(key) for key in identity_keys):
                raise RuntimeError(
                    f"existing metrics metadata belongs to another run: {metadata_path}"
                )
            if not METRICS_RESUME:
                raise RuntimeError(
                    f"metrics metadata already exists: {metadata_path}; "
                    "set METRICS_RESUME=true to resume"
                )
        prepare_csv(combined_csv, COMBINED_COLUMNS, METRICS_RESUME)
        prepare_csv(topic_csv, TOPIC_COLUMNS, METRICS_RESUME)
        if not metadata_path.exists():
            metadata_path.write_text(
                json.dumps(metadata, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    except (OSError, RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(3)

    print(
        f"[metrics] collector started for topics={','.join(topics)} "
        f"consumer_group={KAFKA_CONSUMER_GROUP} "
        f"result_topic={result_topic} work_dir={work_dir}"
    )

    while True:
        ts = int(time.time() * 1000)
        topic_offsets = {
            topic: get_topic_offset(topic, KAFKA_PARTITIONS)
            for topic in topics
        }
        offset = aggregate_complete(topic_offsets, topics)
        result_offset = get_topic_offset(result_topic) if result_topic else -1
        consumer_by_topic = get_consumer_group_lags(
            KAFKA_CONSUMER_GROUP,
            topics,
        )
        committed_offset = aggregate_complete(
            {
                topic: values.get("consumerCommittedOffset", -1)
                for topic, values in consumer_by_topic.items()
            },
            topics,
        )
        consumer_log_end = aggregate_complete(
            {
                topic: values.get("consumerLogEndOffset", -1)
                for topic, values in consumer_by_topic.items()
            },
            topics,
        )
        consumer_lag = aggregate_complete(
            {
                topic: values.get("consumerLag", -1)
                for topic, values in consumer_by_topic.items()
            },
            topics,
        )
        source, last_valid_source = read_source_count(work_dir, offset, am_host, last_valid_source)
        input_lag = max(0, offset - source) if offset >= 0 and source >= 0 else -1
        result_lag = max(0, offset - result_offset) if offset >= 0 and result_offset >= 0 else -1
        lat = read_latency(log_file)
        source_queue = read_source_queue_metrics(work_dir, am_host, source_hosts)
        cpu = read_scaler_metrics(work_dir, am_host)
        required_poll_failed = (
            offset < 0
            or consumer_lag < 0
            or source < 0
            or (bool(am_host) and not cpu)
        )
        if required_poll_failed:
            consecutive_failures += 1
        else:
            consecutive_failures = 0

        row = {
            "timestamp": ts,
            "inputOffset": offset,
            "resultOffset": result_offset,
            "sourceCount": source,
            "inputLag": input_lag,
            "resultLag": result_lag,
            "kafkaQueueTimeP50": source_queue.get("p50", -1),
            "kafkaQueueTimeP95": source_queue.get("p95", -1),
            "kafkaQueueTimeP99": source_queue.get("p99", -1),
            "latencyMedian": lat.get("median", -1),
            "latencyP95": lat.get("p95", -1),
            "latencyP99": lat.get("p99", -1),
            "latencyTail": lat.get("tail", -1),
            "avgCpu": cpu.get("avgCpu", -1),
            "avgInput": cpu.get("avgInput", -1),
            "avgProcess": cpu.get("avgProcess", -1),
            "queueSize": cpu.get("queue", -1),
            "numExecutors": cpu.get("numExecutors", -1),
            "numLambdaExecutors": cpu.get("numLambdaExecutors", -1),
            "consumerCommittedOffset": committed_offset,
            "consumerLogEndOffset": consumer_log_end,
            "consumerLag": consumer_lag,
            "kafkaQueueTimeUnweightedMeanMs": source_queue.get(
                "unweightedMean", -1
            ),
            "kafkaQueueTimeWeightedMeanMs": source_queue.get("weightedMean", -1),
            "kafkaQueueTimeTotalSamples": source_queue.get("totalSamples", -1),
            "kafkaQueueTimeValidTasks": source_queue.get("validTasks", -1),
        }

        append_csv_rows(combined_csv, COMBINED_COLUMNS, [row])
        topic_rows = []
        for topic in topics:
            topic_consumer = consumer_by_topic.get(topic, {})
            topic_rows.append(
                {
                    "timestamp": ts,
                    "topic": topic,
                    "endOffset": topic_offsets.get(topic, -1),
                    "committedOffset": topic_consumer.get(
                        "consumerCommittedOffset", -1
                    ),
                    "consumerLogEndOffset": topic_consumer.get(
                        "consumerLogEndOffset", -1
                    ),
                    "consumerLag": topic_consumer.get("consumerLag", -1),
                }
            )
        append_csv_rows(topic_csv, TOPIC_COLUMNS, topic_rows)

        print(
            f"[metrics] ts={ts} input={offset} result={result_offset} source={source} "
            f"inputLag={input_lag} resultLag={result_lag} "
            f"consumerLag={consumer_lag} topics={topic_offsets} "
            f"kafkaQueueMean={source_queue.get('unweightedMean', -1):.1f}ms "
            f"kafkaQueueP95={source_queue.get('p95', -1):.1f}ms "
            f"cpu={cpu.get('avgCpu', -1):.4f} queue={cpu.get('queue', -1):.0f}"
        )
        if consecutive_failures >= METRICS_MAX_CONSECUTIVE_FAILURES:
            print(
                "ERROR: required metric polls failed "
                f"{consecutive_failures} consecutive times",
                file=sys.stderr,
            )
            sys.exit(4)

        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
