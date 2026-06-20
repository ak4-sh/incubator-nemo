#!/usr/bin/env python3
"""
Metrics collector for Nemo autoscaler tests.
Polls Kafka offsets, reads source/task metrics, and produces combined_metrics.csv.
"""

import os
import sys
import time
import subprocess
import re
import shlex
from pathlib import Path

# ── configuration ─────────────────────────────────────────────────────────
INTERVAL_S = 5
OFFSET_CMD = [
    "ssh", "node1", "/opt/kafka/bin/kafka-run-class.sh", "kafka.tools.GetOffsetShell",
    "--broker-list", "node1:9092,node2:9092,node3:9092",
    "--topic", "{topic}", "--time", "-1"
]

# ── helpers ───────────────────────────────────────────────────────────────
def get_topic_offset(topic: str) -> int:
    """Return the latest offset for a topic, or -1 on error."""
    cmd = [c.replace("{topic}", topic) for c in OFFSET_CMD]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, timeout=10)
        # output format: topic:partition:offset
        total = 0
        count = 0
        for line in out.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) == 3:
                total += int(parts[2])
                count += 1
        return total if count > 0 else -1
    except Exception as e:
        print(f"[metrics] offset check failed: {e}", file=sys.stderr)
    return -1


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
    for host, path in paths:
        for line in _read_tail(host, path, 200):
            if line.startswith("timestamp"):
                continue
            parts = line.split(",")
            if len(parts) >= 10:
                try:
                    avg_ns = float(parts[5])
                    samples = int(float(parts[7]))
                    if avg_ns >= 0 and samples > 0:
                        values.append(avg_ns / 1_000_000.0)
                except (ValueError, IndexError):
                    continue
    if not values:
        return {}
    values.sort()
    def pct(p: float) -> float:
        idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * p))))
        return values[idx]
    return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}


# ── main loop ─────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 3:
        print("Usage: metrics_collector.py <input_topic> <work_dir> [subscriber_log_file] [am_host] [result_topic] [source_hosts]")
        sys.exit(1)

    topic = sys.argv[1]
    work_dir = sys.argv[2]
    log_file = sys.argv[3] if len(sys.argv) > 3 else "/tmp/nx-auto-sub-{}.log".format(topic)
    am_host = sys.argv[4] if len(sys.argv) > 4 else None
    result_topic = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] else None
    source_hosts = []
    if len(sys.argv) > 6 and sys.argv[6]:
        source_hosts = [host for host in re.split(r"[ ,]+", sys.argv[6]) if host]
    last_valid_source = -1

    os.makedirs(work_dir, exist_ok=True)

    combined_csv = Path(work_dir) / "combined_metrics.csv"
    with open(combined_csv, "w") as f:
        f.write(
            "timestamp,inputOffset,resultOffset,sourceCount,inputLag,resultLag,"
            "kafkaQueueTimeP50,kafkaQueueTimeP95,kafkaQueueTimeP99,latencyMedian,latencyP95,"
            "latencyP99,latencyTail,avgCpu,avgInput,avgProcess,queueSize,numExecutors,numLambdaExecutors\n"
        )

    print(f"[metrics] collector started for topic={topic} result_topic={result_topic} work_dir={work_dir}")

    while True:
        ts = int(time.time() * 1000)
        offset = get_topic_offset(topic)
        result_offset = get_topic_offset(result_topic) if result_topic else -1
        source, last_valid_source = read_source_count(work_dir, offset, am_host, last_valid_source)
        input_lag = max(0, offset - source) if offset >= 0 and source >= 0 else -1
        result_lag = max(0, offset - result_offset) if offset >= 0 and result_offset >= 0 else -1
        lat = read_latency(log_file)
        source_queue = read_source_queue_metrics(work_dir, am_host, source_hosts)
        cpu = read_scaler_metrics(work_dir, am_host)

        row = [
            ts,
            offset,
            result_offset,
            source,
            input_lag,
            result_lag,
            source_queue.get("p50", -1),
            source_queue.get("p95", -1),
            source_queue.get("p99", -1),
            lat.get("median", -1),
            lat.get("p95", -1),
            lat.get("p99", -1),
            lat.get("tail", -1),
            cpu.get("avgCpu", -1),
            cpu.get("avgInput", -1),
            cpu.get("avgProcess", -1),
            cpu.get("queue", -1),
            cpu.get("numExecutors", -1),
            cpu.get("numLambdaExecutors", -1),
        ]

        with open(combined_csv, "a") as f:
            f.write(",".join(str(v) for v in row) + "\n")

        print(
            f"[metrics] ts={ts} input={offset} result={result_offset} source={source} "
            f"inputLag={input_lag} resultLag={result_lag} "
            f"kafkaQueueP95={source_queue.get('p95', -1):.1f}ms "
            f"cpu={cpu.get('avgCpu', -1):.4f} queue={cpu.get('queue', -1):.0f}"
        )

        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
