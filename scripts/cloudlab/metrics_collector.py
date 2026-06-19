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


def _ssh_read_last_line(host: str, filepath: str) -> str:
    """Return the last line of a remote file via SSH, or empty string."""
    try:
        out = subprocess.check_output(
            ["ssh", host, "tail", "-1", shlex.quote(filepath)],
            stderr=subprocess.DEVNULL, text=True, timeout=5
        )
        return out.strip()
    except Exception:
        return ""


def _read_last_line_local(filepath: str) -> str:
    """Return the last line of a local file, or empty string."""
    try:
        with open(filepath) as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else ""
    except Exception:
        return ""


def _read_last_line(host: str | None, filepath: str) -> str:
    if host:
        return _ssh_read_last_line(host, filepath)
    return _read_last_line_local(filepath)


def read_source_count(work_dir: str, am_host: str | None = None) -> int:
    """Read the latest source count from source_metrics.csv."""
    path = str(Path(work_dir) / "source_metrics.csv")
    if am_host:
        path = "/tmp/source_metrics.csv"
    line = _read_last_line(am_host, path)
    if line:
        parts = line.split(",")
        if len(parts) >= 2:
            try:
                return int(parts[1])
            except ValueError:
                pass
    return -1


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


def read_cpu_metrics(work_dir: str, am_host: str | None = None) -> dict:
    """Read the latest avgCpu from scaling_decisions.csv (or return empty)."""
    path = str(Path(work_dir) / "scaling_decisions.csv")
    if am_host:
        path = "/tmp/scaling_decisions.csv"
    line = _read_last_line(am_host, path)
    if line:
        parts = line.split(",")
        if len(parts) >= 9:
            try:
                return {
                    "avgCpu": float(parts[2]),
                    "avgInput": float(parts[3]),
                    "avgProcess": float(parts[4]),
                    "queue": float(parts[5]),
                    "numExecutors": int(parts[8]),
                }
            except (ValueError, IndexError):
                pass
    return {}


# ── main loop ─────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 3:
        print("Usage: metrics_collector.py <topic> <work_dir> [subscriber_log_file] [am_host]")
        sys.exit(1)

    topic = sys.argv[1]
    work_dir = sys.argv[2]
    log_file = sys.argv[3] if len(sys.argv) > 3 else "/tmp/nx-auto-sub-{}.log".format(topic)
    am_host = sys.argv[4] if len(sys.argv) > 4 else None

    os.makedirs(work_dir, exist_ok=True)

    combined_csv = Path(work_dir) / "combined_metrics.csv"
    with open(combined_csv, "w") as f:
        f.write(
            "timestamp,topicOffset,sourceCount,kafkaLag,latencyMedian,latencyP95,"
            "latencyP99,latencyTail,avgCpu,avgInput,avgProcess,queueSize,numExecutors\n"
        )

    print(f"[metrics] collector started for topic={topic} work_dir={work_dir}")

    while True:
        ts = int(time.time() * 1000)
        offset = get_topic_offset(topic)
        source = read_source_count(work_dir, am_host)
        lag = offset - source if offset >= 0 and source >= 0 else -1
        lat = read_latency(log_file)
        cpu = read_cpu_metrics(work_dir, am_host)

        row = [
            ts,
            offset,
            source,
            lag,
            lat.get("median", -1),
            lat.get("p95", -1),
            lat.get("p99", -1),
            lat.get("tail", -1),
            cpu.get("avgCpu", -1),
            cpu.get("avgInput", -1),
            cpu.get("avgProcess", -1),
            cpu.get("queue", -1),
            cpu.get("numExecutors", -1),
        ]

        with open(combined_csv, "a") as f:
            f.write(",".join(str(v) for v in row) + "\n")

        print(
            f"[metrics] ts={ts} offset={offset} source={source} lag={lag} "
            f"lat median={lat.get('median', -1):.1f} p95={lat.get('p95', -1):.1f} "
            f"cpu={cpu.get('avgCpu', -1):.4f} queue={cpu.get('queue', -1):.0f}"
        )

        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
