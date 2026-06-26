#!/usr/bin/env python3
"""
Post-run plot script for Nemo autoscaler tests.
Reads combined_metrics.csv and task_metrics.csv and generates comparison plots.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ── configuration ─────────────────────────────────────────────────────────
WARMUP_MS = 30_000  # skip first 30s

# ── helpers ───────────────────────────────────────────────────────────────────
def load_combined(work_dir: str) -> pd.DataFrame:
    path = Path(work_dir) / "combined_metrics.csv"
    if not path.exists():
        print(f"[plot] missing {path}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Backward compatibility for older artifacts.
    if "topicOffset" in df.columns and "inputOffset" not in df.columns:
        df = df.rename(columns={"topicOffset": "inputOffset", "kafkaLag": "inputLag"})
        if "resultOffset" not in df.columns:
            df["resultOffset"] = pd.NA
        if "resultLag" not in df.columns:
            df["resultLag"] = pd.NA
    for col in [
        "inputOffset", "resultOffset", "sourceCount", "inputLag", "resultLag",
        "kafkaQueueTimeP50", "kafkaQueueTimeP95", "kafkaQueueTimeP99",
        "avgCpu", "avgInput", "avgProcess", "queueSize", "numExecutors", "numLambdaExecutors"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if {"inputOffset", "sourceCount"}.issubset(df.columns):
        # AM-side fallback metrics can contain stale rows from older runs.
        invalid = df["sourceCount"] > df["inputOffset"]
        df.loc[invalid, "sourceCount"] = pd.NA
        df["sourceCount"] = df["sourceCount"].ffill().fillna(0)
        df["inputLag"] = (df["inputOffset"] - df["sourceCount"]).clip(lower=0)
    if {"inputOffset", "resultOffset"}.issubset(df.columns):
        df["resultLag"] = (df["inputOffset"] - df["resultOffset"]).clip(lower=0)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    # relative time in seconds
    t0 = df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    # Skip warmup for full runs, but keep short smoke runs plottable.
    warmed = df[df["rel_s"] >= WARMUP_MS / 1000]
    return warmed if not warmed.empty else df


def load_task_metrics(work_dir: str, job_id: str = None) -> pd.DataFrame:
    path = Path(work_dir) / "task_metrics.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        cols = [
            "timestamp", "executorId", "taskId", "inputReceiveRate", "inputRate", "outputRate",
            "processingTime", "deserTime", "inbytes", "serializedTime", "outbytes"
        ]
        df = pd.read_csv(path, names=cols, header=None)
    if "jobId" not in df.columns:
        df["jobId"] = "unknown"
    if job_id:
        before = len(df)
        df = df[df["jobId"].astype(str) == job_id].copy()
        if df.empty:
            print(f"[plot] no task_metrics rows for jobId={job_id}; skipping task rate plot ({before} stale rows ignored)")
            return pd.DataFrame()
    df = df[pd.to_numeric(df["timestamp"], errors="coerce").notna()].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    for col in ["inputReceiveRate", "inputRate", "outputRate", "processingTimeNs", "processingTime",
                "deserTimeNs", "deserTime", "inBytes", "inbytes", "serializedTimeNs",
                "serializedTime", "outBytes", "outbytes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    t0 = df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    warmed = df[df["rel_s"] >= WARMUP_MS / 1000]
    return warmed if not warmed.empty else df


def load_source_task_metrics(work_dir: str) -> pd.DataFrame:
    path = Path(work_dir) / "source_task_metrics.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df = df[pd.to_numeric(df["timestamp"], errors="coerce").notna()].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    for col in ["idleTimeNs", "kafkaQueueTimeNs", "kafkaQueueTimeAvgNs", "kafkaQueueTimeMaxNs",
                "kafkaQueueSamples", "inputRate", "recordsRead"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1)
    t0 = df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    warmed = df[df["rel_s"] >= WARMUP_MS / 1000]
    return warmed if not warmed.empty else df


def save_plot(work_dir: str, name: str):
    out = Path(work_dir) / "plots"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}.png"
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"[plot] saved {path}")



def infer_job_id(work_dir: str):
    """Infer the current run's jobId from per-run metrics files."""
    for name in ["scaler_metrics.csv", "source_aggregate_metrics.csv", "task_metrics.csv"]:
        path = Path(work_dir) / name
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, usecols=lambda c: c == "jobId")
        except Exception:
            continue
        if "jobId" not in df.columns:
            continue
        vals = df["jobId"].dropna().astype(str)
        vals = vals[(vals != "") & (vals != "unknown")]
        if not vals.empty:
            return vals.mode().iloc[0]
    return None

# ── plot generators ─────────────────────────────────────────────────────────
def plot_input_rate(df: pd.DataFrame, work_dir: str):
    if df.empty:
        return
    plt.figure(figsize=(10, 5))
    plt.plot(df["rel_s"], df["sourceCount"].diff().fillna(0) / df["rel_s"].diff().fillna(1), label="Source input rate")
    plt.plot(df["rel_s"], df["inputOffset"].diff().fillna(0) / df["rel_s"].diff().fillna(1), label="Kafka input offset rate")
    if "resultOffset" in df.columns and df["resultOffset"].notna().any():
        plt.plot(df["rel_s"], df["resultOffset"].diff().fillna(0) / df["rel_s"].diff().fillna(1), label="Kafka result offset rate")
    plt.xlabel("Time (s)")
    plt.ylabel("Events / s")
    plt.title("Input Rate vs Kafka Offset Rate")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "input_rate")


def plot_kafka_lag(df: pd.DataFrame, work_dir: str):
    if df.empty or "inputLag" not in df.columns:
        return
    plt.figure(figsize=(10, 5))
    valid = df["inputLag"] >= 0
    plt.plot(df.loc[valid, "rel_s"], df.loc[valid, "inputLag"], label="Input offset - source count", color="red")
    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    plt.title("Kafka Source Lag")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "kafka_source_lag")


def plot_kafka_result_lag(df: pd.DataFrame, work_dir: str):
    if df.empty or "resultLag" not in df.columns:
        return
    valid = df["resultLag"] >= 0
    if not valid.any():
        return
    plt.figure(figsize=(10, 5))
    plt.plot(df.loc[valid, "rel_s"], df.loc[valid, "resultLag"], label="Input offset - result offset", color="purple")
    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    plt.title("Kafka Result Lag")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "kafka_result_lag")


def plot_source_queue_time(source_df: pd.DataFrame, work_dir: str):
    if source_df.empty or "kafkaQueueTimeAvgNs" not in source_df.columns:
        return
    valid = source_df["kafkaQueueTimeAvgNs"] >= 0
    if not valid.any():
        return
    plt.figure(figsize=(10, 5))
    for task, group in source_df.loc[valid].groupby("taskId"):
        plt.plot(group["rel_s"], group["kafkaQueueTimeAvgNs"] / 1_000_000.0, label=f"{task} avg")
    plt.xlabel("Time (s)")
    plt.ylabel("Kafka queue time (ms)")
    plt.title("Source Kafka Queue Time")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "source_kafka_queue_time")


def plot_latency(df: pd.DataFrame, work_dir: str):
    if df.empty:
        return
    plt.figure(figsize=(10, 5))
    for col, label in [("latencyMedian", "p50"), ("latencyP95", "p95"), ("latencyP99", "p99")]:
        if col in df.columns:
            valid = df[col] >= 0
            plt.plot(df.loc[valid, "rel_s"], df.loc[valid, col], label=label)
    plt.xlabel("Time (s)")
    plt.ylabel("Latency (ms)")
    plt.title("End-to-End Latency")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "latency")


def plot_cpu(df: pd.DataFrame, work_dir: str):
    if df.empty or "avgCpu" not in df.columns:
        return
    valid = df["avgCpu"] >= 0
    plt.figure(figsize=(10, 5))
    plt.plot(df.loc[valid, "rel_s"], df.loc[valid, "avgCpu"], label="Avg CPU")
    plt.xlabel("Time (s)")
    plt.ylabel("CPU")
    plt.title("Average CPU Usage")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "cpu")


def plot_scaling_events(df: pd.DataFrame, work_dir: str):
    """Overlay vertical lines for scale-out / scale-in events."""
    decisions = Path(work_dir) / "scaling_decisions.csv"
    if not decisions.exists() or df.empty:
        return

    dec = pd.read_csv(decisions, header=None)
    dec.columns = ["timestamp", "action", "avgCpu", "avgInput", "avgProcess", "queue", "queue2", "ratio", "numExecutors"]
    dec["timestamp"] = pd.to_datetime(dec["timestamp"], unit="ms")
    t0 = df["timestamp"].min()
    dec["rel_s"] = (dec["timestamp"] - t0).dt.total_seconds()
    dec = dec[dec["rel_s"] >= WARMUP_MS / 1000]

    plt.figure(figsize=(10, 5))
    # Plot source rate
    y_col = "resultLag" if "resultLag" in df.columns and (df["resultLag"] >= 0).any() else "inputLag"
    plt.plot(df["rel_s"], df[y_col], label=y_col)

    # Overlay scaling events
    for _, row in dec.iterrows():
        color = "green" if row["action"] == "SCALE_OUT" else "orange"
        label = row["action"]
        plt.axvline(x=row["rel_s"], color=color, linestyle="--", alpha=0.7, label=label)

    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    plt.title("Kafka Lag with Scaling Events")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "scaling_events")


def plot_task_rates(task_df: pd.DataFrame, work_dir: str):
    if task_df.empty:
        return
    # Group by executor and plot input/output rates
    plt.figure(figsize=(10, 5))
    for executor, group in task_df.groupby("executorId"):
        plt.plot(group["rel_s"], group["inputRate"], label=f"{executor} input")
        plt.plot(group["rel_s"], group["outputRate"], label=f"{executor} output", linestyle="--")
    plt.xlabel("Time (s)")
    plt.ylabel("Records / s")
    plt.title("Per-Task Input/Output Rates")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "task_rates")


# ── main ────────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Usage: plot_metrics.py <work_dir>")
        sys.exit(1)

    work_dir = sys.argv[1]
    job_id = infer_job_id(work_dir)
    if job_id:
        print(f"[plot] inferred jobId={job_id}")
    else:
        print("[plot] could not infer jobId; task metrics will not be filtered")
    df = load_combined(work_dir)
    task_df = load_task_metrics(work_dir, job_id)
    source_task_df = load_source_task_metrics(work_dir)

    if df.empty:
        print("[plot] No combined metrics found; nothing to plot.")
        sys.exit(1)

    plot_input_rate(df, work_dir)
    plot_kafka_lag(df, work_dir)
    plot_kafka_result_lag(df, work_dir)
    plot_source_queue_time(source_task_df, work_dir)
    plot_latency(df, work_dir)
    plot_cpu(df, work_dir)
    plot_scaling_events(df, work_dir)
    plot_task_rates(task_df, work_dir)

    print(f"[plot] All plots saved to {Path(work_dir) / 'plots'}")


if __name__ == "__main__":
    main()
