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
    for col in ["topicOffset", "sourceCount", "kafkaLag"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if {"topicOffset", "sourceCount"}.issubset(df.columns):
        # AM-side fallback metrics can contain stale rows from older runs.
        invalid = df["sourceCount"] > df["topicOffset"]
        df.loc[invalid, "sourceCount"] = pd.NA
        df["sourceCount"] = df["sourceCount"].ffill().fillna(0)
        df["kafkaLag"] = df["topicOffset"] - df["sourceCount"]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    # relative time in seconds
    t0 = df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    # skip warmup
    return df[df["rel_s"] >= WARMUP_MS / 1000]


def load_task_metrics(work_dir: str) -> pd.DataFrame:
    path = Path(work_dir) / "task_metrics.csv"
    if not path.exists():
        return pd.DataFrame()
    cols = [
        "timestamp", "executorId", "taskId", "inputReceiveRate", "inputRate", "outputRate",
        "processingTime", "deserTime", "inbytes", "serializedTime", "outbytes"
    ]
    df = pd.read_csv(path, names=cols, header=None)
    df = df[pd.to_numeric(df["timestamp"], errors="coerce").notna()].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    for col in cols[3:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    t0 = df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    return df[df["rel_s"] >= WARMUP_MS / 1000]


def save_plot(work_dir: str, name: str):
    out = Path(work_dir) / "plots"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}.png"
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"[plot] saved {path}")


# ── plot generators ─────────────────────────────────────────────────────────
def plot_input_rate(df: pd.DataFrame, work_dir: str):
    if df.empty:
        return
    plt.figure(figsize=(10, 5))
    plt.plot(df["rel_s"], df["sourceCount"].diff().fillna(0) / df["rel_s"].diff().fillna(1), label="Source input rate")
    plt.plot(df["rel_s"], df["topicOffset"].diff().fillna(0) / df["rel_s"].diff().fillna(1), label="Kafka topic offset rate")
    plt.xlabel("Time (s)")
    plt.ylabel("Events / s")
    plt.title("Input Rate vs Kafka Offset Rate")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "input_rate")


def plot_kafka_lag(df: pd.DataFrame, work_dir: str):
    if df.empty or "kafkaLag" not in df.columns:
        return
    plt.figure(figsize=(10, 5))
    plt.plot(df["rel_s"], df["kafkaLag"], label="Kafka Lag", color="red")
    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    plt.title("Kafka Consumer Lag")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "kafka_lag")


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
    plt.plot(df["rel_s"], df["sourceCount"].diff().fillna(0) / df["rel_s"].diff().fillna(1), label="Source rate")

    # Overlay scaling events
    for _, row in dec.iterrows():
        color = "green" if row["action"] == "SCALE_OUT" else "orange"
        label = row["action"]
        plt.axvline(x=row["rel_s"], color=color, linestyle="--", alpha=0.7, label=label)

    plt.xlabel("Time (s)")
    plt.ylabel("Events / s")
    plt.title("Input Rate with Scaling Events")
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
    df = load_combined(work_dir)
    task_df = load_task_metrics(work_dir)

    if df.empty:
        print("[plot] No combined metrics found; nothing to plot.")
        sys.exit(1)

    plot_input_rate(df, work_dir)
    plot_kafka_lag(df, work_dir)
    plot_latency(df, work_dir)
    plot_cpu(df, work_dir)
    plot_scaling_events(df, work_dir)
    plot_task_rates(task_df, work_dir)

    print(f"[plot] All plots saved to {Path(work_dir) / 'plots'}")


if __name__ == "__main__":
    main()
