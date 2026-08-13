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
def load_production_t0(work_dir: str):
    path = Path(work_dir) / "producer_phases.csv"
    if not path.exists():
        return None
    try:
        phases = pd.read_csv(path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None
    required = {"timestamp", "event", "phase"}
    if phases.empty or not required.issubset(phases.columns):
        return None
    phase = pd.to_numeric(phases["phase"], errors="coerce")
    starts = phases[phases["event"].astype(str).eq("start") & phase.eq(1)]
    timestamps = pd.to_numeric(starts["timestamp"], errors="coerce").dropna()
    if timestamps.empty:
        return None
    return pd.to_datetime(timestamps.iloc[0], unit="ms")


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
        "kafkaQueueTimeUnweightedMeanMs", "kafkaQueueTimeWeightedMeanMs",
        "kafkaQueueTimeTotalSamples", "kafkaQueueTimeValidTasks",
        "kafkaQueueTimeValidIntervals", "kafkaQueueTimeLatestTimestamp",
        "avgCpu", "avgInput", "avgProcess", "queueSize", "numExecutors", "numLambdaExecutors",
        "registeredVmWorkers", "activeVmWorkers", "vmWorkerRunningTasks",
        "consumerCommittedOffset", "consumerLogEndOffset", "consumerLag"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if {"inputOffset", "sourceCount"}.issubset(df.columns):
        if (df["inputOffset"] > 0).any() or (df["sourceCount"] > 0).any():
            # AM-side fallback metrics can contain stale rows from older runs.
            invalid = df["sourceCount"] > df["inputOffset"]
            df.loc[invalid, "sourceCount"] = pd.NA
            df["sourceCount"] = df["sourceCount"].ffill().fillna(0)
            df["inputLag"] = (df["inputOffset"] - df["sourceCount"]).clip(lower=0)
    if {"inputOffset", "resultOffset"}.issubset(df.columns):
        # Count-only runs deliberately have no result Kafka topic and encode
        # that absence as resultOffset=-1. Do not turn the sentinel into a
        # synthetic inputOffset+1 result lag.
        valid_result = df["resultOffset"] >= 0
        if valid_result.any():
            df.loc[valid_result, "resultLag"] = (
                df.loc[valid_result, "inputOffset"]
                - df.loc[valid_result, "resultOffset"]
            ).clip(lower=0)
            df.loc[~valid_result, "resultLag"] = pd.NA
        else:
            df["resultLag"] = pd.NA
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    # relative time in seconds
    production_t0 = load_production_t0(work_dir)
    t0 = production_t0 if production_t0 is not None else df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    if production_t0 is not None:
        return df[df["rel_s"] >= 0]
    # Skip warmup for full runs, but keep short smoke runs plottable.
    warmed = df[df["rel_s"] >= WARMUP_MS / 1000]
    return warmed if not warmed.empty else df


def load_kafka_topic_metrics(work_dir: str) -> pd.DataFrame:
    path = Path(work_dir) / "kafka_topic_metrics.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    required = {
        "timestamp",
        "topic",
        "endOffset",
        "committedOffset",
        "consumerLogEndOffset",
        "consumerLag",
    }
    if df.empty or not required.issubset(df.columns):
        print(f"[plot] ignoring malformed {path}")
        return pd.DataFrame()
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "topic"]).copy()
    for col in required - {"timestamp", "topic"}:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if df.empty:
        return df
    production_t0 = load_production_t0(work_dir)
    t0 = (
        production_t0.value / 1_000_000
        if production_t0 is not None
        else df["timestamp"].min()
    )
    df["rel_s"] = (df["timestamp"] - t0) / 1000.0
    if production_t0 is not None:
        return df[df["rel_s"] >= 0]
    warmed = df[df["rel_s"] >= WARMUP_MS / 1000]
    return warmed if not warmed.empty else df


def load_task_metrics(work_dir: str, job_id: str = None) -> pd.DataFrame:
    root = Path(work_dir)
    paths = [root / "task_metrics.csv"]
    paths.extend(sorted((root / "remote_tmp_metrics").glob("node*/task_metrics.csv")))
    paths = [path for path in paths if path.exists()]
    if not paths:
        return pd.DataFrame()
    frames = []
    for path in paths:
        frame = pd.read_csv(path)
        if "timestamp" not in frame.columns:
            cols = [
                "timestamp", "executorId", "taskId", "inputReceiveRate", "inputRate", "outputRate",
                "processingTime", "deserTime", "inbytes", "serializedTime", "outbytes"
            ]
            frame = pd.read_csv(path, names=cols, header=None)
        frames.append(frame)
    df = pd.concat(frames, ignore_index=True)
    if "jobId" not in df.columns:
        df["jobId"] = "unknown"
    if job_id:
        before = len(df)
        df = df[df["jobId"].astype(str) == job_id].copy()
        if df.empty:
            print(f"[plot] no task_metrics rows for jobId={job_id}; skipping task rate plot ({before} stale rows ignored)")
            return pd.DataFrame()
    df = df[pd.to_numeric(df["timestamp"], errors="coerce").notna()].copy()
    df = df.drop_duplicates(
        subset=[
            col
            for col in ["timestamp", "jobId", "executorId", "taskId"]
            if col in df.columns
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    for col in ["inputReceiveRate", "inputRate", "outputRate", "processingTimeNs", "processingTime",
                "deserTimeNs", "deserTime", "inBytes", "inbytes", "serializedTimeNs",
                "serializedTime", "outBytes", "outbytes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    production_t0 = load_production_t0(work_dir)
    t0 = production_t0 if production_t0 is not None else df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    if production_t0 is not None:
        return df[df["rel_s"] >= 0]
    warmed = df[df["rel_s"] >= WARMUP_MS / 1000]
    return warmed if not warmed.empty else df


def load_source_task_metrics(
    work_dir: str,
    start_timestamp: pd.Timestamp = None,
    end_timestamp: pd.Timestamp = None,
) -> pd.DataFrame:
    root = Path(work_dir)
    paths = [root / "source_task_metrics.csv"]
    paths.extend(
        sorted((root / "remote_tmp_metrics").glob("node*/source_task_metrics.csv"))
    )
    paths = [path for path in paths if path.exists()]
    if not paths:
        return pd.DataFrame()
    df = pd.concat((pd.read_csv(path) for path in paths), ignore_index=True)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df = df[pd.to_numeric(df["timestamp"], errors="coerce").notna()].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    if start_timestamp is not None:
        df = df[df["timestamp"] >= start_timestamp]
    if end_timestamp is not None:
        df = df[df["timestamp"] <= end_timestamp]
    df = df.drop_duplicates(
        subset=[col for col in ["timestamp", "jobId", "taskId"] if col in df.columns]
    )
    for col in ["idleTimeNs", "kafkaQueueTimeNs", "kafkaQueueTimeAvgNs", "kafkaQueueTimeMaxNs",
                "kafkaQueueSamples", "inputRate", "recordsRead"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1)
    production_t0 = load_production_t0(work_dir)
    t0 = production_t0 if production_t0 is not None else df["timestamp"].min()
    df["rel_s"] = (df["timestamp"] - t0).dt.total_seconds()
    if production_t0 is not None:
        return df[df["rel_s"] >= 0]
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


def load_scaling_events(df: pd.DataFrame, work_dir: str) -> pd.DataFrame:
    """Load scaler decisions and align them with the combined-metrics time axis."""
    decisions = Path(work_dir) / "scaling_decisions.csv"
    if not decisions.exists() or df.empty:
        return pd.DataFrame(columns=["timestamp", "action", "rel_s"])
    try:
        dec = pd.read_csv(decisions, header=None, usecols=[0, 1])
    except (ValueError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=["timestamp", "action", "rel_s"])
    if dec.empty:
        return pd.DataFrame(columns=["timestamp", "action", "rel_s"])
    dec.columns = ["timestamp", "action"]
    dec["timestamp"] = pd.to_datetime(
        pd.to_numeric(dec["timestamp"], errors="coerce"),
        unit="ms",
        errors="coerce",
    )
    dec["action"] = dec["action"].astype(str)
    dec = dec.dropna(subset=["timestamp"])
    dec = dec[dec["action"].isin(["SCALE_OUT", "SCALE_IN"])]
    if dec.empty:
        return pd.DataFrame(columns=["timestamp", "action", "rel_s"])
    minimum_timestamp = df["timestamp"].min()
    if not isinstance(minimum_timestamp, pd.Timestamp):
        minimum_timestamp = pd.to_datetime(minimum_timestamp, unit="ms")
    true_t0 = minimum_timestamp - pd.Timedelta(seconds=float(df["rel_s"].min()))
    dec["rel_s"] = (dec["timestamp"] - true_t0).dt.total_seconds()
    return dec


def add_scaling_event_markers(df: pd.DataFrame, work_dir: str):
    """Add scale-out/scale-in markers to the current axes."""
    events = load_scaling_events(df, work_dir)
    seen = set()
    for _, row in events.iterrows():
        action = row["action"]
        color = "tab:red" if action == "SCALE_OUT" else "tab:orange"
        label = action.replace("_", " ").title() if action not in seen else None
        plt.axvline(
            x=row["rel_s"],
            color=color,
            linestyle="--",
            linewidth=1.4,
            alpha=0.85,
            label=label,
        )
        seen.add(action)


def add_workload_markers(df: pd.DataFrame, work_dir: str):
    """Add producer phase-start markers aligned to the plot's time axis."""
    path = Path(work_dir) / "producer_phases.csv"
    if not path.exists() or df.empty:
        return
    try:
        phases = pd.read_csv(path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return
    required = {"timestamp", "event", "targetRate"}
    if phases.empty or not required.issubset(phases.columns):
        return
    starts = phases[phases["event"].astype(str).eq("start")].copy()
    starts["timestamp"] = pd.to_datetime(
        pd.to_numeric(starts["timestamp"], errors="coerce"),
        unit="ms",
        errors="coerce",
    )
    starts["targetRate"] = pd.to_numeric(starts["targetRate"], errors="coerce")
    starts = starts.dropna(subset=["timestamp", "targetRate"])
    if starts.empty:
        return
    minimum_timestamp = df["timestamp"].min()
    if not isinstance(minimum_timestamp, pd.Timestamp):
        minimum_timestamp = pd.to_datetime(minimum_timestamp, unit="ms")
    true_t0 = minimum_timestamp - pd.Timedelta(seconds=float(df["rel_s"].min()))
    for _, row in starts.iloc[1:].iterrows():
        rel_s = (row["timestamp"] - true_t0).total_seconds()
        plt.axvline(
            rel_s,
            color="black",
            linestyle=":",
            linewidth=1.1,
            alpha=0.65,
            label=f"Workload → {int(row['targetRate']):,}/s",
        )

# ── plot generators ─────────────────────────────────────────────────────────
def plot_input_rate(df: pd.DataFrame, work_dir: str):
    if df.empty:
        return
    plt.figure(figsize=(10, 5))
    dt = df["rel_s"].diff().replace(0, float("nan"))
    if "sourceCount" in df.columns and (df["sourceCount"] > 0).any():
        plt.plot(df["rel_s"], df["sourceCount"].diff() / dt, label="Source input rate")
    elif "avgInput" in df.columns and (df["avgInput"] > 0).any():
        plt.plot(df["rel_s"], df["avgInput"], label="Source input rate (avg)")
    if "inputOffset" in df.columns and (df["inputOffset"] > 0).any():
        raw = df["inputOffset"].diff() / dt
        smoothed = raw.rolling(10, min_periods=1, center=True).mean()
        plt.plot(df["rel_s"], smoothed, label="Kafka input offset rate")
    if "resultOffset" in df.columns and (df["resultOffset"] > 0).any():
        raw = df["resultOffset"].diff() / dt
        smoothed = raw.rolling(5, min_periods=1, center=True).mean()
        plt.plot(df["rel_s"], smoothed, label="Kafka result offset rate")
    add_workload_markers(df, work_dir)
    add_scaling_event_markers(df, work_dir)
    plt.xlabel("Time (s)")
    plt.ylabel("Events / s")
    plt.title("Input Rate vs Kafka Offset Rate")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "input_rate")


def plot_kafka_lag(df: pd.DataFrame, work_dir: str):
    if df.empty:
        return
    plt.figure(figsize=(10, 5))
    plotted = False
    if "inputLag" in df.columns:
        valid = df["inputLag"] >= 0
        if valid.any():
            plt.plot(df.loc[valid, "rel_s"], df.loc[valid, "inputLag"], label="Input offset - source count", color="red")
            plotted = True
    if not plotted and "queueSize" in df.columns and (df["queueSize"] >= 0).any():
        valid = df["queueSize"] >= 0
        plt.plot(df.loc[valid, "rel_s"], df.loc[valid, "queueSize"], label="Queue size (proxy)", color="red")
        plt.ylim(bottom=0)
        plotted = True
    if not plotted:
        plt.close()
        return
    add_workload_markers(df, work_dir)
    add_scaling_event_markers(df, work_dir)
    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    title = "Kafka Source Lag" if plotted and "inputLag" in df.columns and (df["inputLag"] > 0).any() else "Lambda Queue Size (proxy for lag)"
    plt.title(title)
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
    add_workload_markers(df, work_dir)
    add_scaling_event_markers(df, work_dir)
    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    plt.title("Kafka Result Lag")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "kafka_result_lag")


def plot_kafka_topics(topic_df: pd.DataFrame, work_dir: str):
    if topic_df.empty:
        return

    plt.figure(figsize=(10, 5))
    plotted_rate = False
    for topic, group in topic_df.groupby("topic"):
        group = group.sort_values("timestamp")
        valid = group["endOffset"] >= 0
        group = group.loc[valid]
        if len(group) < 2:
            continue
        dt = group["timestamp"].diff() / 1000.0
        rate = group["endOffset"].diff() / dt.replace(0, float("nan"))
        plt.plot(
            group["rel_s"],
            rate.rolling(3, min_periods=1, center=True).mean(),
            label=f"{topic} rate",
        )
        plotted_rate = True
    complete_offsets = topic_df.pivot_table(
        index="timestamp", columns="topic", values="endOffset", aggfunc="last"
    ).dropna()
    complete_offsets = complete_offsets[
        (complete_offsets >= 0).all(axis=1)
    ]
    if len(complete_offsets) >= 2:
        aggregate = complete_offsets.sum(axis=1)
        dt = aggregate.index.to_series().diff() / 1000.0
        aggregate_rate = aggregate.diff() / dt.replace(0, float("nan"))
        rel_s = (aggregate.index - topic_df["timestamp"].min()) / 1000.0
        plt.plot(
            rel_s,
            aggregate_rate.rolling(3, min_periods=1, center=True).mean(),
            label="aggregate rate",
            color="black",
            linewidth=2,
        )
        plotted_rate = True
    if plotted_rate:
        add_workload_markers(topic_df, work_dir)
        plt.xlabel("Time (s)")
        plt.ylabel("Events / s")
        plt.title("Kafka Input Rate by Topic")
        plt.legend()
        plt.grid(True)
        save_plot(work_dir, "kafka_topic_rates")
    else:
        plt.close()

    plt.figure(figsize=(10, 5))
    plotted_lag = False
    for topic, group in topic_df.groupby("topic"):
        valid = group["consumerLag"] >= 0
        if valid.any():
            plt.plot(
                group.loc[valid, "rel_s"],
                group.loc[valid, "consumerLag"],
                label=f"{topic} lag",
            )
            plotted_lag = True
    complete_lag = topic_df.pivot_table(
        index="timestamp", columns="topic", values="consumerLag", aggfunc="last"
    ).dropna()
    complete_lag = complete_lag[(complete_lag >= 0).all(axis=1)]
    if not complete_lag.empty:
        aggregate_lag = complete_lag.sum(axis=1)
        rel_s = (aggregate_lag.index - topic_df["timestamp"].min()) / 1000.0
        plt.plot(
            rel_s,
            aggregate_lag,
            label="aggregate lag",
            color="black",
            linewidth=2,
        )
        plotted_lag = True
    if plotted_lag:
        add_workload_markers(topic_df, work_dir)
        plt.xlabel("Time (s)")
        plt.ylabel("Events")
        plt.title("Kafka Consumer Lag by Topic")
        plt.legend()
        plt.grid(True)
        save_plot(work_dir, "kafka_topic_lag")
    else:
        plt.close()


def plot_source_queue_time(df: pd.DataFrame, source_df: pd.DataFrame, work_dir: str):
    if not df.empty and "kafkaQueueTimeUnweightedMeanMs" in df.columns:
        valid = df["kafkaQueueTimeUnweightedMeanMs"] >= 0
        if valid.any():
            plt.figure(figsize=(10, 5))
            plt.plot(
                df.loc[valid, "rel_s"],
                df.loc[valid, "kafkaQueueTimeUnweightedMeanMs"],
                label="Current-window unweighted task mean",
                linewidth=2.0,
            )
            if "kafkaQueueTimeWeightedMeanMs" in df.columns:
                weighted = df["kafkaQueueTimeWeightedMeanMs"] >= 0
                if weighted.any():
                    plt.plot(
                        df.loc[weighted, "rel_s"],
                        df.loc[weighted, "kafkaQueueTimeWeightedMeanMs"],
                        label="Current-window sample-weighted mean",
                        linestyle="--",
                        alpha=0.8,
                    )
            for col, label, linestyle in [
                ("kafkaQueueTimeP50", "P50", ":"),
                ("kafkaQueueTimeP95", "P95", "-."),
                ("kafkaQueueTimeP99", "P99", "--"),
            ]:
                if col in df.columns:
                    pct_valid = df[col] >= 0
                    if pct_valid.any():
                        plt.plot(df.loc[pct_valid, "rel_s"], df.loc[pct_valid, col], label=label, linestyle=linestyle, alpha=0.55)
            add_workload_markers(df, work_dir)
            add_scaling_event_markers(df, work_dir)
            plt.xlabel("Time (s)")
            plt.ylabel("Kafka queue residence time (ms)")
            plt.title("Source Kafka Queue Residence Time")
            plt.legend()
            plt.grid(True)
            save_plot(work_dir, "source_kafka_queue_time")

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
    plt.title("Per-Task Source Kafka Queue Time")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "source_kafka_queue_time_by_task")


def plot_latency(df: pd.DataFrame, work_dir: str):
    if df.empty:
        return
    plt.figure(figsize=(10, 5))
    col = "latencyMedian"
    if col not in df.columns:
        plt.close()
        return
    valid = df[col] >= 0
    if not valid.any():
        plt.close()
        return
    plt.plot(df.loc[valid, "rel_s"], df.loc[valid, col], label="Latency (median)")
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
    add_workload_markers(df, work_dir)
    add_scaling_event_markers(df, work_dir)
    plt.xlabel("Time (s)")
    plt.ylabel("CPU")
    plt.title("Average CPU Usage")
    plt.legend()
    plt.grid(True)
    save_plot(work_dir, "cpu")


def plot_scaling_events(df: pd.DataFrame, work_dir: str):
    """Overlay vertical lines for scale-out / scale-in events."""
    if df.empty:
        return

    dec = load_scaling_events(df, work_dir)
    if dec.empty:
        return

    plt.figure(figsize=(10, 5))
    # Choose best available background metric
    if "resultLag" in df.columns and (df["resultLag"] >= 0).any():
        y_col, y_label = "resultLag", "Result lag"
    elif "inputLag" in df.columns and (df["inputLag"] >= 0).any():
        y_col, y_label = "inputLag", "Input lag"
    elif "queueSize" in df.columns and (df["queueSize"] >= 0).any():
        y_col, y_label = "queueSize", "Queue size"
    elif "avgInput" in df.columns and (df["avgInput"] >= 0).any():
        y_col, y_label = "avgInput", "Avg input rate"
    else:
        plt.close()
        return
    valid = df[y_col] >= 0
    plt.plot(df.loc[valid, "rel_s"], df.loc[valid, y_col], label=y_label)
    if y_col in ("queueSize", "avgInput"):
        plt.ylim(bottom=0)

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
    task_df = task_df.copy()
    task_df["time_bin"] = (task_df["rel_s"] // 5) * 5
    rates = (
        task_df.groupby(["executorId", "time_bin"], as_index=False)[
            ["inputRate", "outputRate"]
        ]
        .sum()
        .rename(columns={"time_bin": "rel_s"})
    )
    # Sum the rates of tasks sharing an executor within each five-second bin.
    plt.figure(figsize=(10, 5))
    for executor, group in rates.groupby("executorId"):
        plt.plot(group["rel_s"], group["inputRate"], label=f"{executor} input")
        plt.plot(group["rel_s"], group["outputRate"], label=f"{executor} output", linestyle="--")
    add_workload_markers(task_df, work_dir)
    add_scaling_event_markers(task_df, work_dir)
    plt.xlabel("Time (s)")
    plt.ylabel("Records / s")
    plt.title("Aggregate Task Input/Output Rates by Executor")
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
    topic_df = load_kafka_topic_metrics(work_dir)
    task_df = load_task_metrics(work_dir, job_id)
    source_task_df = load_source_task_metrics(
        work_dir,
        start_timestamp=df["timestamp"].min(),
        end_timestamp=df["timestamp"].max(),
    )

    if df.empty:
        print("[plot] No combined metrics found; nothing to plot.")
        sys.exit(1)

    plot_input_rate(df, work_dir)
    plot_kafka_lag(df, work_dir)
    plot_kafka_result_lag(df, work_dir)
    plot_kafka_topics(topic_df, work_dir)
    plot_source_queue_time(df, source_task_df, work_dir)
    plot_latency(df, work_dir)
    plot_cpu(df, work_dir)
    plot_scaling_events(df, work_dir)
    plot_task_rates(task_df, work_dir)

    print(f"[plot] All plots saved to {Path(work_dir) / 'plots'}")


if __name__ == "__main__":
    main()
