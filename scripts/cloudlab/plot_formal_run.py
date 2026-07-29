#!/usr/bin/env python3
"""Generate plots for archived CloudLab formal Q6 runs.

The formal archives keep authoritative run metrics under a split layout:
``workdir/`` for collector/producer files, ``formal_metrics/`` for AM-side
runtime CSVs with host suffixes, and ``node_metrics/`` for per-node system
metrics. This script resolves that layout directly and intentionally does not
use ``consumer_group_lag_timeseries.csv`` because older wrappers wrote the
wrong consumer-group column there.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd


DECISION_COLUMNS = [
    "timestamp",
    "action",
    "avgCpu",
    "avgInput",
    "avgProcess",
    "queue",
    "queue2",
    "ratio",
    "numExecutors",
    "trigger",
    "cpuThreshold",
    "cpuRatio",
    "queueScale",
    "queueRatio",
    "queueDelay",
    "queueDelayThreshold",
    "baselineExecutors",
    "numLambdaExecutors",
]

EXPECTED_PLOTS = [
    "input_processing_rates.png",
    "queue_size.png",
    "source_kafka_queue_time.png",
    "latency.png",
    "scaling_decisions.png",
    "cpu_executors.png",
    "kafka_source_lag.png",
    "kafka_consumer_lag.png",
    "kafka_offsets.png",
    "task_rates_by_executor.png",
    "node_cpu_utilization.png",
    "node_nemo_rss.png",
    "node_network_throughput.png",
]


class FormalRun:
    def __init__(self, root: Path):
        self.root = root
        self.workdir = root / "workdir"
        self.formal_metrics = root / "formal_metrics"
        self.node_metrics = root / "node_metrics"
        self.plots = root / "plots"
        self.generated_plots: set[str] = set()


def fail(message: str) -> None:
    print(f"[plot-formal] ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def first_match(pattern: str, directory: Path, required: bool = True) -> Path | None:
    matches = sorted(directory.glob(pattern))
    if not matches:
        if required:
            fail(f"missing {directory / pattern}")
        return None
    if len(matches) > 1:
        print(f"[plot-formal] using {matches[0]} ({len(matches)} matches for {pattern})")
    return matches[0]


def read_csv(path: Path, required: bool = True, **kwargs) -> pd.DataFrame:
    if not path.exists():
        if required:
            fail(f"missing {path}")
        return pd.DataFrame()
    return pd.read_csv(path, **kwargs)


def read_decisions(path: Path) -> pd.DataFrame:
    first_line = path.read_text().splitlines()[0] if path.exists() else ""
    if first_line.startswith("timestamp,"):
        return pd.read_csv(path)
    return pd.read_csv(path, header=None, names=DECISION_COLUMNS)


def numeric_columns(df: pd.DataFrame, exclude: Iterable[str] = ()) -> pd.DataFrame:
    excluded = set(exclude)
    for col in df.columns:
        if col not in excluded:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def add_relative_seconds(df: pd.DataFrame, t0_ms: float, timestamp_col: str = "timestamp") -> pd.DataFrame:
    if not df.empty and timestamp_col in df.columns:
        df[timestamp_col] = pd.to_numeric(df[timestamp_col], errors="coerce")
        df.sort_values(timestamp_col, inplace=True)
        df["rel_s"] = (df[timestamp_col] - t0_ms) / 1000.0
    return df


def save_plot(run: FormalRun, name: str) -> None:
    run.plots.mkdir(parents=True, exist_ok=True)
    path = run.plots / f"{name}.png"
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    run.generated_plots.add(path.name)
    print(f"[plot-formal] saved {path}")


def clear_plots(run: FormalRun) -> None:
    run.plots.mkdir(parents=True, exist_ok=True)
    for path in run.plots.glob("*.png"):
        path.unlink()
    run.generated_plots.clear()


def unique_legend(fontsize: int = 8, ncol: int = 1) -> None:
    handles, labels = plt.gca().get_legend_handles_labels()
    seen = set()
    uniq_h = []
    uniq_l = []
    for handle, label in zip(handles, labels):
        if label and label not in seen:
            uniq_h.append(handle)
            uniq_l.append(label)
            seen.add(label)
    if uniq_h:
        plt.legend(uniq_h, uniq_l, loc="best", fontsize=fontsize, ncol=ncol)


def safe_int(value) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "?"
    return str(int(numeric))


def decorate(run: FormalRun, title: str, phases: pd.DataFrame, scaler_enable: pd.DataFrame, decisions: pd.DataFrame) -> None:
    plt.title(f"{run.root.name} - {title}")
    plt.grid(True, alpha=0.3)

    if not phases.empty:
        starts = phases[phases["event"].astype(str).eq("start")] if "event" in phases.columns else pd.DataFrame()
        for _, row in starts.iterrows():
            rate_col = "targetRate" if "targetRate" in row.index else "rate"
            rate = row[rate_col] if rate_col in row.index else float("nan")
            label = f"phase {safe_int(row.get('phase'))}: {safe_int(rate)}/s"
            plt.axvline(row["rel_s"], color="gray", linestyle=":", alpha=0.35)
            _, ymax = plt.ylim()
            plt.text(row["rel_s"], ymax, label, rotation=90, va="top", ha="right", fontsize=8, color="gray")

    if not scaler_enable.empty:
        for _, row in scaler_enable.iterrows():
            plt.axvline(row["rel_s"], color="blue", linestyle="-.", alpha=0.7, label="scaler enable")

    if not decisions.empty:
        for _, row in decisions.iterrows():
            color = "green" if row["action"] == "SCALE_OUT" else "orange"
            plt.axvline(row["rel_s"], color=color, linestyle="--", alpha=0.75, label=row["action"])

    unique_legend()


def infer_run_id(run: FormalRun, scaler: pd.DataFrame) -> str:
    run_id_path = run.workdir / "run_id.txt"
    if run_id_path.exists():
        run_id = run_id_path.read_text().strip()
    else:
        run_id = run.root.name

    if "jobId" in scaler.columns:
        job_ids = scaler["jobId"].dropna().astype(str)
        job_ids = job_ids[(job_ids != "") & (job_ids != "unknown") & (job_ids != "nan")]
        if not job_ids.empty:
            scaler_job_id = job_ids.mode().iloc[0]
            if scaler_job_id != run_id:
                print(
                    f"[plot-formal] WARNING: inferred run_id={run_id} differs from scaler jobId={scaler_job_id}",
                    file=sys.stderr,
                )
    return run_id


def choose_t0_ms(combined: pd.DataFrame, phases: pd.DataFrame) -> float:
    if not phases.empty and {"timestamp", "event", "phase"}.issubset(phases.columns):
        phase_numbers = pd.to_numeric(phases["phase"], errors="coerce")
        phase_starts = phases[phases["event"].astype(str).eq("start") & phase_numbers.eq(1)]
        phase_ts = pd.to_numeric(phase_starts["timestamp"], errors="coerce").dropna()
        if not phase_ts.empty:
            return float(phase_ts.iloc[0])

    combined_ts = pd.to_numeric(combined["timestamp"], errors="coerce").dropna()
    if combined_ts.empty:
        fail("combined_metrics.csv has no numeric timestamps")
    return float(combined_ts.min())


def timestamp_window(dfs: Iterable[pd.DataFrame]) -> tuple[float, float]:
    timestamps = []
    for df in dfs:
        if not df.empty and "timestamp" in df.columns:
            vals = pd.to_numeric(df["timestamp"], errors="coerce").dropna()
            if not vals.empty:
                timestamps.append(vals)
    if not timestamps:
        fail("cannot infer run timestamp window")
    all_ts = pd.concat(timestamps, ignore_index=True)
    return float(all_ts.min()), float(all_ts.max())


def load_task_metrics(run: FormalRun, run_id: str, start_ms: float, end_ms: float) -> pd.DataFrame:
    paths = list(run.formal_metrics.glob("task_metrics_*.csv"))
    paths += list((run.root / "remote_tmp_metrics").glob("node*/task_metrics.csv"))
    frames = []
    for path in sorted(paths):
        df = read_csv(path, required=False)
        if df.empty or "timestamp" not in df.columns:
            continue
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            continue
        if "jobId" in df.columns:
            job_ids = df["jobId"].astype(str)
            matching = job_ids.eq(run_id)
            unknown = job_ids.isin(["", "unknown", "nan"])
            df = df[matching | (unknown & df["timestamp"].between(start_ms, end_ms))]
        else:
            df = df[df["timestamp"].between(start_ms, end_ms)]
        if df.empty:
            continue
        df["metric_node"] = path.parent.name if path.parent.name.startswith("node") else "formal"
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    task = pd.concat(frames, ignore_index=True)
    dedupe_columns = [
        col
        for col in [
            "timestamp",
            "jobId",
            "executorId",
            "taskId",
            "inputReceiveRate",
            "inputRate",
            "outputRate",
        ]
        if col in task.columns
    ]
    task = task.drop_duplicates(subset=dedupe_columns).sort_values("timestamp").reset_index(drop=True)
    return task


def load_inputs(run: FormalRun) -> dict[str, pd.DataFrame]:
    if not run.root.exists():
        fail(f"archive directory does not exist: {run.root}")
    if not run.workdir.exists():
        fail(f"missing workdir: {run.workdir}")
    if not run.formal_metrics.exists():
        fail(f"missing formal_metrics: {run.formal_metrics}")

    combined = read_csv(run.workdir / "combined_metrics.csv")
    if combined.empty or "timestamp" not in combined.columns:
        fail("combined_metrics.csv is empty or missing timestamp")

    scaler_path = first_match("scaler_metrics_am_*.csv", run.formal_metrics)
    decisions_path = first_match("scaling_decisions_am_*.csv", run.formal_metrics)

    scaler = read_csv(scaler_path)
    decisions = read_decisions(decisions_path)
    producer = read_csv(
        run.workdir / "producer_metrics.csv",
        required=False,
    )
    phases = read_csv(run.workdir / "producer_phases.csv")
    kafka_topics = read_csv(
        run.workdir / "kafka_topic_metrics.csv",
        required=False,
    )
    scaler_enable = read_csv(run.workdir / "scaler_enable.csv", required=False)
    if "reason" in scaler_enable.columns:
        scaler_enable = scaler_enable.rename(columns={"reason": "mode"})

    run_id = infer_run_id(run, scaler)
    start_ms, end_ms = timestamp_window(
        [
            combined,
            scaler,
            decisions,
            producer,
            phases,
            kafka_topics,
            scaler_enable,
        ]
    )
    task = load_task_metrics(run, run_id, start_ms, end_ms)
    t0_ms = choose_t0_ms(combined, phases)

    for df in [
        combined,
        scaler,
        decisions,
        task,
        producer,
        phases,
        kafka_topics,
        scaler_enable,
    ]:
        add_relative_seconds(df, t0_ms)
        numeric_columns(
            df,
            exclude={
                "jobId",
                "executorId",
                "taskId",
                "metric_node",
                "action",
                "trigger",
                "event",
                "mode",
                "topic",
            },
        )

    return {
        "combined": combined,
        "scaler": scaler,
        "decisions": decisions,
        "task": task,
        "producer": producer,
        "phases": phases,
        "kafka_topics": kafka_topics,
        "scaler_enable": scaler_enable,
        "run_id": run_id,
        "t0_ms": t0_ms,
    }


def plot_input_processing(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    combined = data["combined"]
    producer = data["producer"]
    plt.figure(figsize=(12, 5))
    if {"totalSent", "timestamp"}.issubset(producer.columns):
        producer = producer.dropna(subset=["timestamp", "totalSent"]).sort_values("timestamp").copy()
        total_delta = producer["totalSent"].diff().dropna()
        if not total_delta.empty and (total_delta < 0).any():
            print(
                "[plot-formal] producer totalSent is not monotonic; cannot compute a global interval rate safely",
                file=sys.stderr,
            )
            if "outputRate" in producer.columns:
                plt.plot(producer["rel_s"], producer["outputRate"], label="Cumulative average producer rate", linewidth=2)
        else:
            interval_rate = producer["totalSent"].diff() / (producer["timestamp"].diff() / 1000.0).replace(0, pd.NA)
            plt.plot(producer["rel_s"], interval_rate, label="Producer interval rate", linewidth=2)
    elif "outputRate" in producer.columns:
        plt.plot(producer["rel_s"], producer["outputRate"], label="Cumulative average producer rate", linewidth=2)
    if "avgInput" in combined.columns:
        plt.plot(combined["rel_s"], combined["avgInput"], label="Scaler avg input")
    if "avgProcess" in combined.columns:
        plt.plot(combined["rel_s"], combined["avgProcess"], label="Scaler avg process")
    if {"timestamp", "sourceCount"}.issubset(combined.columns):
        dt = combined["timestamp"].diff() / 1000.0
        source_rate = combined["sourceCount"].diff() / dt.replace(0, pd.NA)
        plt.plot(
            combined["rel_s"],
            source_rate.rolling(3, min_periods=1, center=True).mean(),
            label="Source count interval rate",
            alpha=0.8,
        )
    plt.xlabel("Time (s)")
    plt.ylabel("Events/s")
    decorate(run, "Formal Run Input And Processing Rates", data["phases"], data["scaler_enable"], data["decisions"])
    save_plot(run, "input_processing_rates")


def plot_kafka(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    combined = data["combined"]
    if "inputLag" in combined.columns and (combined["inputLag"] >= 0).any():
        plt.figure(figsize=(12, 5))
        plt.plot(combined["rel_s"], combined["inputLag"].clip(lower=0), label="Kafka source lag: inputOffset - sourceCount", color="red")
        plt.xlabel("Time (s)")
        plt.ylabel("Events")
        decorate(run, "Formal Run Kafka Source Lag", data["phases"], data["scaler_enable"], data["decisions"])
        save_plot(run, "kafka_source_lag")
    else:
        print("[plot-formal] skipping kafka_source_lag.png: no non-negative inputLag values")

    if "consumerLag" in combined.columns and (combined["consumerLag"] >= 0).any():
        plt.figure(figsize=(12, 5))
        plt.plot(combined["rel_s"], combined["consumerLag"].clip(lower=0), label="Committed consumer lag", color="purple")
        plt.xlabel("Time (s)")
        plt.ylabel("Events")
        decorate(run, "Formal Run Kafka Consumer Lag", data["phases"], data["scaler_enable"], data["decisions"])
        save_plot(run, "kafka_consumer_lag")
    else:
        print("[plot-formal] skipping kafka_consumer_lag.png: no consumerLag column/values")

    offset_cols = ["inputOffset", "consumerCommittedOffset", "consumerLogEndOffset"]
    if all(col in combined.columns for col in offset_cols):
        plt.figure(figsize=(12, 5))
        plt.plot(combined["rel_s"], combined["inputOffset"], label="Topic end offset")
        plt.plot(combined["rel_s"], combined["consumerCommittedOffset"], label="Consumer committed offset")
        plt.plot(combined["rel_s"], combined["consumerLogEndOffset"], label="Consumer log-end offset", linestyle="--")
        plt.xlabel("Time (s)")
        plt.ylabel("Offset")
        decorate(run, "Formal Run Kafka Offsets", data["phases"], data["scaler_enable"], data["decisions"])
        save_plot(run, "kafka_offsets")
    else:
        print("[plot-formal] skipping kafka_offsets.png: missing committed consumer offset fields")


def plot_kafka_topics(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    topic_metrics = data["kafka_topics"]
    required = {"timestamp", "rel_s", "topic", "endOffset", "consumerLag"}
    if topic_metrics.empty or not required.issubset(topic_metrics.columns):
        return

    plt.figure(figsize=(12, 5))
    plotted_rate = False
    for topic, group in topic_metrics.groupby("topic"):
        group = group.sort_values("timestamp")
        group = group[group["endOffset"] >= 0]
        if len(group) < 2:
            continue
        dt = group["timestamp"].diff() / 1000.0
        rate = group["endOffset"].diff() / dt.replace(0, pd.NA)
        plt.plot(
            group["rel_s"],
            rate.rolling(3, min_periods=1, center=True).mean(),
            label=f"{topic} rate",
        )
        plotted_rate = True
    complete_offsets = topic_metrics.pivot_table(
        index="timestamp", columns="topic", values="endOffset", aggfunc="last"
    ).dropna()
    complete_offsets = complete_offsets[
        (complete_offsets >= 0).all(axis=1)
    ]
    if len(complete_offsets) >= 2:
        aggregate = complete_offsets.sum(axis=1)
        dt = aggregate.index.to_series().diff() / 1000.0
        rate = aggregate.diff() / dt.replace(0, pd.NA)
        rel_s = (aggregate.index - data["t0_ms"]) / 1000.0
        plt.plot(
            rel_s,
            rate.rolling(3, min_periods=1, center=True).mean(),
            label="aggregate rate",
            color="black",
            linewidth=2,
        )
        plotted_rate = True
    if plotted_rate:
        plt.xlabel("Time (s)")
        plt.ylabel("Events/s")
        decorate(
            run,
            "Formal Run Kafka Input Rate by Topic",
            data["phases"],
            data["scaler_enable"],
            data["decisions"],
        )
        save_plot(run, "kafka_topic_rates")
    else:
        plt.close()

    plt.figure(figsize=(12, 5))
    plotted_lag = False
    for topic, group in topic_metrics.groupby("topic"):
        valid = group["consumerLag"] >= 0
        if valid.any():
            plt.plot(
                group.loc[valid, "rel_s"],
                group.loc[valid, "consumerLag"],
                label=f"{topic} lag",
            )
            plotted_lag = True
    complete_lag = topic_metrics.pivot_table(
        index="timestamp", columns="topic", values="consumerLag", aggfunc="last"
    ).dropna()
    complete_lag = complete_lag[(complete_lag >= 0).all(axis=1)]
    if not complete_lag.empty:
        aggregate_lag = complete_lag.sum(axis=1)
        rel_s = (aggregate_lag.index - data["t0_ms"]) / 1000.0
        plt.plot(
            rel_s,
            aggregate_lag,
            label="aggregate lag",
            color="black",
            linewidth=2,
        )
        plotted_lag = True
    if plotted_lag:
        plt.xlabel("Time (s)")
        plt.ylabel("Events")
        decorate(
            run,
            "Formal Run Kafka Consumer Lag by Topic",
            data["phases"],
            data["scaler_enable"],
            data["decisions"],
        )
        save_plot(run, "kafka_topic_lag")
    else:
        plt.close()


def plot_queue(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    combined = data["combined"]
    scaler = data["scaler"]
    plt.figure(figsize=(12, 5))
    if "queueSize" in combined.columns:
        plt.plot(combined["rel_s"], combined["queueSize"].clip(lower=0), label="Collector queue size")
    if "queue" in scaler.columns:
        plt.plot(scaler["rel_s"], scaler["queue"].clip(lower=0), label="Scaler queue", alpha=0.8)
    plt.xlabel("Time (s)")
    plt.ylabel("Events")
    decorate(run, "Formal Run Queue Size", data["phases"], data["scaler_enable"], data["decisions"])
    save_plot(run, "queue_size")


def plot_cpu_executors(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    scaler = data["scaler"]
    fig, ax1 = plt.subplots(figsize=(12, 5))
    if "avgCpu" in scaler.columns:
        ax1.plot(scaler["rel_s"], scaler["avgCpu"], label="Scaler baseline-executor avg CPU", color="tab:blue")
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("CPU fraction", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    if "numExecutors" in scaler.columns:
        ax2.plot(scaler["rel_s"], scaler["numExecutors"], label="Baseline executors", color="tab:purple")
    if "numLambdaExecutors" in scaler.columns:
        ax2.plot(scaler["rel_s"], scaler["numLambdaExecutors"], label="Prestarted VMWorker pool", color="tab:red")
    ax2.set_ylabel("Executors / workers")

    plt.sca(ax1)
    decorate(run, "Formal Run Scaler CPU And Executor Counts", data["phases"], data["scaler_enable"], data["decisions"])
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="best", fontsize=8)
    save_plot(run, "cpu_executors")


def plot_latency_queue_time(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    combined = data["combined"]
    queue_cols = [
        ("kafkaQueueTimeUnweightedMeanMs", "unweighted mean", 2.0, "-", 1.0),
        ("kafkaQueueTimeWeightedMeanMs", "weighted mean", 1.4, "--", 0.8),
        ("kafkaQueueTimeP50", "p50", 1.0, ":", 0.55),
        ("kafkaQueueTimeP95", "p95", 1.0, "-.", 0.55),
        ("kafkaQueueTimeP99", "p99", 1.0, "--", 0.55),
    ]
    if any(col in combined.columns for col, _, _, _, _ in queue_cols):
        plt.figure(figsize=(12, 5))
        plotted = False
        for col, label, linewidth, linestyle, alpha in queue_cols:
            if col in combined.columns:
                valid = combined[col] >= 0
                if valid.any():
                    plt.plot(
                        combined.loc[valid, "rel_s"],
                        combined.loc[valid, col],
                        label=label,
                        linewidth=linewidth,
                        linestyle=linestyle,
                        alpha=alpha,
                    )
                    plotted = True
        if plotted:
            plt.xlabel("Time (s)")
            plt.ylabel("Kafka queue residence time (ms)")
            decorate(run, "Formal Run Source Kafka Queue Residence Time", data["phases"], data["scaler_enable"], data["decisions"])
            save_plot(run, "source_kafka_queue_time")
        else:
            plt.close()

    latency_cols = [("latencyMedian", "median"), ("latencyP95", "p95"), ("latencyP99", "p99"), ("latencyTail", "tail")]
    if any(col in combined.columns for col, _ in latency_cols):
        plt.figure(figsize=(12, 5))
        for col, label in latency_cols:
            if col in combined.columns:
                plt.plot(combined["rel_s"], combined[col], label=label)
        plt.xlabel("Time (s)")
        plt.ylabel("Latency (ms)")
        decorate(run, "Formal Run End-to-End Latency", data["phases"], data["scaler_enable"], data["decisions"])
        save_plot(run, "latency")


def plot_task_rates(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    task = data["task"]
    rate_cols = [col for col in ["inputRate", "outputRate"] if col in task.columns]
    if not rate_cols:
        print("[plot-formal] skipping task_rates_by_executor.png: missing task rate columns")
        return
    task = task.copy()
    for col in rate_cols:
        task.loc[task[col] < 0, col] = pd.NA
    plt.figure(figsize=(12, 6))
    grouped = task.groupby(["rel_s", "executorId"], as_index=False)[rate_cols].sum()
    for executor, group in grouped.groupby("executorId"):
        if "inputRate" in group.columns:
            plt.plot(group["rel_s"], group["inputRate"], label=f"{executor} input")
        if "outputRate" in group.columns:
            plt.plot(group["rel_s"], group["outputRate"], linestyle="--", label=f"{executor} output")
    plt.xlabel("Time (s)")
    plt.ylabel("Records/s")
    decorate(run, "Formal Run Task Rates By Executor", data["phases"], data["scaler_enable"], data["decisions"])
    save_plot(run, "task_rates_by_executor")


def plot_scaling_decisions(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    decisions = data["decisions"]
    if decisions.empty or "queueDelay" not in decisions.columns:
        print("[plot-formal] skipping scaling_decisions.png: missing decision queueDelay")
        return
    valid = decisions[pd.to_numeric(decisions["queueDelay"], errors="coerce").ge(0)].copy()
    if valid.empty:
        print("[plot-formal] skipping scaling_decisions.png: no non-negative decision queueDelay values")
        return
    plt.figure(figsize=(12, 5))
    colors = ["green" if action == "SCALE_OUT" else "orange" for action in valid["action"]]
    plt.scatter(valid["rel_s"], valid["queueDelay"], c=colors, s=80, label="decision queue delay")
    if "queueDelayThreshold" in valid.columns and valid["queueDelayThreshold"].notna().any():
        plt.axhline(valid["queueDelayThreshold"].dropna().iloc[0], linestyle="--", color="red", label="queue-delay threshold")
    for _, row in valid.iterrows():
        plt.annotate(str(row["action"]), (row["rel_s"], row["queueDelay"]), textcoords="offset points", xytext=(5, 5), fontsize=9)
    plt.xlabel("Time (s)")
    plt.ylabel("Queue delay (s)")
    decorate(run, "Formal Run Scaling Decisions", data["phases"], data["scaler_enable"], decisions)
    save_plot(run, "scaling_decisions")


def load_node_metrics(run: FormalRun, t0_ms: float) -> pd.DataFrame:
    if not run.node_metrics.exists():
        print("[plot-formal] skipping node plots: missing node_metrics/")
        return pd.DataFrame()
    frames = []
    for path in sorted(run.node_metrics.glob("node*.csv")):
        df = read_csv(path, required=False)
        if df.empty or "timestamp_ms" not in df.columns:
            continue
        df["node"] = path.stem
        df["timestamp_ms"] = pd.to_numeric(df["timestamp_ms"], errors="coerce")
        df["rel_s"] = (df["timestamp_ms"] - t0_ms) / 1000.0
        frames.append(df)
    if not frames:
        print("[plot-formal] skipping node plots: no node*.csv metrics")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def plot_node_metrics(run: FormalRun, data: dict[str, pd.DataFrame]) -> None:
    t0_ms = data["t0_ms"]
    nodes = load_node_metrics(run, t0_ms)
    if nodes.empty:
        return

    if "cpu_util" in nodes.columns:
        plt.figure(figsize=(12, 6))
        for node, group in nodes.groupby("node"):
            plt.plot(group["rel_s"], pd.to_numeric(group["cpu_util"], errors="coerce") * 100.0, label=node, alpha=0.75)
        plt.xlabel("Time (s)")
        plt.ylabel("CPU utilization (%)")
        decorate(run, "Formal Run Per-Node CPU Utilization", data["phases"], data["scaler_enable"], data["decisions"])
        unique_legend(ncol=2)
        save_plot(run, "node_cpu_utilization")

    if "nemo_rss_kb" in nodes.columns:
        plt.figure(figsize=(12, 6))
        positive_nodes = (
            nodes.assign(rss_mb=pd.to_numeric(nodes["nemo_rss_kb"], errors="coerce") / 1024.0)
            .groupby("node")["rss_mb"]
            .max()
        )
        positive_nodes = positive_nodes[positive_nodes > 0]
        if len(positive_nodes) < 2:
            print(
                "[plot-formal] WARNING: Nemo RSS was detected on fewer than two nodes; "
                "the collector process match is probably incomplete",
                file=sys.stderr,
            )
        for node, group in nodes.groupby("node"):
            rss_mb = pd.to_numeric(group["nemo_rss_kb"], errors="coerce") / 1024.0
            if rss_mb.max() > 0:
                plt.plot(group["rel_s"], rss_mb, label=node, alpha=0.8)
        plt.xlabel("Time (s)")
        plt.ylabel("Matched process RSS on node (MiB)")
        decorate(run, "RSS of Processes Matched by Nemo Collector", data["phases"], data["scaler_enable"], data["decisions"])
        save_plot(run, "node_nemo_rss")

    if {"rx_bytes", "tx_bytes", "timestamp_ms"}.issubset(nodes.columns):
        plt.figure(figsize=(12, 6))
        for node, group in nodes.groupby("node"):
            group = group.sort_values("timestamp_ms")
            dt_s = group["timestamp_ms"].diff() / 1000.0
            rx_delta = pd.to_numeric(group["rx_bytes"], errors="coerce").diff()
            tx_delta = pd.to_numeric(group["tx_bytes"], errors="coerce").diff()
            rx_delta = rx_delta.where(rx_delta >= 0)
            tx_delta = tx_delta.where(tx_delta >= 0)
            rx_mib_s = rx_delta / dt_s / (1024 * 1024)
            tx_mib_s = tx_delta / dt_s / (1024 * 1024)
            total_mib_s = (rx_mib_s + tx_mib_s).rolling(5, min_periods=1, center=True).mean()
            if total_mib_s.max() > 0:
                plt.plot(group["rel_s"], total_mib_s, label=node, alpha=0.75)
        plt.xlabel("Time (s)")
        plt.ylabel("RX+TX throughput (MiB/s)")
        decorate(run, "Formal Run Per-Node Network Throughput", data["phases"], data["scaler_enable"], data["decisions"])
        unique_legend(ncol=2)
        save_plot(run, "node_network_throughput")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot a CloudLab formal run archive")
    parser.add_argument("run_dir", type=Path, help="Formal run archive directory, e.g. results/cloudlab/q6-formal-delayed-...")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run = FormalRun(args.run_dir)
    clear_plots(run)
    data = load_inputs(run)

    plot_input_processing(run, data)
    plot_queue(run, data)
    plot_latency_queue_time(run, data)
    plot_scaling_decisions(run, data)
    plot_cpu_executors(run, data)
    plot_kafka(run, data)
    plot_kafka_topics(run, data)
    plot_task_rates(run, data)
    plot_node_metrics(run, data)

    generated = sorted(run.generated_plots)
    print(f"[plot-formal] generated {len(generated)} PNG files in {run.plots}")
    missing = [name for name in EXPECTED_PLOTS if name not in generated]
    if missing:
        print(f"[plot-formal] skipped/missing: {', '.join(missing)}")


if __name__ == "__main__":
    main()
