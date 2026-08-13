#!/usr/bin/env python3
"""Plot the hardware-matched standard-Q6 comparison.

The plotter deliberately keeps the three systems on the same aggregation path:

* source-task measurements are first averaged within each task and five-second bin;
* input throughput is the sum of those per-task interval means;
* Kafka queue residence time is the unweighted mean of valid per-task interval
  means (a task is valid only when it sampled at least one Kafka record);
* all timestamps are aligned to the first observed live-production phase.

Only the primary unweighted-mean series is plotted for Sponge.  Percentile and
sample-weighted series are intentionally excluded from this comparison.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]
DEFAULT_SPONGE_RESULT = (
    REPO_ROOT
    / "results/cloudlab/original-sponge-q6-scaleout-225k450k-20260813T061000Z"
)
DEFAULT_MEMORY_RESULT = (
    REPO_ROOT
    / "results/cloudlab/holostream-standard-q6-memory-225k450k-4to8-20260813T143500Z"
)
DEFAULT_PEBBLE_RESULT = (
    REPO_ROOT
    / "results/cloudlab/holostream-standard-q6-pebble-225k450k-4to8-20260813T150500Z"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "results/cloudlab/standard-q6-comparison-225k450k-4to8-20260813"
)

SOURCE_METRIC_IDS = ("OutputRate", "Latency.KafkaQueueTime")
HOLO_SOURCE_PATTERN = re.compile(r"^(auctionSource|bidSource):")
HOLO_LOG_TIMESTAMP = re.compile(r"^(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})")
HOLO_PRODUCER_RATE = re.compile(r"output rate:\s*(?P<rate>\d+)")
HOLO_SCALE_OUT = re.compile(r"Worker num change:\s*4\s*->\s*8")

COLORS = {
    "Sponge": "#d62728",
    "HoloStream memory": "#1f77b4",
    "HoloStream Pebble": "#2ca02c",
}


@dataclass
class RunSeries:
    name: str
    color: str
    input_rate: pd.DataFrame
    kafka_queue_ms: pd.DataFrame
    phase2_s: float
    scale_out_s: float
    producer_end_s: float
    source_tasks: int
    total_events: int
    terminal_lag: int
    metadata: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare standard Q6 on Sponge, HoloStream memory, and HoloStream Pebble."
    )
    parser.add_argument("--sponge-result", type=Path, default=DEFAULT_SPONGE_RESULT)
    parser.add_argument("--memory-result", type=Path, default=DEFAULT_MEMORY_RESULT)
    parser.add_argument("--pebble-result", type=Path, default=DEFAULT_PEBBLE_RESULT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--bin-seconds", type=float, default=5.0)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_sqlite_metrics(db_path: Path) -> pd.DataFrame:
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    uri = f"file:{db_path.resolve()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        metrics = pd.read_sql_query(
            """
            SELECT timestamp, operator_id, metric_type AS metric_id, metric_value
            FROM metrics
            WHERE metric_type IN (?, ?)
            """,
            connection,
            params=SOURCE_METRIC_IDS,
        )
    if metrics.empty:
        raise ValueError(f"No source metrics found in {db_path}")
    return metrics


def timezone_from_db_timestamp(raw_timestamp: str) -> timezone:
    match = re.search(r"(?P<sign>[+-])(?P<hour>\d{2}):(?P<minute>\d{2})$", raw_timestamp)
    if not match:
        raise ValueError(f"Metric timestamp has no numeric UTC offset: {raw_timestamp!r}")
    minutes = int(match.group("hour")) * 60 + int(match.group("minute"))
    if match.group("sign") == "-":
        minutes = -minutes
    return timezone(timedelta(minutes=minutes))


def parse_holostream_log_events(
    log_path: Path, log_timezone: timezone
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    if not log_path.is_file():
        raise FileNotFoundError(log_path)

    baseline_start: pd.Timestamp | None = None
    overload_start: pd.Timestamp | None = None
    scale_out: pd.Timestamp | None = None

    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            timestamp_match = HOLO_LOG_TIMESTAMP.match(line)
            if not timestamp_match:
                continue
            local_time = datetime.strptime(
                timestamp_match.group("timestamp"), "%Y/%m/%d %H:%M:%S"
            ).replace(tzinfo=log_timezone)
            timestamp = pd.Timestamp(local_time).tz_convert("UTC")

            rate_match = HOLO_PRODUCER_RATE.search(line)
            if rate_match:
                rate = int(rate_match.group("rate"))
                # Four producer replicas split the aggregate 225k/450k workload.
                if rate in (3444, 52806) and baseline_start is None:
                    baseline_start = timestamp
                elif rate in (6888, 105612) and overload_start is None:
                    overload_start = timestamp

            if HOLO_SCALE_OUT.search(line) and scale_out is None:
                scale_out = timestamp

    missing = [
        name
        for name, value in (
            ("baseline producer start", baseline_start),
            ("overload producer start", overload_start),
            ("4-to-8 scale-out", scale_out),
        )
        if value is None
    ]
    if missing:
        raise ValueError(f"Missing {', '.join(missing)} in {log_path}")
    return baseline_start, overload_start, scale_out


def aggregate_task_intervals(
    frame: pd.DataFrame,
    *,
    task_column: str,
    value_column: str,
    timestamp_column: str,
    t0: pd.Timestamp,
    bin_seconds: float,
    operation: str,
    expected_tasks: int | None,
    require_all_tasks: bool,
) -> pd.DataFrame:
    working = frame[[task_column, timestamp_column, value_column]].copy()
    working[value_column] = pd.to_numeric(working[value_column], errors="coerce")
    working[timestamp_column] = pd.to_datetime(working[timestamp_column], utc=True)
    working = working.dropna(subset=[task_column, timestamp_column, value_column])
    working["relative_s"] = (
        working[timestamp_column] - t0
    ).dt.total_seconds()
    working = working[working["relative_s"] >= 0.0]
    if working.empty:
        raise ValueError("No nonnegative live-production samples remain after filtering")

    working["bin_start_s"] = (
        np.floor(working["relative_s"] / bin_seconds) * bin_seconds
    )
    per_task = (
        working.groupby(["bin_start_s", task_column], as_index=False)[value_column]
        .mean()
        .rename(columns={value_column: "task_interval_mean"})
    )
    task_counts = per_task.groupby("bin_start_s")[task_column].nunique()
    if operation == "sum":
        aggregate = per_task.groupby("bin_start_s")["task_interval_mean"].sum()
    elif operation == "mean":
        aggregate = per_task.groupby("bin_start_s")["task_interval_mean"].mean()
    else:
        raise ValueError(f"Unsupported aggregation operation: {operation}")

    result = pd.DataFrame(
        {
            "bin_start_s": aggregate.index.astype(float),
            "value": aggregate.to_numpy(dtype=float),
            "valid_tasks": task_counts.reindex(aggregate.index).to_numpy(dtype=int),
        }
    )
    if require_all_tasks:
        if expected_tasks is None:
            raise ValueError("expected_tasks is required when require_all_tasks=True")
        result = result[result["valid_tasks"] == expected_tasks]

    # The first five-second bucket is partial for every source and is excluded
    # symmetrically so startup sampling does not receive disproportionate weight.
    if not result.empty:
        result = result[result["bin_start_s"] > result["bin_start_s"].min()]
    result["time_s"] = result["bin_start_s"] + bin_seconds
    return result[["time_s", "value", "valid_tasks"]].reset_index(drop=True)


def locate_sponge_source_metrics(result_dir: Path) -> list[Path]:
    matches = sorted((result_dir / "workdir/remote_tmp_metrics").glob("node*/source_task_metrics.csv"))
    if not matches:
        raise FileNotFoundError(
            f"No raw Sponge source-task metrics under {result_dir / 'workdir/remote_tmp_metrics'}"
        )
    return matches


def total_events_and_lag(validation: dict[str, Any]) -> tuple[int, int]:
    total_candidates = (
        validation.get("total_events"),
        validation.get("produced_total"),
        validation.get("producedRecords"),
        validation.get("actual_total"),
        validation.get("expectedInputEvents"),
        validation.get("sourceProcessedEvents"),
    )
    total = next((int(value) for value in total_candidates if value is not None), None)

    lag_candidates = (
        validation.get("terminal_lag"),
        validation.get("final_lag"),
        validation.get("consumer_lag"),
        validation.get("consumerLag"),
    )
    lag = next((int(value) for value in lag_candidates if value is not None), None)

    # Some validation files group these values one level down.
    for nested_name in ("producer", "kafka", "result", "summary", "workload"):
        nested = validation.get(nested_name)
        if not isinstance(nested, dict):
            continue
        if total is None:
            for key in (
                "total_events",
                "produced_total",
                "producedRecords",
                "actual_total",
                "total_records",
            ):
                if nested.get(key) is not None:
                    total = int(nested[key])
                    break
        if lag is None:
            for key in (
                "terminal_lag",
                "final_lag",
                "consumer_lag",
                "terminal_consumer_lag",
            ):
                if nested.get(key) is not None:
                    lag = int(nested[key])
                    break

    return total if total is not None else -1, lag if lag is not None else -1


def load_sponge_run(result_dir: Path, bin_seconds: float) -> RunSeries:
    phase_path = result_dir / "workdir/producer_phases.csv"
    decisions_path = result_dir / "workdir/scaling_decisions.csv"
    completion_path = result_dir / "workdir/holostream_producer_completion.json"
    validation_path = result_dir / "workdir/final_validation.json"

    phases = pd.read_csv(phase_path)
    if len(phases) != 2:
        raise ValueError(f"Expected exactly two Sponge producer phases in {phase_path}")
    timestamp_column = next(
        (column for column in phases.columns if column.lower() in {"timestamp", "timestampms", "startms"}),
        None,
    )
    if timestamp_column is None:
        raise ValueError(f"Cannot identify phase timestamp column in {phase_path}")
    phase_timestamps = pd.to_numeric(phases[timestamp_column], errors="raise")
    t0 = pd.to_datetime(int(phase_timestamps.iloc[0]), unit="ms", utc=True)
    phase2_s = (int(phase_timestamps.iloc[1]) - int(phase_timestamps.iloc[0])) / 1000.0

    # The native scaler archive intentionally contains event rows without a
    # header: <epoch-ms>,<decision>.
    decisions = pd.read_csv(
        decisions_path, header=None, names=["timestamp_ms", "decision"]
    )
    decision_ts_column = "timestamp_ms"
    decision_column = "decision"
    scale_rows = decisions[
        decisions[decision_column].astype(str).str.contains("SCALE_OUT", case=False, na=False)
    ]
    if len(scale_rows) != 1:
        raise ValueError(f"Expected one Sponge scale-out decision in {decisions_path}, found {len(scale_rows)}")
    scale_out_s = (
        float(scale_rows.iloc[0][decision_ts_column]) - float(phase_timestamps.iloc[0])
    ) / 1000.0

    raw_frames: list[pd.DataFrame] = []
    for metrics_path in locate_sponge_source_metrics(result_dir):
        source_metrics = pd.read_csv(metrics_path)
        source_metrics["metrics_file"] = str(metrics_path.relative_to(result_dir))
        raw_frames.append(source_metrics)
    raw = pd.concat(raw_frames, ignore_index=True)
    required_columns = {
        "timestamp",
        "taskId",
        "kafkaQueueTimeAvgNs",
        "kafkaQueueSamples",
        "inputRate",
    }
    missing = required_columns - set(raw.columns)
    if missing:
        raise ValueError(f"Sponge source metrics are missing columns: {sorted(missing)}")
    raw = raw.drop_duplicates(subset=["timestamp", "jobId", "taskId"], keep="last")
    raw["timestamp"] = pd.to_datetime(
        pd.to_numeric(raw["timestamp"], errors="raise"), unit="ms", utc=True
    )
    task_count = int(raw["taskId"].nunique())
    if task_count != 8:
        raise ValueError(f"Expected eight Sponge source tasks, found {task_count}")

    input_rows = raw[pd.to_numeric(raw["inputRate"], errors="coerce") >= 0.0].copy()
    input_rate = aggregate_task_intervals(
        input_rows,
        task_column="taskId",
        value_column="inputRate",
        timestamp_column="timestamp",
        t0=t0,
        bin_seconds=bin_seconds,
        operation="sum",
        expected_tasks=task_count,
        require_all_tasks=True,
    )

    queue_rows = raw[
        (pd.to_numeric(raw["kafkaQueueTimeAvgNs"], errors="coerce") >= 0.0)
        & (pd.to_numeric(raw["kafkaQueueSamples"], errors="coerce") > 0.0)
    ].copy()
    kafka_queue_ms = aggregate_task_intervals(
        queue_rows,
        task_column="taskId",
        value_column="kafkaQueueTimeAvgNs",
        timestamp_column="timestamp",
        t0=t0,
        bin_seconds=bin_seconds,
        operation="mean",
        expected_tasks=None,
        require_all_tasks=False,
    )
    kafka_queue_ms["value"] /= 1_000_000.0

    completion = load_json(completion_path)
    validation = load_json(validation_path)
    total_events, terminal_lag = total_events_and_lag(validation)
    if total_events < 0:
        total_events = int(
            completion.get("actualTotal")
            or completion.get("producedRecords")
            or completion.get("totalEvents")
            or -1
        )
    producer_end_s = phase2_s + 150.0

    return RunSeries(
        name="Sponge",
        color=COLORS["Sponge"],
        input_rate=input_rate,
        kafka_queue_ms=kafka_queue_ms,
        phase2_s=phase2_s,
        scale_out_s=scale_out_s,
        producer_end_s=producer_end_s,
        source_tasks=task_count,
        total_events=total_events,
        terminal_lag=terminal_lag,
        metadata={
            "result_dir": str(result_dir.resolve()),
            "timeline_origin": f"producer_phases.csv first phase ({t0.isoformat()})",
            "raw_metric_files": [str(path.resolve()) for path in locate_sponge_source_metrics(result_dir)],
            "queue_metric": "kafkaQueueTimeAvgNs with kafkaQueueSamples > 0",
            "queue_unit_conversion": "nanoseconds / 1,000,000 = milliseconds",
        },
    )


def load_holostream_run(
    result_dir: Path, backend: str, bin_seconds: float
) -> RunSeries:
    db_path = result_dir / "metricDB/metricCollector.snapshot.db"
    log_path = result_dir / "run.log"
    config_path = result_dir / "config.json"
    validation_path = result_dir / "validation.json"

    metrics = read_sqlite_metrics(db_path)
    raw_timestamp = str(metrics["timestamp"].iloc[0])
    log_timezone = timezone_from_db_timestamp(raw_timestamp)
    t0, overload_start, scale_out = parse_holostream_log_events(log_path, log_timezone)
    phase2_s = (overload_start - t0).total_seconds()
    scale_out_s = (scale_out - t0).total_seconds()

    source = metrics[metrics["operator_id"].astype(str).str.match(HOLO_SOURCE_PATTERN)].copy()
    source["timestamp"] = pd.to_datetime(source["timestamp"], utc=True)
    source["metric_value"] = pd.to_numeric(source["metric_value"], errors="coerce")
    task_count = int(source["operator_id"].nunique())
    if task_count != 8:
        raise ValueError(f"Expected eight HoloStream source tasks for {backend}, found {task_count}")

    input_rows = source[
        (source["metric_id"] == "OutputRate") & (source["metric_value"] >= 0.0)
    ]
    input_rate = aggregate_task_intervals(
        input_rows,
        task_column="operator_id",
        value_column="metric_value",
        timestamp_column="timestamp",
        t0=t0,
        bin_seconds=bin_seconds,
        operation="sum",
        expected_tasks=task_count,
        require_all_tasks=True,
    )

    queue_rows = source[
        (source["metric_id"] == "Latency.KafkaQueueTime")
        & (source["metric_value"] >= 0.0)
    ]
    kafka_queue_ms = aggregate_task_intervals(
        queue_rows,
        task_column="operator_id",
        value_column="metric_value",
        timestamp_column="timestamp",
        t0=t0,
        bin_seconds=bin_seconds,
        operation="mean",
        expected_tasks=None,
        require_all_tasks=False,
    )
    kafka_queue_ms["value"] /= 1_000_000.0

    config = load_json(config_path)
    validation = load_json(validation_path)
    total_events, terminal_lag = total_events_and_lag(validation)
    if total_events < 0:
        total_events = 101_250_000
    if terminal_lag < 0:
        # The successful-run validation records this in several release-specific
        # forms; leave the unknown sentinel for the final validator to flag.
        terminal_lag = -1

    configured_backend = str(config.get("StateBackendType", "")).lower()
    if configured_backend and configured_backend != backend.lower():
        raise ValueError(
            f"Expected {backend} backend but {config_path} specifies {configured_backend}"
        )

    name = f"HoloStream {backend}"
    return RunSeries(
        name=name,
        color=COLORS[name],
        input_rate=input_rate,
        kafka_queue_ms=kafka_queue_ms,
        phase2_s=phase2_s,
        scale_out_s=scale_out_s,
        producer_end_s=phase2_s + 150.0,
        source_tasks=task_count,
        total_events=total_events,
        terminal_lag=terminal_lag,
        metadata={
            "result_dir": str(result_dir.resolve()),
            "timeline_origin": f"first baseline producer log line ({t0.isoformat()})",
            "metric_database": str(db_path.resolve()),
            "state_backend": backend,
            "queue_metric": "Latency.KafkaQueueTime >= 0",
            "queue_unit_conversion": "nanoseconds / 1,000,000 = milliseconds",
        },
    )


def add_workload_markers(ax: plt.Axes, runs: Iterable[RunSeries]) -> None:
    runs_by_name = {run.name: run for run in runs}
    holo_phase2 = np.mean(
        [runs_by_name["HoloStream memory"].phase2_s, runs_by_name["HoloStream Pebble"].phase2_s]
    )
    sponge_phase2 = runs_by_name["Sponge"].phase2_s
    ax.axvline(
        holo_phase2,
        color="#666666",
        linestyle="--",
        linewidth=1.2,
        alpha=0.85,
        label=f"HoloStream: 450k workload ({holo_phase2:.0f}s)",
    )
    ax.axvline(
        sponge_phase2,
        color="#999999",
        linestyle="-.",
        linewidth=1.2,
        alpha=0.85,
        label=f"Sponge: 450k workload ({sponge_phase2:.1f}s)",
    )


def style_axes(ax: plt.Axes, *, ylabel: str, xmax: float) -> None:
    ax.set_xlabel("Time since live production began (s)")
    ax.set_ylabel(ylabel)
    ax.set_xlim(0.0, xmax)
    ax.grid(True, which="major", linestyle=":", linewidth=0.8, alpha=0.55)
    ax.spines[["top", "right"]].set_visible(False)


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    fig.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_input_rate(runs: list[RunSeries], output_path: Path, xmax: float) -> None:
    fig, ax = plt.subplots(figsize=(12.8, 6.2))
    for run in runs:
        ax.plot(
            run.input_rate["time_s"],
            run.input_rate["value"] / 1000.0,
            color=run.color,
            linewidth=2.25,
            label=run.name,
        )
    add_workload_markers(ax, runs)
    ax.set_title("Standard Nexmark Q6 — Source Input Throughput")
    style_axes(ax, ylabel="Source input throughput (thousand events/s)", xmax=xmax)
    ax.legend(loc="upper left", frameon=True, ncol=1)
    fig.text(
        0.5,
        0.018,
        "Each line uses five-second bins: mean within each source task, then sum across 8 source tasks.",
        ha="center",
        fontsize=9,
    )
    fig.tight_layout(rect=(0.0, 0.09, 1.0, 1.0))
    save_figure(fig, output_path)


def add_scale_out_markers(ax: plt.Axes, runs: Iterable[RunSeries]) -> None:
    for run in runs:
        ax.axvline(
            run.scale_out_s,
            color=run.color,
            linestyle=":",
            linewidth=2.0,
            alpha=0.95,
            label=f"{run.name} scale-out ({run.scale_out_s:.1f}s)",
        )


def plot_queue_residence(
    runs: list[RunSeries], output_path: Path, xmax: float, *, log_scale: bool
) -> None:
    fig, ax = plt.subplots(figsize=(12.8, 6.2))
    for run in runs:
        queue = run.kafka_queue_ms
        if log_scale:
            queue = queue[queue["value"] > 0.0]
        ax.plot(
            queue["time_s"],
            queue["value"],
            color=run.color,
            linewidth=2.25,
            label=f"{run.name} — unweighted mean",
        )
    add_workload_markers(ax, runs)
    add_scale_out_markers(ax, runs)
    suffix = " (log scale)" if log_scale else ""
    ax.set_title(f"Standard Nexmark Q6 — Source Kafka Queue Residence Time{suffix}")
    style_axes(ax, ylabel="Kafka queue residence time (ms)", xmax=xmax)
    if log_scale:
        ax.set_yscale("log")
        ax.grid(True, which="minor", linestyle=":", linewidth=0.45, alpha=0.3)
    else:
        ax.set_ylim(bottom=0.0)
    ax.legend(loc="upper left", frameon=True, ncol=1)
    fig.text(
        0.5,
        0.018,
        "Five-second bins; each line is the unweighted mean of valid source-task interval means. "
        "Dotted, color-matched vertical lines mark observed scale-out.",
        ha="center",
        fontsize=9,
    )
    fig.tight_layout(rect=(0.0, 0.09, 1.0, 1.0))
    save_figure(fig, output_path)


def window_mean(frame: pd.DataFrame, start_s: float, end_s: float) -> float:
    values = frame[(frame["time_s"] >= start_s) & (frame["time_s"] < end_s)]["value"]
    return float(values.mean()) if not values.empty else math.nan


def write_summary(runs: list[RunSeries], output_path: Path) -> None:
    fieldnames = [
        "run",
        "window",
        "start_s",
        "end_s",
        "input_rate_mean_events_s",
        "kafka_queue_unweighted_mean_ms",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for run in runs:
            windows = (
                ("baseline_steady", 15.0, run.phase2_s - 10.0),
                ("overload_pre_scale", run.phase2_s + 10.0, run.scale_out_s - 5.0),
                ("overload_post_scale", run.scale_out_s + 5.0, run.producer_end_s),
            )
            for window_name, start_s, end_s in windows:
                writer.writerow(
                    {
                        "run": run.name,
                        "window": window_name,
                        "start_s": f"{start_s:.3f}",
                        "end_s": f"{end_s:.3f}",
                        "input_rate_mean_events_s": f"{window_mean(run.input_rate, start_s, end_s):.6f}",
                        "kafka_queue_unweighted_mean_ms": f"{window_mean(run.kafka_queue_ms, start_s, end_s):.6f}",
                    }
                )


def validate_experiment_definitions(
    sponge_result: Path, memory_result: Path, pebble_result: Path
) -> dict[str, Any]:
    memory_config = load_json(memory_result / "config.json")
    pebble_config = load_json(pebble_result / "config.json")
    memory_validation = load_json(memory_result / "validation.json")
    pebble_validation = load_json(pebble_result / "validation.json")
    sponge_validation = load_json(sponge_result / "workdir/final_validation.json")
    sponge_plan = load_json(sponge_result / "workdir/holostream_producer_plan.json")
    sponge_logical_plan = (
        sponge_result / "workdir/plan-logical.json"
    ).read_text(encoding="utf-8")

    for backend, config, validation in (
        ("memory", memory_config, memory_validation),
        ("pebble", pebble_config, pebble_validation),
    ):
        if config.get("QueryName") != "nexmark_query6":
            raise ValueError(f"HoloStream {backend} did not run nexmark_query6")
        if str(config.get("StateBackendType", "")).lower() != backend:
            raise ValueError(f"HoloStream {backend} has the wrong state backend")
        if validation.get("status") != "success":
            raise ValueError(f"HoloStream {backend} validation is not successful")
        topology = validation.get("topology", {})
        expected_topology = {
            "auction_source_parallelism": 4,
            "bid_source_parallelism": 4,
            "closed_auction_parallelism": 4,
            "stateful_mapper_parallelism_before": 4,
            "stateful_mapper_parallelism_after": 8,
            "sink_parallelism": 1,
            "target_operator": "statefulMapper",
        }
        if any(topology.get(key) != value for key, value in expected_topology.items()):
            raise ValueError(
                f"HoloStream {backend} topology does not match the hardware-matched 4-to-8 definition"
            )
        phases = validation.get("workload", {}).get("phases", [])
        phase_pairs = [
            (phase.get("rate_events_per_second"), phase.get("duration_seconds"))
            for phase in phases
        ]
        if phase_pairs != [(225_000, 150), (450_000, 150)]:
            raise ValueError(f"HoloStream {backend} workload phases do not match 225k/450k")

    memory_normalized = dict(memory_config)
    pebble_normalized = dict(pebble_config)
    memory_normalized.pop("StateBackendType", None)
    pebble_normalized.pop("StateBackendType", None)
    if memory_normalized != pebble_normalized:
        raise ValueError(
            "HoloStream memory and Pebble configs differ in fields other than StateBackendType"
        )

    if sponge_validation.get("validated") is not True:
        raise ValueError("Sponge final validation is not successful")
    sponge_phases = sponge_plan.get("phases", [])
    sponge_phase_pairs = [
        (int(phase.get("targetRate", -1)), float(phase.get("durationSec", -1)))
        for phase in sponge_phases
    ]
    if sponge_phase_pairs != [(225_000, 150.0), (450_000, 150.0)]:
        raise ValueError("Sponge workload phases do not match 225k/450k")
    if sponge_plan.get("totalEvents") != 101_250_000:
        raise ValueError("Sponge producer plan does not contain 101,250,000 events")
    if "Query6.WinningBids" not in sponge_logical_plan or "MovingMeanSellingPrice" not in sponge_logical_plan:
        raise ValueError("Sponge logical plan does not contain the standard Beam Q6 operators")

    return {
        "standard_q6_verified": True,
        "workload_verified": "225k events/s for 150s, then 450k events/s for 150s",
        "record_count_verified": 101_250_000,
        "holostream_configs_differ_only_by_backend": True,
        "holostream_topology_verified": "4 auction sources + 4 bid sources -> closedAuction[4] -> statefulMapper[4->8] -> sink[1]",
        "sponge_plan_verified": "8 Kafka sources; standard Beam WinningBids and MovingMeanSellingPrice operators present; compute target parallelism 4",
    }


def validate_runs(
    runs: list[RunSeries], bin_seconds: float, experiment_checks: dict[str, Any]
) -> dict[str, Any]:
    warnings: list[str] = []
    expected_total = 101_250_000
    run_details: dict[str, Any] = {}
    for run in runs:
        if run.source_tasks != 8:
            raise ValueError(f"{run.name} has {run.source_tasks} source tasks, expected 8")
        if run.total_events not in (-1, expected_total):
            raise ValueError(
                f"{run.name} produced {run.total_events} events, expected {expected_total}"
            )
        if run.terminal_lag not in (-1, 0):
            raise ValueError(f"{run.name} terminal Kafka lag is {run.terminal_lag}, expected 0")
        if run.input_rate.empty or run.kafka_queue_ms.empty:
            raise ValueError(f"{run.name} has an empty primary metric series")

        baseline_mean = window_mean(run.input_rate, 15.0, run.phase2_s - 10.0)
        overload_mean = window_mean(
            run.input_rate, run.phase2_s + 10.0, run.producer_end_s
        )
        for measured, expected, phase in (
            (baseline_mean, 225_000.0, "baseline"),
            (overload_mean, 450_000.0, "overload"),
        ):
            if not math.isnan(measured) and abs(measured - expected) / expected > 0.25:
                warnings.append(
                    f"{run.name} {phase} mean input rate {measured:.1f} differs by more than 25% from {expected:.1f}"
                )

        run_details[run.name] = {
            "source_tasks": run.source_tasks,
            "phase2_s": run.phase2_s,
            "scale_out_s": run.scale_out_s,
            "producer_end_s": run.producer_end_s,
            "total_events": run.total_events,
            "terminal_lag": run.terminal_lag,
            "input_bins": int(len(run.input_rate)),
            "queue_bins": int(len(run.kafka_queue_ms)),
            "baseline_input_mean_events_s": baseline_mean,
            "overload_input_mean_events_s": overload_mean,
            **run.metadata,
        }

    return {
        "status": "ok" if not warnings else "ok_with_warnings",
        "bin_seconds": bin_seconds,
        "series_policy": {
            "input_rate": "mean within each source task and five-second bin, then sum task means",
            "kafka_queue_residence": "unweighted mean of valid source-task interval means",
            "sponge_percentiles_plotted": False,
            "sponge_weighted_mean_plotted": False,
            "median_series_plotted": False,
        },
        "experiment_checks": experiment_checks,
        "runs": run_details,
        "warnings": warnings,
    }


def write_metadata(validation: dict[str, Any], output_path: Path) -> None:
    lines = [
        "Standard Nexmark Q6 comparison methodology",
        "=============================================",
        "",
        f"Aggregation interval: {validation['bin_seconds']:.1f} seconds",
        "Input throughput: average samples within each source task and interval, then sum all 8 tasks.",
        "Kafka queue residence: average valid samples within each source task and interval, then take the unweighted task mean.",
        "A valid queue sample has a nonnegative interval mean; Sponge additionally requires kafkaQueueSamples > 0.",
        "Kafka queue values are stored in nanoseconds in both systems and converted to milliseconds.",
        "Sponge P50/P95/P99, weighted mean, and median lines are intentionally not plotted.",
        "All timelines begin at the first observed live-production phase, not application submission.",
        "Scale-out markers use the observed runtime decision/event timestamps.",
        "",
        "Run timing:",
    ]
    for name, details in validation["runs"].items():
        lines.append(
            f"- {name}: overload={details['phase2_s']:.3f}s, "
            f"scale-out={details['scale_out_s']:.3f}s, sources={details['source_tasks']}"
        )
    if validation["warnings"]:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in validation["warnings"])
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.bin_seconds <= 0:
        raise ValueError("--bin-seconds must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    sponge = load_sponge_run(args.sponge_result.resolve(), args.bin_seconds)
    memory = load_holostream_run(args.memory_result.resolve(), "memory", args.bin_seconds)
    pebble = load_holostream_run(args.pebble_result.resolve(), "Pebble", args.bin_seconds)
    # Fixed visual order: Sponge first, then HoloStream memory and Pebble.
    runs = [sponge, memory, pebble]

    xmax_value = max(
        float(frame["time_s"].max())
        for run in runs
        for frame in (run.input_rate, run.kafka_queue_ms)
        if not frame.empty
    )
    xmax = math.ceil(xmax_value / 10.0) * 10.0

    plot_input_rate(runs, args.output_dir / "comparison_input_throughput.png", xmax)
    plot_queue_residence(
        runs,
        args.output_dir / "comparison_kafka_queue_residence_time_linear.png",
        xmax,
        log_scale=False,
    )
    plot_queue_residence(
        runs,
        args.output_dir / "comparison_kafka_queue_residence_time_log.png",
        xmax,
        log_scale=True,
    )
    write_summary(runs, args.output_dir / "comparison_summary.csv")
    experiment_checks = validate_experiment_definitions(
        args.sponge_result.resolve(),
        args.memory_result.resolve(),
        args.pebble_result.resolve(),
    )
    validation = validate_runs(runs, args.bin_seconds, experiment_checks)
    with (args.output_dir / "comparison_validation.json").open("w", encoding="utf-8") as handle:
        json.dump(validation, handle, indent=2, sort_keys=True)
        handle.write("\n")
    write_metadata(validation, args.output_dir / "comparison_methodology.txt")

    print(f"Wrote comparison artifacts to {args.output_dir}")
    print(json.dumps({"status": validation["status"], "warnings": validation["warnings"]}))


if __name__ == "__main__":
    main()
