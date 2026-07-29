#!/usr/bin/env python3
"""Launch and supervise unchanged HoloStream Nexmark Kafka producers for Sponge."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_AUCTION_TOPIC = "nexmark-auction"
DEFAULT_BID_TOPIC = "nexmark-bid"
DEFAULT_KAFKA_HOME = "/users/akash01/kafka"
DEFAULT_KAFKA_NODE = "node1"
DEFAULT_BOOTSTRAP = "node1:9092,node2:9092,node3:9092"
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=10")
SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9._-]+$")
GO_DURATION = re.compile(r"^([0-9]+(?:\.[0-9]+)?)(ns|us|µs|ms|s|m|h)$")
GO_DURATION_UNIT_MS = {
    "ns": 0.000001,
    "us": 0.001,
    "µs": 0.001,
    "ms": 1.0,
    "s": 1000.0,
    "m": 60_000.0,
    "h": 3_600_000.0,
}


class LauncherError(RuntimeError):
    """A failure which makes the produced dataset unusable."""


class LauncherInterrupted(LauncherError):
    """The launcher received SIGINT or SIGTERM."""

    def __init__(self, signum: int):
        super().__init__(f"interrupted by signal {signum}")
        self.signum = signum


@dataclass(frozen=True)
class SourcePlan:
    event_type: str
    num_events_per_replica: int
    rates_per_interval: tuple[int, ...]
    interval_counts: tuple[int, ...]
    generator_interval_ms: float

    @property
    def scheduled_events_per_replica(self) -> int:
        return sum(
            rate * interval_count
            for rate, interval_count in zip(
                self.rates_per_interval, self.interval_counts
            )
        )


@dataclass(frozen=True)
class ProducerPlan:
    config_path: Path
    config_sha256: str
    work_dir: str
    producer_hosts: tuple[str, ...]
    auction: SourcePlan
    bid: SourcePlan

    @property
    def replicas(self) -> int:
        return len(self.producer_hosts)


@dataclass
class ReplicaProcess:
    replica_id: int
    host: str
    remote_config: str
    remote_pid_file: str
    log_path: Path
    log_handle: Any
    process: subprocess.Popen[str]
    termination: str = ""
    cleanup_error: str = ""


@dataclass
class RunState:
    replicas: list[ReplicaProcess] = field(default_factory=list)
    producer_sha256_by_host: dict[str, str] = field(default_factory=dict)
    production_started_ms: int = 0
    auction_offsets: dict[int, int] = field(default_factory=dict)
    bid_offsets: dict[int, int] = field(default_factory=dict)
    offset_snapshots: list[dict[str, Any]] = field(default_factory=list)
    emitted_phases: set[int] = field(default_factory=set)
    cleanup_errors: list[str] = field(default_factory=list)


_interrupted_signal: int | None = None


def _handle_signal(signum: int, _frame: Any) -> None:
    global _interrupted_signal
    _interrupted_signal = signum


def _check_interrupted() -> None:
    if _interrupted_signal is not None:
        raise LauncherInterrupted(_interrupted_signal)


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LauncherError(f"{label} must be a positive integer, got {value!r}")
    return value


def _positive_int_tuple(value: Any, label: str) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise LauncherError(f"{label} must be a non-empty list")
    return tuple(_positive_int(item, f"{label}[{index}]") for index, item in enumerate(value))


def _duration_ms(value: Any, label: str) -> float:
    if not isinstance(value, str):
        raise LauncherError(f"{label} must be a Go duration string")
    match = GO_DURATION.fullmatch(value)
    if match is None:
        raise LauncherError(
            f"{label}={value!r} is unsupported; use one positive duration component"
        )
    duration_ms = float(match.group(1)) * GO_DURATION_UNIT_MS[match.group(2)]
    if duration_ms <= 0:
        raise LauncherError(f"{label} must be positive")
    return duration_ms


def _source_plan(raw_source: dict[str, Any], expected_type: str) -> SourcePlan:
    if raw_source.get("EventType") != expected_type:
        raise LauncherError(
            f"expected {expected_type} source, got {raw_source.get('EventType')!r}"
        )
    if "Topic" in raw_source:
        raise LauncherError(
            f"{expected_type} source contains Topic override unsupported by unchanged HoloStream"
        )
    source_config = raw_source.get("NexmarkSourceConfig")
    if not isinstance(source_config, dict):
        raise LauncherError(f"{expected_type} source is missing NexmarkSourceConfig")
    rate_config = source_config.get("RateLimiterConfig")
    if not isinstance(rate_config, dict) or rate_config.get("UseRateLimit") is not True:
        raise LauncherError(f"{expected_type} source must enable RateLimiterConfig")

    rates = _positive_int_tuple(
        rate_config.get("RateLimit"), f"{expected_type}.RateLimit"
    )
    intervals = _positive_int_tuple(
        rate_config.get("RateChangeInterval"),
        f"{expected_type}.RateChangeInterval",
    )
    if len(rates) != len(intervals):
        raise LauncherError(
            f"{expected_type} RateLimit and RateChangeInterval lengths differ"
        )

    plan = SourcePlan(
        event_type=expected_type,
        num_events_per_replica=_positive_int(
            source_config.get("NumEvents"), f"{expected_type}.NumEvents"
        ),
        rates_per_interval=rates,
        interval_counts=intervals,
        generator_interval_ms=_duration_ms(
            rate_config.get("GeneratorInterval"),
            f"{expected_type}.GeneratorInterval",
        ),
    )
    if plan.scheduled_events_per_replica != plan.num_events_per_replica:
        raise LauncherError(
            f"{expected_type} NumEvents={plan.num_events_per_replica} but its "
            f"rate schedule produces {plan.scheduled_events_per_replica}"
        )
    return plan


def load_plan(config_path: Path, expected_partitions: int) -> ProducerPlan:
    try:
        config_bytes = config_path.read_bytes()
        raw = json.loads(config_bytes)
    except (OSError, json.JSONDecodeError) as error:
        raise LauncherError(f"cannot read producer config {config_path}: {error}") from error

    if not isinstance(raw, dict):
        raise LauncherError("producer config root must be an object")
    if "ExitOnCompletion" in raw:
        raise LauncherError(
            "producer config contains ExitOnCompletion unsupported by unchanged HoloStream"
        )

    work_dir = raw.get("WorkDir")
    if not isinstance(work_dir, str) or not work_dir.strip():
        raise LauncherError("producer config WorkDir must be a non-empty string")

    producer_hosts = raw.get("ProducerIPs")
    if (
        not isinstance(producer_hosts, list)
        or not producer_hosts
        or any(not isinstance(host, str) or not host.strip() for host in producer_hosts)
    ):
        raise LauncherError("ProducerIPs must be a non-empty list of hosts")
    if len(producer_hosts) != expected_partitions:
        raise LauncherError(
            f"ProducerIPs has {len(producer_hosts)} replicas, "
            f"expected {expected_partitions}"
        )

    raw_sources = raw.get("NexmarkLogicalSourceConfigs")
    if not isinstance(raw_sources, list):
        raise LauncherError("NexmarkLogicalSourceConfigs must be a list")
    sources_by_type: dict[str, dict[str, Any]] = {}
    for raw_source in raw_sources:
        if not isinstance(raw_source, dict):
            raise LauncherError("each logical source must be an object")
        event_type = raw_source.get("EventType")
        if event_type in sources_by_type:
            raise LauncherError(f"duplicate logical source {event_type!r}")
        if isinstance(event_type, str):
            sources_by_type[event_type] = raw_source
    if set(sources_by_type) != {"Auction", "Bid"}:
        raise LauncherError(
            "formal Sponge producer config must contain exactly Auction and Bid sources"
        )

    auction = _source_plan(sources_by_type["Auction"], "Auction")
    bid = _source_plan(sources_by_type["Bid"], "Bid")
    if auction.interval_counts != bid.interval_counts:
        raise LauncherError("Auction and Bid phase interval counts must match")
    if auction.generator_interval_ms != bid.generator_interval_ms:
        raise LauncherError("Auction and Bid GeneratorInterval values must match")

    return ProducerPlan(
        config_path=config_path.resolve(),
        config_sha256=hashlib.sha256(config_bytes).hexdigest(),
        work_dir=work_dir,
        producer_hosts=tuple(producer_hosts),
        auction=auction,
        bid=bid,
    )


def describe_plan(plan: ProducerPlan) -> dict[str, Any]:
    phases = []
    cumulative_auction = 0
    cumulative_bid = 0
    for index, (auction_rate, bid_rate, interval_count) in enumerate(
        zip(
            plan.auction.rates_per_interval,
            plan.bid.rates_per_interval,
            plan.auction.interval_counts,
        ),
        start=1,
    ):
        phase_auction = auction_rate * interval_count
        phase_bid = bid_rate * interval_count
        cumulative_auction += phase_auction
        cumulative_bid += phase_bid
        phases.append(
            {
                "phase": index,
                "durationSec": (
                    interval_count * plan.auction.generator_interval_ms / 1000.0
                ),
                "targetRate": (
                    (auction_rate + bid_rate)
                    * plan.replicas
                    * 1000.0
                    / plan.auction.generator_interval_ms
                ),
                "auctionPerReplica": phase_auction,
                "bidPerReplica": phase_bid,
                "cumulativeAuctionPerReplica": cumulative_auction,
                "cumulativeBidPerReplica": cumulative_bid,
            }
        )
    return {
        "configPath": str(plan.config_path),
        "configSha256": plan.config_sha256,
        "workDir": plan.work_dir,
        "replicas": plan.replicas,
        "auctionTopic": DEFAULT_AUCTION_TOPIC,
        "bidTopic": DEFAULT_BID_TOPIC,
        "auctionPerPartition": plan.auction.num_events_per_replica,
        "bidPerPartition": plan.bid.num_events_per_replica,
        "auctionTotal": plan.auction.num_events_per_replica * plan.replicas,
        "bidTotal": plan.bid.num_events_per_replica * plan.replicas,
        "totalEvents": (
            plan.auction.num_events_per_replica + plan.bid.num_events_per_replica
        )
        * plan.replicas,
        "phases": phases,
    }


def _run_checked(
    command: list[str],
    *,
    label: str,
    timeout: int = 30,
    check_interrupted: bool = True,
) -> str:
    if check_interrupted:
        _check_interrupted()
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise LauncherError(f"{label} failed: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        raise LauncherError(f"{label} exited {result.returncode}: {detail}")
    return result.stdout


def _ssh(
    host: str,
    remote_command: str,
    *,
    label: str,
    timeout: int = 30,
    check_interrupted: bool = True,
) -> str:
    return _run_checked(
        ["ssh", *SSH_OPTIONS, host, remote_command],
        label=label,
        timeout=timeout,
        check_interrupted=check_interrupted,
    )


def stage_config(plan: ProducerPlan, run_id: str) -> tuple[str, str]:
    remote_dir = f"/tmp/sponge-holostream-{run_id}"
    remote_config = f"{remote_dir}/producer.json"
    for host in dict.fromkeys(plan.producer_hosts):
        _ssh(
            host,
            f"mkdir -p {shlex.quote(remote_dir)}",
            label=f"create remote run directory on {host}",
        )
        _run_checked(
            [
                "scp",
                *SSH_OPTIONS,
                str(plan.config_path),
                f"{host}:{remote_config}",
            ],
            label=f"copy producer config to {host}",
        )
        remote_hash = _ssh(
            host,
            f"sha256sum {shlex.quote(remote_config)}",
            label=f"hash remote producer config on {host}",
        ).split()[0]
        if remote_hash != plan.config_sha256:
            raise LauncherError(
                f"producer config checksum mismatch on {host}: "
                f"{remote_hash} != {plan.config_sha256}"
            )
    return remote_dir, remote_config


def verify_producer_binaries(
    plan: ProducerPlan,
    producer_binary: str,
    expected_sha256: str,
) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for host in dict.fromkeys(plan.producer_hosts):
        remote_command = (
            f"cd {shlex.quote(plan.work_dir)} && "
            f"test -x {shlex.quote(producer_binary)} && "
            f"sha256sum -- {shlex.quote(producer_binary)}"
        )
        output = _ssh(
            host,
            remote_command,
            label=f"verify producer binary on {host}",
        )
        match = re.match(r"^([0-9a-fA-F]{64})\s", output)
        if match is None:
            raise LauncherError(
                f"could not parse producer binary checksum from {host}: "
                f"{output.strip()!r}"
            )
        hashes[host] = match.group(1).lower()

    distinct_hashes = set(hashes.values())
    if len(distinct_hashes) != 1:
        raise LauncherError(
            f"producer binary checksums differ across hosts: {hashes}"
        )
    actual_sha256 = next(iter(distinct_hashes))
    if expected_sha256 and actual_sha256 != expected_sha256.lower():
        raise LauncherError(
            f"producer binary checksum {actual_sha256} does not match expected "
            f"{expected_sha256.lower()}"
        )
    return hashes


def launch_replicas(
    plan: ProducerPlan,
    remote_dir: str,
    remote_config: str,
    producer_binary: str,
    output_dir: Path,
    state: RunState,
) -> None:
    for replica_id, host in enumerate(plan.producer_hosts):
        _check_interrupted()
        remote_pid_file = f"{remote_dir}/replica-{replica_id}.pid"
        producer_args = [
            producer_binary,
            remote_config,
            str(replica_id),
        ]
        inner_script = (
            f"echo $$ > {shlex.quote(remote_pid_file)}; "
            f"exec {shlex.join(producer_args)}"
        )
        remote_command = (
            f"cd {shlex.quote(plan.work_dir)} && "
            f"sh -c {shlex.quote(inner_script)}"
        )
        log_path = output_dir / f"holostream-producer-replica-{replica_id}.log"
        log_handle = log_path.open("w")
        try:
            process = subprocess.Popen(
                ["ssh", *SSH_OPTIONS, host, remote_command],
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except OSError as error:
            log_handle.close()
            raise LauncherError(
                f"failed to launch replica {replica_id} on {host}: {error}"
            ) from error
        state.replicas.append(
            ReplicaProcess(
                replica_id=replica_id,
                host=host,
                remote_config=remote_config,
                remote_pid_file=remote_pid_file,
                log_path=log_path,
                log_handle=log_handle,
                process=process,
            )
        )
        print(
            f"[holostream] launched replica={replica_id} host={host} "
            f"ssh_pid={process.pid} log={log_path}",
            flush=True,
        )


def get_partition_offsets(
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    topic: str,
) -> dict[int, int]:
    kafka_tool = f"{kafka_home}/bin/kafka-run-class.sh"
    remote_command = shlex.join(
        [
            kafka_tool,
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            bootstrap_servers,
            "--topic",
            topic,
            "--time",
            "-1",
        ]
    )
    output = _ssh(
        kafka_node,
        remote_command,
        label=f"read offsets for {topic}",
        timeout=20,
    )
    offsets: dict[int, int] = {}
    for line in output.splitlines():
        match = re.search(r"^([^:]+):(\d+):(\d+)\s*$", line.strip())
        if match and match.group(1) == topic:
            offsets[int(match.group(2))] = int(match.group(3))
    if not offsets:
        raise LauncherError(f"offset command returned no partitions for {topic}")
    return offsets


def validate_partition_set(
    offsets: dict[int, int],
    expected_partitions: int,
    topic: str,
) -> None:
    expected = set(range(expected_partitions))
    actual = set(offsets)
    if actual != expected:
        raise LauncherError(
            f"{topic} partitions are {sorted(actual)}, expected {sorted(expected)}"
        )


def require_empty_topics(
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    auction_topic: str,
    bid_topic: str,
    expected_partitions: int,
) -> None:
    for topic in (auction_topic, bid_topic):
        offsets = get_partition_offsets(
            kafka_node, kafka_home, bootstrap_servers, topic
        )
        validate_partition_set(offsets, expected_partitions, topic)
        nonempty = {partition: offset for partition, offset in offsets.items() if offset != 0}
        if nonempty:
            raise LauncherError(
                f"{topic} is not empty before production: {nonempty}; "
                "reset the fixed HoloStream topics first"
            )


def _snapshot(
    state: RunState,
    auction_offsets: dict[int, int],
    bid_offsets: dict[int, int],
) -> None:
    state.auction_offsets = dict(auction_offsets)
    state.bid_offsets = dict(bid_offsets)
    state.offset_snapshots.append(
        {
            "timestampMs": int(time.time() * 1000),
            "auctionOffsets": auction_offsets,
            "bidOffsets": bid_offsets,
        }
    )


def emit_started_phases(
    plan: ProducerPlan,
    state: RunState,
    phase_path: Path,
    auction_offsets: dict[int, int],
    bid_offsets: dict[int, int],
) -> None:
    phases = describe_plan(plan)["phases"]
    for phase in phases:
        phase_number = int(phase["phase"])
        if phase_number in state.emitted_phases:
            continue
        if phase_number == 1:
            threshold_auction = 0
            threshold_bid = 0
        else:
            previous_phase = phases[phase_number - 2]
            threshold_auction = int(previous_phase["cumulativeAuctionPerReplica"])
            threshold_bid = int(previous_phase["cumulativeBidPerReplica"])
        if not all(
            auction_offsets.get(replica_id, 0) >= threshold_auction
            and bid_offsets.get(replica_id, 0) >= threshold_bid
            for replica_id in range(plan.replicas)
        ):
            break

        phase_path.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not phase_path.exists() or phase_path.stat().st_size == 0
        with phase_path.open("a", newline="") as output:
            writer = csv.writer(output)
            if needs_header:
                writer.writerow(
                    [
                        "timestamp",
                        "event",
                        "phase",
                        "targetRate",
                        "durationSec",
                        "totalSent",
                        "elapsedMs",
                    ]
                )
            timestamp_ms = int(time.time() * 1000)
            writer.writerow(
                [
                    timestamp_ms,
                    "start",
                    phase_number,
                    round(float(phase["targetRate"])),
                    float(phase["durationSec"]),
                    sum(auction_offsets.values()) + sum(bid_offsets.values()),
                    max(0, timestamp_ms - state.production_started_ms),
                ]
            )
        state.emitted_phases.add(phase_number)
        print(
            f"[holostream] phase {phase_number} started; "
            f"targetRate={round(float(phase['targetRate']))}",
            flush=True,
        )


def monitor_completion(
    plan: ProducerPlan,
    state: RunState,
    *,
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    auction_topic: str,
    bid_topic: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
    max_offset_errors: int,
    phase_path: Path,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    consecutive_errors = 0
    last_progress: tuple[int, int] | None = None
    emit_started_phases(plan, state, phase_path, {}, {})

    while time.monotonic() < deadline:
        _check_interrupted()
        try:
            auction_offsets = get_partition_offsets(
                kafka_node, kafka_home, bootstrap_servers, auction_topic
            )
            bid_offsets = get_partition_offsets(
                kafka_node, kafka_home, bootstrap_servers, bid_topic
            )
            validate_partition_set(auction_offsets, plan.replicas, auction_topic)
            validate_partition_set(bid_offsets, plan.replicas, bid_topic)
            consecutive_errors = 0
        except LauncherError as error:
            consecutive_errors += 1
            if consecutive_errors > max_offset_errors:
                raise LauncherError(
                    f"Kafka offset polling failed {consecutive_errors} times: {error}"
                ) from error
            print(
                f"[holostream] transient offset error "
                f"{consecutive_errors}/{max_offset_errors}: {error}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(poll_interval_seconds)
            continue

        _snapshot(state, auction_offsets, bid_offsets)
        emit_started_phases(
            plan,
            state,
            phase_path,
            auction_offsets,
            bid_offsets,
        )
        for replica_id in range(plan.replicas):
            auction_offset = auction_offsets[replica_id]
            bid_offset = bid_offsets[replica_id]
            if auction_offset > plan.auction.num_events_per_replica:
                raise LauncherError(
                    f"{auction_topic} partition {replica_id} offset "
                    f"{auction_offset} exceeds expected "
                    f"{plan.auction.num_events_per_replica}"
                )
            if bid_offset > plan.bid.num_events_per_replica:
                raise LauncherError(
                    f"{bid_topic} partition {replica_id} offset {bid_offset} "
                    f"exceeds expected {plan.bid.num_events_per_replica}"
                )

        auction_total = sum(auction_offsets.values())
        bid_total = sum(bid_offsets.values())
        progress = (auction_total, bid_total)
        if progress != last_progress:
            print(
                f"[holostream] progress auctions={auction_total}/"
                f"{plan.auction.num_events_per_replica * plan.replicas} "
                f"bids={bid_total}/"
                f"{plan.bid.num_events_per_replica * plan.replicas}",
                flush=True,
            )
            last_progress = progress

        complete = all(
            auction_offsets[replica_id] == plan.auction.num_events_per_replica
            and bid_offsets[replica_id] == plan.bid.num_events_per_replica
            for replica_id in range(plan.replicas)
        )
        if complete:
            print("[holostream] all producer partitions reached exact offsets", flush=True)
            return

        for replica in state.replicas:
            exit_code = replica.process.poll()
            if exit_code is None:
                continue
            replica_complete = (
                auction_offsets.get(replica.replica_id)
                == plan.auction.num_events_per_replica
                and bid_offsets.get(replica.replica_id)
                == plan.bid.num_events_per_replica
            )
            if not replica_complete:
                raise LauncherError(
                    f"replica {replica.replica_id} on {replica.host} exited "
                    f"{exit_code} before its partitions completed; see "
                    f"{replica.log_path}"
                )

        time.sleep(poll_interval_seconds)

    raise LauncherError(
        f"producer completion timed out after {timeout_seconds} seconds"
    )


def _signal_replica(replica: ReplicaProcess, signal_name: str) -> None:
    script = """
pid=$(cat "$1") || exit 2
args=$(ps -p "$pid" -o args=) || exit 3
case "$args" in
  *"$2 $3") kill "-$4" "$pid" ;;
  *) echo "PID $pid args do not match expected config/replica: $args" >&2; exit 4 ;;
esac
""".strip()
    remote_command = shlex.join(
        [
            "sh",
            "-c",
            script,
            "sh",
            replica.remote_pid_file,
            replica.remote_config,
            str(replica.replica_id),
            signal_name,
        ]
    )
    _ssh(
        replica.host,
        remote_command,
        label=(
            f"send {signal_name} to replica {replica.replica_id} "
            f"on {replica.host}"
        ),
        check_interrupted=False,
    )


def cleanup_replicas(
    state: RunState,
    *,
    terminate_timeout_seconds: int,
) -> None:
    for replica in state.replicas:
        if replica.process.poll() is not None:
            replica.termination = "already-exited"
            continue
        try:
            _signal_replica(replica, "TERM")
            replica.termination = "SIGTERM"
        except LauncherError as error:
            if replica.process.poll() is None:
                replica.cleanup_error = str(error)
                state.cleanup_errors.append(str(error))
            else:
                replica.termination = "already-exited"

    deadline = time.monotonic() + terminate_timeout_seconds
    for replica in state.replicas:
        if replica.process.poll() is not None:
            continue
        remaining = max(0.0, deadline - time.monotonic())
        try:
            replica.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            try:
                _signal_replica(replica, "KILL")
                replica.termination = "SIGKILL"
            except LauncherError as error:
                if replica.process.poll() is None:
                    replica.cleanup_error = str(error)
                    state.cleanup_errors.append(str(error))
                else:
                    replica.termination = "already-exited"

    for replica in state.replicas:
        if replica.process.poll() is None:
            try:
                replica.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                error = (
                    f"replica {replica.replica_id} SSH process did not exit "
                    "after remote cleanup"
                )
                replica.cleanup_error = error
                state.cleanup_errors.append(error)
                replica.process.terminate()
        if not replica.log_handle.closed:
            replica.log_handle.close()


def write_artifacts(
    output_dir: Path,
    run_id: str,
    plan: ProducerPlan | None,
    state: RunState,
    *,
    started_ms: int,
    ended_ms: int,
    status: str,
    error: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for replica in state.replicas:
        rows.append(
            {
                "replica": replica.replica_id,
                "host": replica.host,
                "auctionOffset": state.auction_offsets.get(replica.replica_id, -1),
                "bidOffset": state.bid_offsets.get(replica.replica_id, -1),
                "exitCode": replica.process.poll(),
                "termination": replica.termination,
                "cleanupError": replica.cleanup_error,
            }
        )

    csv_path = output_dir / "holostream_producer_replicas.csv"
    with csv_path.open("w", newline="") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "replica",
                "host",
                "auctionOffset",
                "bidOffset",
                "exitCode",
                "termination",
                "cleanupError",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    manifest = {
        "runId": run_id,
        "status": status,
        "error": error,
        "startedMs": started_ms,
        "endedMs": ended_ms,
        "durationMs": max(0, ended_ms - started_ms),
        "configPath": str(plan.config_path) if plan else "",
        "configSha256": plan.config_sha256 if plan else "",
        "producerSha256ByHost": state.producer_sha256_by_host,
        "workDir": plan.work_dir if plan else "",
        "replicas": plan.replicas if plan else 0,
        "expectedAuctionPerPartition": (
            plan.auction.num_events_per_replica if plan else -1
        ),
        "expectedBidPerPartition": (
            plan.bid.num_events_per_replica if plan else -1
        ),
        "expectedAuctionTotal": (
            plan.auction.num_events_per_replica * plan.replicas if plan else -1
        ),
        "expectedBidTotal": (
            plan.bid.num_events_per_replica * plan.replicas if plan else -1
        ),
        "phases": describe_plan(plan)["phases"] if plan else [],
        "emittedPhases": sorted(state.emitted_phases),
        "finalAuctionOffsets": state.auction_offsets,
        "finalBidOffsets": state.bid_offsets,
        "replicaResults": rows,
        "cleanupErrors": state.cleanup_errors,
        "offsetSnapshots": state.offset_snapshots,
    }
    manifest_path = output_dir / "holostream_producer_completion.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--kafka-node", default=DEFAULT_KAFKA_NODE)
    parser.add_argument("--kafka-home", default=DEFAULT_KAFKA_HOME)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--auction-topic", default=DEFAULT_AUCTION_TOPIC)
    parser.add_argument("--bid-topic", default=DEFAULT_BID_TOPIC)
    parser.add_argument("--producer-binary", default="bin/nexmarkKafkaProducer")
    parser.add_argument(
        "--expected-producer-sha256",
        default="",
        help=(
            "require the deployed producer binary to match this SHA-256; "
            "all producer hosts must match each other regardless"
        ),
    )
    parser.add_argument("--expected-partitions", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-offset-errors", type=int, default=3)
    parser.add_argument("--terminate-timeout-seconds", type=int, default=20)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate the local workload config without SSH or Kafka access",
    )
    parser.add_argument(
        "--describe-json",
        action="store_true",
        help="print the validated workload plan as JSON without SSH or Kafka access",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    if not SAFE_RUN_ID.fullmatch(args.run_id):
        print(
            "ERROR: --run-id may contain only letters, numbers, '.', '_' and '-'",
            file=sys.stderr,
        )
        return 2
    if args.expected_partitions <= 0:
        print("ERROR: --expected-partitions must be positive", file=sys.stderr)
        return 2
    if args.timeout_seconds <= 0 or args.poll_interval_seconds <= 0:
        print("ERROR: timeout and poll interval must be positive", file=sys.stderr)
        return 2
    if args.max_offset_errors < 0 or args.terminate_timeout_seconds <= 0:
        print(
            "ERROR: max offset errors must be non-negative and terminate "
            "timeout must be positive",
            file=sys.stderr,
        )
        return 2
    if not args.producer_binary:
        print("ERROR: --producer-binary must not be empty", file=sys.stderr)
        return 2
    if args.expected_producer_sha256 and not re.fullmatch(
        r"[0-9a-fA-F]{64}", args.expected_producer_sha256
    ):
        print(
            "ERROR: --expected-producer-sha256 must contain 64 hexadecimal characters",
            file=sys.stderr,
        )
        return 2
    if (
        args.auction_topic != DEFAULT_AUCTION_TOPIC
        or args.bid_topic != DEFAULT_BID_TOPIC
    ):
        print(
            "ERROR: unchanged HoloStream requires topics "
            f"{DEFAULT_AUCTION_TOPIC} and {DEFAULT_BID_TOPIC}",
            file=sys.stderr,
        )
        return 2

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    started_ms = int(time.time() * 1000)
    state = RunState()
    plan: ProducerPlan | None = None
    status = "failed"
    error_message = ""
    exit_code = 1

    try:
        plan = load_plan(args.config, args.expected_partitions)
        if args.describe_json:
            print(json.dumps(describe_plan(plan), sort_keys=True))
            return 0
        print(
            f"[holostream] validated config={plan.config_path} "
            f"sha256={plan.config_sha256} replicas={plan.replicas} "
            f"auctions={plan.auction.num_events_per_replica * plan.replicas} "
            f"bids={plan.bid.num_events_per_replica * plan.replicas}",
            flush=True,
        )
        if args.validate_only:
            return 0

        args.output_dir.mkdir(parents=True, exist_ok=True)
        state.producer_sha256_by_host = verify_producer_binaries(
            plan,
            args.producer_binary,
            args.expected_producer_sha256,
        )
        print(
            "[holostream] verified producer binary sha256="
            f"{next(iter(state.producer_sha256_by_host.values()))}",
            flush=True,
        )
        require_empty_topics(
            args.kafka_node,
            args.kafka_home,
            args.bootstrap_servers,
            args.auction_topic,
            args.bid_topic,
            plan.replicas,
        )
        remote_dir, remote_config = stage_config(plan, args.run_id)
        state.production_started_ms = int(time.time() * 1000)
        launch_replicas(
            plan,
            remote_dir,
            remote_config,
            args.producer_binary,
            args.output_dir,
            state,
        )
        monitor_completion(
            plan,
            state,
            kafka_node=args.kafka_node,
            kafka_home=args.kafka_home,
            bootstrap_servers=args.bootstrap_servers,
            auction_topic=args.auction_topic,
            bid_topic=args.bid_topic,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
            max_offset_errors=args.max_offset_errors,
            phase_path=args.output_dir / "producer_phases.csv",
        )
        status = "success"
        exit_code = 0
    except LauncherInterrupted as error:
        error_message = str(error)
        exit_code = 128 + error.signum
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
    except LauncherError as error:
        error_message = str(error)
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
    except Exception as error:  # Keep unexpected failures visible in the manifest.
        error_message = f"unexpected launcher failure: {error}"
        print(f"ERROR: {error_message}", file=sys.stderr, flush=True)
    finally:
        if state.replicas:
            cleanup_replicas(
                state,
                terminate_timeout_seconds=args.terminate_timeout_seconds,
            )
        if state.cleanup_errors:
            if status == "success":
                status = "failed"
                exit_code = 1
                error_message = "; ".join(state.cleanup_errors)
            else:
                cleanup_summary = "; ".join(state.cleanup_errors)
                error_message = (
                    f"{error_message}; cleanup errors: {cleanup_summary}"
                    if error_message
                    else f"cleanup errors: {cleanup_summary}"
                )
        if not args.validate_only and not args.describe_json:
            write_artifacts(
                args.output_dir,
                args.run_id,
                plan,
                state,
                started_ms=started_ms,
                ended_ms=int(time.time() * 1000),
                status=status,
                error=error_message,
            )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
