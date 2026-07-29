#!/usr/bin/env python3
"""Safely reset the two fixed Kafka topics used by unchanged HoloStream."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


AUCTION_TOPIC = "nexmark-auction"
BID_TOPIC = "nexmark-bid"
TOPICS = (AUCTION_TOPIC, BID_TOPIC)
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=10")
OFFSET_LINE = re.compile(r"^([^:]+):(\d+):(\d+)\s*$")
PARTITION_REPLICAS = re.compile(
    r"Partition:\s*(\d+).*?Replicas:\s*([0-9]+(?:,[0-9]+)*)"
)


class ResetError(RuntimeError):
    """A failure which prevents a verified empty-topic reset."""


def run_command(command: list[str], label: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ResetError(f"{label} failed: {error}") from error


def ssh(
    host: str,
    remote_args: list[str],
    label: str,
    *,
    timeout: int = 30,
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[str]:
    command = ["ssh", *SSH_OPTIONS, host, shlex.join(remote_args)]
    result = run_command(command, label, timeout)
    if result.returncode != 0 and not allow_failure:
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        raise ResetError(f"{label} exited {result.returncode}: {detail}")
    return result


def kafka_tool(kafka_home: str, name: str) -> str:
    return f"{kafka_home}/bin/{name}"


def list_topics(
    kafka_node: str,
    kafka_home: str,
    zookeeper: str,
) -> set[str]:
    result = ssh(
        kafka_node,
        [
            kafka_tool(kafka_home, "kafka-topics.sh"),
            "--zookeeper",
            zookeeper,
            "--list",
        ],
        "list Kafka topics",
    )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def topic_command_output(
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    zookeeper: str,
    topic: str,
    *,
    describe_config: bool,
) -> dict[str, Any]:
    if describe_config:
        args = [
            kafka_tool(kafka_home, "kafka-configs.sh"),
            "--zookeeper",
            zookeeper,
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--describe",
        ]
        label = f"describe configuration for {topic}"
    else:
        args = [
            kafka_tool(kafka_home, "kafka-topics.sh"),
            "--zookeeper",
            zookeeper,
            "--describe",
            "--topic",
            topic,
        ]
        label = f"describe topic {topic}"
    result = ssh(
        kafka_node,
        args,
        label,
        allow_failure=True,
    )
    return {
        "command": shlex.join(args),
        "exitCode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def partition_offsets(
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    topic: str,
    timestamp: int,
) -> dict[int, int]:
    args = [
        kafka_tool(kafka_home, "kafka-run-class.sh"),
        "kafka.tools.GetOffsetShell",
        "--broker-list",
        bootstrap_servers,
        "--topic",
        topic,
        "--time",
        str(timestamp),
    ]
    result = ssh(
        kafka_node,
        args,
        f"read {'end' if timestamp == -1 else 'beginning'} offsets for {topic}",
    )
    offsets: dict[int, int] = {}
    for line in result.stdout.splitlines():
        match = OFFSET_LINE.fullmatch(line.strip())
        if match and match.group(1) == topic:
            offsets[int(match.group(2))] = int(match.group(3))
    if not offsets:
        raise ResetError(f"offset command returned no partitions for {topic}")
    return offsets


def capture_state(
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    zookeeper: str,
    consumer_group: str,
) -> dict[str, Any]:
    existing = list_topics(kafka_node, kafka_home, zookeeper)
    topics: dict[str, Any] = {}
    for topic in TOPICS:
        topic_state: dict[str, Any] = {"exists": topic in existing}
        if topic in existing:
            topic_state.update(
                {
                    "description": topic_command_output(
                        kafka_node,
                        kafka_home,
                        bootstrap_servers,
                        zookeeper,
                        topic,
                        describe_config=False,
                    ),
                    "configuration": topic_command_output(
                        kafka_node,
                        kafka_home,
                        bootstrap_servers,
                        zookeeper,
                        topic,
                        describe_config=True,
                    ),
                    "beginningOffsets": partition_offsets(
                        kafka_node,
                        kafka_home,
                        bootstrap_servers,
                        topic,
                        -2,
                    ),
                    "endOffsets": partition_offsets(
                        kafka_node,
                        kafka_home,
                        bootstrap_servers,
                        topic,
                        -1,
                    ),
                }
            )
        topics[topic] = topic_state

    group_args = [
        kafka_tool(kafka_home, "kafka-consumer-groups.sh"),
        "--bootstrap-server",
        bootstrap_servers,
        "--describe",
        "--group",
        consumer_group,
    ]
    group_result = ssh(
        kafka_node,
        group_args,
        f"describe consumer group {consumer_group}",
        allow_failure=True,
    )
    return {
        "capturedMs": int(time.time() * 1000),
        "topics": topics,
        "consumerGroup": {
            "name": consumer_group,
            "command": shlex.join(group_args),
            "exitCode": group_result.returncode,
            "stdout": group_result.stdout,
            "stderr": group_result.stderr,
        },
    }


def wait_for_absence(
    kafka_node: str,
    kafka_home: str,
    zookeeper: str,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        existing = list_topics(kafka_node, kafka_home, zookeeper)
        remaining = set(TOPICS) & existing
        if not remaining:
            return
        time.sleep(1)
    raise ResetError(
        f"timed out waiting for topic deletion; still present: {sorted(remaining)}"
    )


def wait_for_partitions(
    kafka_node: str,
    kafka_home: str,
    bootstrap_servers: str,
    expected_partitions: int,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            for topic in TOPICS:
                end_offsets = partition_offsets(
                    kafka_node,
                    kafka_home,
                    bootstrap_servers,
                    topic,
                    -1,
                )
                if set(end_offsets) != set(range(expected_partitions)):
                    raise ResetError(
                        f"{topic} partitions are {sorted(end_offsets)}, expected "
                        f"{list(range(expected_partitions))}"
                    )
            return
        except ResetError as error:
            last_error = str(error)
            time.sleep(1)
    raise ResetError(
        f"timed out waiting for recreated topic partitions: {last_error}"
    )


def reset_topics(args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    before = capture_state(
        args.kafka_node,
        args.kafka_home,
        args.bootstrap_servers,
        args.zookeeper,
        args.consumer_group,
    )
    manifest["before"] = before

    existing = {
        topic for topic, state in before["topics"].items() if state["exists"]
    }
    for topic in TOPICS:
        if topic not in existing:
            continue
        delete_args = [
            kafka_tool(args.kafka_home, "kafka-topics.sh"),
            "--zookeeper",
            args.zookeeper,
            "--delete",
            "--topic",
            topic,
        ]
        ssh(
            args.kafka_node,
            delete_args,
            f"delete fixed HoloStream topic {topic}",
        )
        manifest["operations"].append(
            {
                "timestampMs": int(time.time() * 1000),
                "operation": "delete",
                "topic": topic,
                "command": shlex.join(delete_args),
            }
        )

    wait_for_absence(
        args.kafka_node,
        args.kafka_home,
        args.zookeeper,
        args.timeout_seconds,
    )
    manifest["deletionConfirmedMs"] = int(time.time() * 1000)

    for topic in TOPICS:
        create_args = [
            kafka_tool(args.kafka_home, "kafka-topics.sh"),
            "--zookeeper",
            args.zookeeper,
            "--create",
            "--topic",
            topic,
            "--partitions",
            str(args.partitions),
            "--replication-factor",
            str(args.replication_factor),
            "--config",
            "min.insync.replicas=1",
            "--config",
            "message.timestamp.type=LogAppendTime",
        ]
        ssh(
            args.kafka_node,
            create_args,
            f"create fixed HoloStream topic {topic}",
        )
        manifest["operations"].append(
            {
                "timestampMs": int(time.time() * 1000),
                "operation": "create",
                "topic": topic,
                "command": shlex.join(create_args),
            }
        )

    wait_for_partitions(
        args.kafka_node,
        args.kafka_home,
        args.bootstrap_servers,
        args.partitions,
        args.timeout_seconds,
    )
    after = capture_state(
        args.kafka_node,
        args.kafka_home,
        args.bootstrap_servers,
        args.zookeeper,
        args.consumer_group,
    )
    manifest["after"] = after

    expected_partition_set = set(range(args.partitions))
    for topic in TOPICS:
        state = after["topics"][topic]
        if not state["exists"]:
            raise ResetError(f"{topic} is absent after recreation")
        beginning = {int(key): value for key, value in state["beginningOffsets"].items()}
        end = {int(key): value for key, value in state["endOffsets"].items()}
        if set(beginning) != expected_partition_set or set(end) != expected_partition_set:
            raise ResetError(
                f"{topic} does not expose exactly {args.partitions} partitions"
            )
        if any(offset != 0 for offset in beginning.values()) or any(
            offset != 0 for offset in end.values()
        ):
            raise ResetError(
                f"{topic} is not empty after recreation: "
                f"beginning={beginning}, end={end}"
            )
        config_text = state["configuration"]["stdout"]
        if "message.timestamp.type=LogAppendTime" not in config_text:
            raise ResetError(
                f"{topic} is missing message.timestamp.type=LogAppendTime"
            )
        if "min.insync.replicas=1" not in config_text:
            raise ResetError(f"{topic} is missing min.insync.replicas=1")
        replica_counts = {
            int(match.group(1)): len(match.group(2).split(","))
            for match in PARTITION_REPLICAS.finditer(
                state["description"]["stdout"]
            )
        }
        if set(replica_counts) != expected_partition_set or any(
            count != args.replication_factor for count in replica_counts.values()
        ):
            raise ResetError(
                f"{topic} replication does not match factor "
                f"{args.replication_factor}: {replica_counts}"
            )
    manifest["recreationVerifiedMs"] = int(time.time() * 1000)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-node", required=True)
    parser.add_argument("--kafka-home", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--zookeeper", required=True)
    parser.add_argument("--consumer-group", required=True)
    parser.add_argument("--partitions", required=True, type=int)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    if args.partitions <= 0 or args.replication_factor <= 0:
        print("ERROR: partitions and replication factor must be positive", file=sys.stderr)
        return 2
    if args.timeout_seconds <= 0:
        print("ERROR: timeout must be positive", file=sys.stderr)
        return 2
    if not args.consumer_group.strip():
        print("ERROR: consumer group must not be empty", file=sys.stderr)
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output_dir / "holostream_topic_reset.json"
    manifest: dict[str, Any] = {
        "status": "failed",
        "error": "",
        "startedMs": int(time.time() * 1000),
        "endedMs": 0,
        "topics": list(TOPICS),
        "kafkaNode": args.kafka_node,
        "bootstrapServers": args.bootstrap_servers,
        "zookeeper": args.zookeeper,
        "partitions": args.partitions,
        "replicationFactor": args.replication_factor,
        "consumerGroup": args.consumer_group,
        "operations": [],
    }
    try:
        reset_topics(args, manifest)
        manifest["status"] = "success"
        return 0
    except ResetError as error:
        manifest["error"] = str(error)
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    finally:
        manifest["endedMs"] = int(time.time() * 1000)
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )


if __name__ == "__main__":
    sys.exit(main())
