#!/usr/bin/env python3
"""Validate the matched Figure 9b-style Q6 sine experiment configurations."""

from __future__ import annotations

import copy
import json
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PRODUCER_CONFIG = (
    SCRIPT_DIR
    / "holostream/config/sponge_hardware_matched_q6_sine_60k450k.json"
)
MEMORY_CONFIG = (
    SCRIPT_DIR
    / "holostream/configs/standard_q6_memory_sine_60k450k_4to8to4.json"
)
PEBBLE_CONFIG = (
    SCRIPT_DIR
    / "holostream/configs/standard_q6_pebble_sine_60k450k_4to8to4.json"
)

EXPECTED_RATES = [
    60_000,
    90_000,
    140_000,
    200_000,
    260_000,
    330_000,
    400_000,
    450_000,
    400_000,
    330_000,
    260_000,
    200_000,
    140_000,
    90_000,
    60_000,
]
EXPECTED_DURATIONS = [120] + [45] * 6 + [150] + [45] * 6 + [120]
EXPECTED_TOTAL_EVENTS = 209_700_000
EXPECTED_RECONFIGURATIONS = [
    {
        "TriggerTimeSeconds": 300,
        "Type": "scaleup",
        "TargetOperator": "statefulMapper",
        "TargetParrallelism": 8,
    },
    {
        "TriggerTimeSeconds": 630,
        "Type": "scaledown",
        "TargetOperator": "statefulMapper",
        "TargetParrallelism": 4,
    },
]


def load(path: Path) -> dict:
    with path.open(encoding="utf-8") as config_file:
        return json.load(config_file)


def sources_by_type(config: dict) -> dict[str, dict]:
    return {
        source["EventType"]: source["NexmarkSourceConfig"]
        for source in config["NexmarkLogicalSourceConfigs"]
    }


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_workload(label: str, config: dict) -> None:
    replicas = len(config["ProducerIPs"])
    sources = sources_by_type(config)
    require(set(sources) == {"Auction", "Bid"}, f"{label}: wrong event types")

    auction = sources["Auction"]
    bid = sources["Bid"]
    auction_limiter = auction["RateLimiterConfig"]
    bid_limiter = bid["RateLimiterConfig"]
    durations = auction_limiter["RateChangeInterval"]

    require(replicas == 4, f"{label}: expected four producer replicas")
    require(durations == EXPECTED_DURATIONS, f"{label}: wrong phase durations")
    require(
        bid_limiter["RateChangeInterval"] == durations,
        f"{label}: Auction and Bid phase durations differ",
    )
    aggregate_rates = [
        replicas * (auction_rate + bid_rate)
        for auction_rate, bid_rate in zip(
            auction_limiter["RateLimit"], bid_limiter["RateLimit"]
        )
    ]
    require(aggregate_rates == EXPECTED_RATES, f"{label}: wrong aggregate rates")

    auction_events = sum(
        rate * duration
        for rate, duration in zip(auction_limiter["RateLimit"], durations)
    )
    bid_events = sum(
        rate * duration
        for rate, duration in zip(bid_limiter["RateLimit"], durations)
    )
    require(auction_events == auction["NumEvents"], f"{label}: wrong Auction total")
    require(bid_events == bid["NumEvents"], f"{label}: wrong Bid total")
    require(
        replicas * (auction_events + bid_events) == EXPECTED_TOTAL_EVENTS,
        f"{label}: wrong overall event total",
    )


def main() -> None:
    producer = load(PRODUCER_CONFIG)
    memory = load(MEMORY_CONFIG)
    pebble = load(PEBBLE_CONFIG)

    for label, config in (
        ("Sponge producer", producer),
        ("HoloStream memory", memory),
        ("HoloStream Pebble", pebble),
    ):
        validate_workload(label, config)

    producer_sources = producer["NexmarkLogicalSourceConfigs"]
    require(
        memory["NexmarkLogicalSourceConfigs"] == producer_sources,
        "memory input differs from Sponge input",
    )
    require(
        pebble["NexmarkLogicalSourceConfigs"] == producer_sources,
        "Pebble input differs from Sponge input",
    )

    require(memory["StateBackendType"] == "memory", "wrong memory backend")
    require(pebble["StateBackendType"] == "pebble", "wrong Pebble backend")
    memory_normalized = copy.deepcopy(memory)
    pebble_normalized = copy.deepcopy(pebble)
    memory_normalized["StateBackendType"] = "BACKEND"
    pebble_normalized["StateBackendType"] = "BACKEND"
    require(
        memory_normalized == pebble_normalized,
        "memory and Pebble configurations differ beyond StateBackendType",
    )

    for label, config in (("memory", memory), ("Pebble", pebble)):
        require(
            config["Reconfigurations"] == EXPECTED_RECONFIGURATIONS,
            f"{label}: wrong reconfiguration schedule",
        )
        require(config["ClosedAuctionParallelism"] == 4, f"{label}: wrong join parallelism")
        require(config["StatefulMapperParallelism"] == 4, f"{label}: wrong baseline target parallelism")
        require(config["SinkParallelism"] == 1, f"{label}: wrong sink parallelism")
        require(config["ReconfigProtocol"] == "lazy", f"{label}: wrong protocol")
        require(config["LazyProtocolVersion"] == "no-migration", f"{label}: wrong lazy variant")
        require(config["TotalRuntimeSeconds"] == 1050, f"{label}: wrong runtime")

    print("Validated matched Q6 sine configurations")
    print(f"  phases: {len(EXPECTED_RATES)}")
    print(f"  workload duration: {sum(EXPECTED_DURATIONS)} s")
    print(f"  total events: {EXPECTED_TOTAL_EVENTS}")
    print("  HoloStream reconfiguration: 4 -> 8 at 300 s; 8 -> 4 at 630 s")


if __name__ == "__main__":
    main()
