#!/usr/bin/env python3
"""Verify CloudLab Nemo executor placement before live production."""

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path


def parse_hosts(value):
    hosts = []
    seen = set()
    for raw_host in (value or "").split(","):
        host = raw_host.strip()
        if host and host not in seen:
            hosts.append(host)
            seen.add(host)
    return hosts


def short_host(host):
    host = (host or "").split(".", 1)[0]
    return host.split("-link-", 1)[0]


def host_matches(expected, actual):
    return expected == actual or short_host(expected) == short_host(actual)


def host_in(expected_hosts, actual):
    return any(host_matches(expected, actual) for expected in expected_hosts)


def load_rows(report_path):
    with open(report_path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def verify(rows, source_hosts, compute_hosts, strict):
    relevant = [row for row in rows if row.get("executorType") in ("Source", "Compute")]
    sources = [row for row in relevant if row.get("executorType") == "Source"]
    computes = [row for row in relevant if row.get("executorType") == "Compute"]
    allowed_hosts = source_hosts + compute_hosts
    failures = []

    if not strict:
      return {
          "passed": True,
          "strict": False,
          "failures": [],
          "source_count": len(sources),
          "compute_count": len(computes),
          "rows": relevant,
      }

    if len(sources) != 1:
        failures.append(f"expected exactly one Source executor, found {len(sources)}")
    for source in sources:
        host = source.get("physicalHost", "")
        if not host_in(source_hosts, host):
            failures.append(f"Source {source.get('executorId')} landed on {host}, expected {source_hosts}")

    if len(computes) != len(compute_hosts):
        failures.append(f"expected {len(compute_hosts)} Compute executors, found {len(computes)}")

    compute_short_hosts = [short_host(row.get("physicalHost", "")) for row in computes]
    for host, count in Counter(compute_short_hosts).items():
        if count > 1:
            failures.append(f"{count} Compute executors landed on {host}")

    for expected_host in compute_hosts:
        if not any(host_matches(expected_host, row.get("physicalHost", "")) for row in computes):
            failures.append(f"missing Compute executor on {expected_host}")

    for compute in computes:
        host = compute.get("physicalHost", "")
        if host_in(source_hosts, host):
            failures.append(f"Compute {compute.get('executorId')} landed on Source host {host}")
        if not host_in(compute_hosts, host):
            failures.append(f"Compute {compute.get('executorId')} landed outside compute hosts: {host}")

    for row in relevant:
        host = row.get("physicalHost", "")
        if not host_in(allowed_hosts, host):
            failures.append(f"{row.get('executorType')} {row.get('executorId')} landed outside allowed hosts: {host}")

    for row in relevant:
        if row.get("placementPassed", "").lower() == "false":
            failures.append(f"{row.get('executorType')} {row.get('executorId')} failed AM placement check")

    return {
        "passed": not failures,
        "strict": True,
        "failures": failures,
        "source_count": len(sources),
        "compute_count": len(computes),
        "source_hosts": source_hosts,
        "compute_hosts": compute_hosts,
        "rows": relevant,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("--source-hosts", default="")
    parser.add_argument("--compute-hosts", default="")
    parser.add_argument("--strict", default="false")
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    strict = args.strict.lower() == "true"
    result = verify(load_rows(args.report), parse_hosts(args.source_hosts), parse_hosts(args.compute_hosts), strict)
    Path(args.output_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not result["passed"]:
        for failure in result["failures"]:
            print(f"ERROR: {failure}", file=sys.stderr)
        return 1
    print(f"placement verification passed: sources={result['source_count']} computes={result['compute_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
