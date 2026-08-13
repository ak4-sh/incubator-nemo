#!/usr/bin/env python3
"""Validate the node-sized Sponge baseline before a CloudLab experiment."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


HOST_RE = re.compile(r"^node[0-9]+(?:-link-[0-9]+)?$")
YARN_NODE_RE = re.compile(r"^\s*(node[0-9]+(?:-link-[0-9]+)?):[0-9]+\s+RUNNING\b")
APPLICATION_RE = re.compile(r"^\s*(application_[0-9]+_[0-9]+)\s+", re.MULTILINE)


def short_host(host: str) -> str:
    return host.split(".", 1)[0].split("-link-", 1)[0]


def parse_hosts(value: str) -> list[str]:
    hosts: list[str] = []
    seen: set[str] = set()
    for raw in re.split(r"[\s,]+", value.strip()):
        if not raw:
            continue
        if HOST_RE.fullmatch(raw) is None:
            raise ValueError(f"invalid CloudLab host name: {raw!r}")
        normalized = short_host(raw)
        if normalized in seen:
            raise ValueError(f"duplicate CloudLab host: {raw!r}")
        hosts.append(raw)
        seen.add(normalized)
    return hosts


def load_executor_config(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("executor configuration must be a JSON list of objects")
    return value


def validate_executor_config(
    resources: list[dict[str, Any]],
    expected_memory_mb: int,
    expected_capacity: int,
    expected_source_slots: int = 8,
    expected_compute_slots: int = 5,
) -> list[str]:
    failures: list[str] = []
    by_type: dict[str, dict[str, Any]] = {}
    for resource in resources:
        resource_type = resource.get("type")
        if not isinstance(resource_type, str):
            failures.append("executor resource is missing a string type")
            continue
        if resource_type in by_type:
            failures.append(f"duplicate executor resource type {resource_type}")
        by_type[resource_type] = resource

    expected = {
        "Source": {
            "memory_mb": expected_memory_mb,
            "capacity": expected_capacity,
            "slot": expected_source_slots,
            "num": 1,
        },
        "Compute": {
            "memory_mb": expected_memory_mb,
            "capacity": expected_capacity,
            "slot": expected_compute_slots,
            "num": 4,
        },
    }
    for resource_type, fields in expected.items():
        resource = by_type.get(resource_type)
        if resource is None:
            failures.append(f"missing {resource_type} resource")
            continue
        for field, expected_value in fields.items():
            if resource.get(field) != expected_value:
                failures.append(
                    f"{resource_type}.{field}={resource.get(field)!r}; "
                    f"expected {expected_value}"
                )

    for resource_type in ("Transient", "Reserved"):
        resource = by_type.get(resource_type)
        if resource is None:
            failures.append(f"missing {resource_type} scale-out resource template")
            continue
        if resource.get("capacity") != 1 or resource.get("slot") != 1:
            failures.append(f"{resource_type} must remain single-core and single-slot")
        if resource.get("num") != 0:
            failures.append(f"{resource_type}.num must be zero at baseline")

    return failures


def run(command: list[str], timeout: int = 30) -> str:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"{completed.stdout.strip()}"
        )
    return completed.stdout


def parse_yarn_site(path: Path) -> dict[str, str]:
    root = ET.parse(path).getroot()
    values: dict[str, str] = {}
    for prop in root.findall("property"):
        name = prop.findtext("name")
        value = prop.findtext("value")
        if name is not None and value is not None:
            values[name.strip()] = value.strip()
    return values


def probe_host(host: str) -> dict[str, Any]:
    remote = r"""
printf 'cpus=%s\n' "$(getconf _NPROCESSORS_ONLN)"
awk '/^MemTotal:/ { printf "memory_kb=%s\n", $2 }' /proc/meminfo
printf 'nm_count=%s\n' "$(pgrep -fc '[p]roc_nodemanager' || true)"
printf 'vmworker_count=%s\n' "$(pgrep -fc '[o]rg.apache.nemo.offloading.workers.vm.VMWorker' || true)"
printf 'reef_count=%s\n' "$(pgrep -fc '[o]rg.apache.reef.runtime.common.REEFLauncher' || true)"
pid=$(pgrep -f '[p]roc_nodemanager' | head -n 1 || true)
if [ -n "$pid" ]; then
  printf 'nm_java=%s\n' "$(readlink -f /proc/$pid/exe)"
else
  printf 'nm_java=\n'
fi
""".strip()
    output = run(["ssh", "-o", "BatchMode=yes", host, remote])
    result: dict[str, Any] = {"host": host, "raw": output}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            continue
        if key in {"cpus", "memory_kb", "nm_count", "vmworker_count", "reef_count"}:
            result[key] = int(value)
        else:
            result[key] = value
    return result


def validate_live_cluster(
    baseline_hosts: list[str],
    offload_hosts: list[str],
    yarn_site: Path,
    expected_node_vcores: int,
    expected_node_memory_mb: int,
    executor_capacity: int,
    executor_memory_mb: int,
) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    details: dict[str, Any] = {}

    yarn_values = parse_yarn_site(yarn_site)
    details["yarnSite"] = yarn_values
    numeric_properties = {
        "yarn.nodemanager.resource.cpu-vcores": expected_node_vcores,
        "yarn.scheduler.maximum-allocation-vcores": expected_node_vcores,
        "yarn.nodemanager.resource.memory-mb": expected_node_memory_mb,
        "yarn.scheduler.maximum-allocation-mb": expected_node_memory_mb,
    }
    for name, minimum in numeric_properties.items():
        try:
            actual = int(yarn_values.get(name, ""))
        except ValueError:
            actual = -1
        if actual < minimum:
            failures.append(f"{name}={actual}; expected at least {minimum}")
    for name in (
        "yarn.nodemanager.vmem-check-enabled",
        "yarn.nodemanager.pmem-check-enabled",
    ):
        if yarn_values.get(name, "").lower() != "false":
            failures.append(f"{name} must be false on this Hadoop 2.7 deployment")

    node_output = run(["yarn", "node", "-list"])
    running_nodes = {
        short_host(match.group(1))
        for line in node_output.splitlines()
        if (match := YARN_NODE_RE.match(line)) is not None
    }
    expected_running = {short_host(host) for host in baseline_hosts}
    details["runningYarnNodes"] = sorted(running_nodes)
    if running_nodes != expected_running:
        failures.append(
            f"RUNNING NodeManagers are {sorted(running_nodes)}; "
            f"expected exactly {sorted(expected_running)}"
        )

    application_output = run(["yarn", "application", "-list"])
    active_applications = APPLICATION_RE.findall(application_output)
    details["activeApplications"] = active_applications
    if active_applications:
        failures.append(f"active YARN applications exist: {active_applications}")

    probes: list[dict[str, Any]] = []
    for host in baseline_hosts + offload_hosts:
        probe = probe_host(short_host(host))
        probes.append(probe)
        if probe.get("reef_count", 0) != 0:
            failures.append(
                f"{host} has {probe.get('reef_count', 0)} stale REEF executor processes"
            )
        if host in baseline_hosts:
            if probe.get("cpus", 0) < expected_node_vcores:
                failures.append(
                    f"{host} exposes {probe.get('cpus', 0)} CPUs; "
                    f"expected at least {expected_node_vcores}"
                )
            memory_mb = probe.get("memory_kb", 0) // 1024
            if memory_mb < expected_node_memory_mb:
                failures.append(
                    f"{host} exposes {memory_mb} MiB RAM; "
                    f"expected at least {expected_node_memory_mb}"
                )
            if probe.get("nm_count") != 1:
                failures.append(
                    f"{host} has {probe.get('nm_count', 0)} NodeManager processes; expected 1"
                )
            if "java-11" not in str(probe.get("nm_java", "")):
                failures.append(
                    f"{host} NodeManager is not using Java 11: {probe.get('nm_java', '')}"
                )
        else:
            if probe.get("nm_count", 0) != 0:
                failures.append(f"offload host {host} is running a NodeManager")
            if probe.get("vmworker_count", 0) != 0:
                failures.append(f"offload host {host} has stale VMWorker processes")
    details["hostProbes"] = probes

    if executor_capacity + 1 > expected_node_vcores:
        failures.append("executor capacity does not leave one vcore of node headroom")
    if executor_memory_mb + 8192 > expected_node_memory_mb:
        failures.append("executor memory does not leave 8192 MiB of node headroom")

    rm_probe = r"""
pid=$(pgrep -f '[p]roc_resourcemanager' | head -n 1 || true)
if [ -n "$pid" ]; then readlink -f /proc/$pid/exe; fi
""".strip()
    rm_java = run(["ssh", "-o", "BatchMode=yes", "node0", rm_probe]).strip()
    details["resourceManagerJava"] = rm_java
    if "java-8" not in rm_java and "jdk8" not in rm_java:
        failures.append(f"ResourceManager is not using Java 8: {rm_java!r}")

    job_launcher_probe = r"""
printf 'joblauncher_count=%s\n' "$(pgrep -fc '[o]rg.apache.nemo.client.JobLauncher' || true)"
""".strip()
    job_launcher_output = run(
        ["ssh", "-o", "BatchMode=yes", "node0", job_launcher_probe]
    ).strip()
    job_launcher_count = int(job_launcher_output.partition("=")[2] or "0")
    details["jobLauncherCount"] = job_launcher_count
    if job_launcher_count != 0:
        failures.append(
            f"node0 has {job_launcher_count} stale Nemo JobLauncher processes"
        )

    return failures, details


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executor-config", type=Path, required=True)
    parser.add_argument("--source-host", default="node5-link-1")
    parser.add_argument(
        "--compute-hosts",
        default="node9-link-1,node10-link-1,node11-link-1,node12-link-1",
    )
    parser.add_argument("--offload-hosts", default="node4,node6,node7,node8,node13")
    parser.add_argument("--executor-memory-mb", type=int, default=114688)
    parser.add_argument("--executor-capacity", type=int, default=55)
    parser.add_argument("--source-slots", type=int, default=8)
    parser.add_argument("--compute-slots", type=int, default=5)
    parser.add_argument("--node-memory-mb", type=int, default=122880)
    parser.add_argument("--node-vcores", type=int, default=56)
    parser.add_argument(
        "--yarn-site",
        type=Path,
        default=Path(os.environ.get("HADOOP_CONF_DIR", "/users/akash01/hadoop/etc/hadoop"))
        / "yarn-site.xml",
    )
    parser.add_argument("--static-only", action="store_true")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()

    failures: list[str] = []
    details: dict[str, Any] = {}
    try:
        source_hosts = parse_hosts(args.source_host)
        compute_hosts = parse_hosts(args.compute_hosts)
        offload_hosts = parse_hosts(args.offload_hosts)
        baseline_hosts = source_hosts + compute_hosts
        if len(source_hosts) != 1:
            failures.append(f"expected one source host, found {len(source_hosts)}")
        if len(compute_hosts) != 4:
            failures.append(f"expected four compute hosts, found {len(compute_hosts)}")
        baseline_short = {short_host(host) for host in baseline_hosts}
        offload_short = {short_host(host) for host in offload_hosts}
        if len(baseline_short) != 5:
            failures.append("source and compute hosts must identify five unique nodes")
        overlap = baseline_short & offload_short
        if overlap:
            failures.append(f"baseline and offload hosts overlap: {sorted(overlap)}")

        resources = load_executor_config(args.executor_config)
        failures.extend(
            validate_executor_config(
                resources,
                args.executor_memory_mb,
                args.executor_capacity,
                args.source_slots,
                args.compute_slots,
            )
        )
        details.update(
            {
                "executorConfig": str(args.executor_config.resolve()),
                "sourceHosts": source_hosts,
                "computeHosts": compute_hosts,
                "offloadHosts": offload_hosts,
                "resources": resources,
            }
        )

        if not args.static_only and not failures:
            live_failures, live_details = validate_live_cluster(
                baseline_hosts,
                offload_hosts,
                args.yarn_site,
                args.node_vcores,
                args.node_memory_mb,
                args.executor_capacity,
                args.executor_memory_mb,
            )
            failures.extend(live_failures)
            details.update(live_details)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        failures.append(str(error))

    result = {"passed": not failures, "failures": failures, **details}
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
