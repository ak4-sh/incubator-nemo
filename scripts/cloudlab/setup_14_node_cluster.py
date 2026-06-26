#!/usr/bin/env python3
"""Prepare the 14-node CloudLab layout used by Nemo Kafka/offloading runs.

Default topology:
  node0: submit/control node
  node1,node2,node3: Kafka brokers
  node4,node6,node7,node8,node13: offload VMWorker pool nodes
  node5,node9,node10,node11,node12: YARN/Nemo worker nodes

The script intentionally focuses on setup. Use run_autoscaler_smoke.sh or
run_sponge_q0_benchmark.sh to launch workloads after this completes.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

DEFAULT_CONTROL_NODE = "node0"
DEFAULT_KAFKA_NODES = "node1,node2,node3"
DEFAULT_OFFLOAD_NODES = "node4,node6,node7,node8,node13"
DEFAULT_WORKER_NODES = "node5,node9,node10,node11,node12"

DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-11-openjdk-amd64"
DEFAULT_HADOOP_HOME = "/users/akash01/hadoop"
DEFAULT_KAFKA_HOME = "/users/akash01/kafka"
DEFAULT_BEAM_GRPC_JAR = "/users/akash01/deps/beam-vendor-grpc-1_21_0-0.1.jar"


def parse_nodes(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        raw = value.replace(",", " ").split()
    else:
        raw = []
        for item in value:
            raw.extend(item.replace(",", " ").split())
    return [node.strip() for node in raw if node.strip()]


def shell_join(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


class Runner:
    def __init__(self, dry_run: bool) -> None:
        self.dry_run = dry_run

    def run(self, cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        print(f"$ {shell_join(cmd)}")
        if self.dry_run:
            return subprocess.CompletedProcess(cmd, 0, "", "")
        return subprocess.run(cmd, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def run_streaming(self, cmd: list[str], *, check: bool = True, env: dict[str, str] | None = None) -> int:
        if env:
            env_prefix = " ".join(f"{key}={shlex.quote(value)}" for key, value in sorted(env.items()))
            print(f"$ {env_prefix} {shell_join(cmd)}")
        else:
            print(f"$ {shell_join(cmd)}")
        if self.dry_run:
            return 0
        run_env = os.environ.copy()
        if env:
            run_env.update(env)
        proc = subprocess.run(cmd, check=False, env=run_env)
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        return proc.returncode

    def ssh(self, node: str, remote_cmd: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.run(["ssh", "-A", node, remote_cmd], check=check)

    def scp(self, src: str, dst: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.run(["scp", src, dst], check=check)


def require_local_file(path: str, label: str) -> None:
    if not Path(path).is_file():
        raise FileNotFoundError(f"{label} not found: {path}")


def require_local_dir(path: str, label: str) -> None:
    if not Path(path).is_dir():
        raise FileNotFoundError(f"{label} not found: {path}")


def validate_topology(control: str, kafka: list[str], offload: list[str], workers: list[str]) -> list[str]:
    all_nodes = [control] + kafka + offload + workers
    duplicates = sorted({node for node in all_nodes if all_nodes.count(node) > 1})
    if duplicates:
        raise ValueError(f"Nodes cannot appear in multiple roles: {duplicates}")
    if len(all_nodes) != 14:
        raise ValueError(f"Expected 14 total nodes, got {len(all_nodes)}: {all_nodes}")
    if len(kafka) != 3:
        raise ValueError(f"Expected 3 Kafka nodes, got {len(kafka)}: {kafka}")
    if len(offload) != 5:
        raise ValueError(f"Expected 5 offload nodes, got {len(offload)}: {offload}")
    if len(workers) != 5:
        raise ValueError(f"Expected 5 worker nodes, got {len(workers)}: {workers}")
    return all_nodes


def check_ssh(runner: Runner, nodes: list[str]) -> None:
    print("\n== Checking SSH access ==")
    for node in nodes:
        result = runner.ssh(node, "hostname -s", check=False)
        if result.returncode != 0:
            raise RuntimeError(f"SSH check failed for {node}: {result.stderr.strip()}")
        if not runner.dry_run:
            print(f"  {node}: {result.stdout.strip()}")


def clean_metrics(runner: Runner, nodes: list[str]) -> None:
    print("\n== Cleaning stale metrics/logs ==")
    cmd = "rm -f /tmp/source_task_metrics.csv /tmp/task_metrics.csv /tmp/scaler_metrics.csv " \
          "/tmp/scaling_decisions.csv /tmp/source_aggregate_metrics.csv /tmp/source_metrics.csv " \
          "/tmp/vmworker-*.log /tmp/start-warm-pool-*.log"
    for node in nodes:
        runner.ssh(node, cmd, check=False)


def stop_nonworker_nodemanager(runner: Runner, nodes: list[str], hadoop_home: str) -> None:
    print("\n== Stopping NodeManagers on non-worker nodes ==")
    cmd = f"{shlex.quote(hadoop_home)}/sbin/yarn-daemon.sh stop nodemanager >/dev/null 2>&1 || true; " \
          "pkill -f '[o]rg.apache.hadoop.yarn.server.nodemanager.NodeManager' || true"
    for node in nodes:
        runner.ssh(node, cmd, check=False)


def ensure_worker_nodemanagers(runner: Runner, workers: list[str], hadoop_home: str, expected_count: int) -> None:
    print("\n== Ensuring worker NodeManagers are running ==")
    for node in workers:
        runner.ssh(node, f"{shlex.quote(hadoop_home)}/sbin/yarn-daemon.sh start nodemanager >/dev/null 2>&1 || true", check=False)

    result = runner.run([f"{hadoop_home}/bin/yarn", "node", "-list"], check=False)
    if runner.dry_run:
        return
    running = sum(1 for line in result.stdout.splitlines() if "RUNNING" in line)
    print(f"  YARN RUNNING NodeManagers observed: {running}/{expected_count}")
    if running < expected_count:
        raise RuntimeError(f"Only {running}/{expected_count} NodeManagers are RUNNING")


def verify_kafka(runner: Runner, kafka_node: str, kafka_home: str, bootstrap: str) -> None:
    print("\n== Verifying Kafka bootstrap ==")
    cmd = f"timeout 10 {shlex.quote(kafka_home)}/bin/kafka-broker-api-versions.sh --bootstrap-server {shlex.quote(bootstrap)} >/dev/null"
    result = runner.ssh(kafka_node, cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Kafka bootstrap check failed via {kafka_node}: {result.stderr.strip()}")
    print(f"  Kafka bootstrap OK: {bootstrap}")


def restart_kafka(runner: Runner, kafka_nodes: list[str], kafka_home: str) -> None:
    print("\n== Restarting Kafka brokers ==")
    for node in kafka_nodes:
        runner.ssh(node, f"{shlex.quote(kafka_home)}/bin/kafka-server-stop.sh >/dev/null 2>&1 || true", check=False)
    for node in kafka_nodes:
        runner.ssh(
            node,
            f"nohup {shlex.quote(kafka_home)}/bin/kafka-server-start.sh -daemon "
            f"{shlex.quote(kafka_home)}/config/server.properties >/tmp/kafka-start.log 2>&1",
            check=False,
        )


def write_vm_addresses(output: Path, nodes: list[str], first_port: int, workers_per_node: int, dry_run: bool) -> None:
    lines = []
    for node in nodes:
        for idx in range(workers_per_node):
            lines.append(f"{node}:{first_port + idx}")
    if dry_run:
        print(f"  Would write {len(lines)} VMWorker addresses to {output}")
        return
    output.write_text("\n".join(lines) + "\n")
    print(f"  Wrote {len(lines)} VMWorker addresses to {output}")


def start_vm_workers(
    runner: Runner,
    offload_nodes: list[str],
    first_port: int,
    workers_per_node: int,
    java_home: str,
    vm_worker_jar: str,
    rebuilt_nemo: str,
    rebuilt_nexmark: str,
    beam_grpc_jar: str,
    clean_existing: bool,
) -> None:
    print("\n== Starting offload VMWorker pools ==")
    for node in offload_nodes:
        print(f"  {node}: {workers_per_node} workers")
        if clean_existing:
            runner.ssh(node, "pkill -f '[o]rg.apache.nemo.offloading.workers.vm.VMWorker' || true", check=False)
        runner.ssh(node, "mkdir -p /tmp/nemo-cloudlab-offload", check=False)
        runner.scp(str(SCRIPT_DIR / "start_warm_pool.sh"), f"{node}:/tmp/nemo-cloudlab-offload/start_warm_pool.sh")
        runner.scp(vm_worker_jar, f"{node}:/tmp/nemo-cloudlab-offload/offloading-vm.jar")
        runner.scp(rebuilt_nemo, f"{node}:/tmp/nemo-cloudlab-offload/nemo-client.jar")
        runner.scp(rebuilt_nexmark, f"{node}:/tmp/nemo-cloudlab-offload/nexmark.jar")
        runner.scp(beam_grpc_jar, f"{node}:/tmp/nemo-cloudlab-offload/beam-grpc.jar")
        extra_cp = ":".join([
            "/tmp/nemo-cloudlab-offload/nemo-client.jar",
            "/tmp/nemo-cloudlab-offload/nexmark.jar",
            "/tmp/nemo-cloudlab-offload/beam-grpc.jar",
        ])
        remote_cmd = (
            f"JAVA_HOME={shlex.quote(java_home)} bash /tmp/nemo-cloudlab-offload/start_warm_pool.sh "
            f"{first_port} {workers_per_node} /tmp/nemo-cloudlab-offload/offloading-vm.jar "
            f"10000000 {shlex.quote(extra_cp)} >/tmp/start-warm-pool-{node}.log 2>&1"
        )
        runner.ssh(node, remote_cmd)


def create_topic(runner: Runner, kafka_node: str, kafka_home: str, zookeeper: str, topic: str, partitions: int) -> None:
    print(f"\n== Creating Kafka topic {topic} ==")
    runner.ssh(
        kafka_node,
        f"{shlex.quote(kafka_home)}/bin/kafka-topics.sh --zookeeper {shlex.quote(zookeeper)} "
        f"--create --topic {shlex.quote(topic)} --partitions {partitions} --replication-factor 1 || true",
        check=False,
    )
    runner.ssh(
        kafka_node,
        f"{shlex.quote(kafka_home)}/bin/kafka-configs.sh --zookeeper {shlex.quote(zookeeper)} "
        f"--entity-type topics --entity-name {shlex.quote(topic)} --alter --add-config min.insync.replicas=1 || true",
        check=False,
    )


def run_nexmark_benchmark(
    runner: Runner,
    args: argparse.Namespace,
    offload_nodes: list[str],
    worker_nodes: list[str],
) -> None:
    print("\n== Running Nexmark benchmark ==")
    run_id = args.run_id or f"nexmark-q{args.query}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    topic = args.topic or run_id
    results_topic = args.results_topic or f"{topic}-results"
    completion_mode = args.completion_mode
    if completion_mode == "auto":
        completion_mode = "exact_output" if args.query == 0 else "source_plus_some_output"
    benchmark_name = args.benchmark_name or f"nexmark-q{args.query}"
    benchmark_script = Path(args.benchmark_script)
    if not benchmark_script.is_absolute():
        benchmark_script = REPO_ROOT / benchmark_script
    require_local_file(str(benchmark_script), "benchmark script")

    env = {
        "RUN_ID": run_id,
        "QUERY": str(args.query),
        "BENCHMARK_NAME": benchmark_name,
        "COMPLETION_MODE": completion_mode,
        "TOPIC": topic,
        "KAFKA_RESULTS_TOPIC": results_topic,
        "KAFKA_CONSUMER_GROUP": args.kafka_consumer_group or f"{run_id}-consumer",
        "SINK_TYPE": args.sink_type,
        "EXECUTOR_JSON": args.executor_json,
        "OFFLOAD_NODES": ",".join(offload_nodes),
        "WORKERS_PER_NODE": str(args.workers_per_offload_node),
        "FIRST_PORT": str(args.first_port),
        "BASELINE_NODES": " ".join(worker_nodes),
        "EXPECTED_NM_COUNT": str(len(worker_nodes)),
        "KAFKA_BOOTSTRAP": args.kafka_bootstrap,
        "KAFKA_ZOOKEEPER": args.kafka_zookeeper,
        "KAFKA_PARTITIONS": str(args.topic_partitions),
    }
    if args.total_events is not None:
        env["TOTAL_EVENTS"] = str(args.total_events)
        env["NUM_EVENTS"] = str(args.total_events)
    if args.prefill_events is not None:
        env["PREFILL_EVENTS"] = str(args.prefill_events)
    if args.cpu_delay_ms is not None:
        env["CPU_DELAY_MS"] = str(args.cpu_delay_ms)
    if args.stream_timeout is not None:
        env["STREAM_TIMEOUT"] = str(args.stream_timeout)
    if args.benchmark_timeout_sec is not None:
        env["BENCHMARK_TIMEOUT_SEC"] = str(args.benchmark_timeout_sec)

    print(f"  query:         {args.query}")
    print(f"  completion:    {completion_mode}")
    print(f"  run id:        {run_id}")
    print(f"  topic:         {topic}")
    print(f"  results topic: {results_topic}")
    print(f"  group:         {env['KAFKA_CONSUMER_GROUP']}")
    runner.run_streaming(["bash", str(benchmark_script)], env=env)


def main() -> int:
    parser = argparse.ArgumentParser(description="Setup Nemo's 14-node Kafka/offloading CloudLab cluster")
    parser.add_argument("--control-node", default=DEFAULT_CONTROL_NODE)
    parser.add_argument("--kafka-nodes", default=DEFAULT_KAFKA_NODES)
    parser.add_argument("--offload-nodes", default=DEFAULT_OFFLOAD_NODES)
    parser.add_argument("--worker-nodes", default=DEFAULT_WORKER_NODES)
    parser.add_argument("--workers-per-offload-node", type=int, default=32)
    parser.add_argument("--first-port", type=int, default=25321)
    parser.add_argument("--java-home", default=os.environ.get("JAVA_HOME", DEFAULT_JAVA_HOME))
    parser.add_argument("--hadoop-home", default=os.environ.get("HADOOP_HOME", DEFAULT_HADOOP_HOME))
    parser.add_argument("--kafka-home", default=os.environ.get("KAFKA_HOME", DEFAULT_KAFKA_HOME))
    parser.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_KAFKA_NODES.replace(",", ":9092,") + ":9092"))
    parser.add_argument("--kafka-zookeeper", default=os.environ.get("KAFKA_ZOOKEEPER", DEFAULT_KAFKA_NODES.replace(",", ":2181,") + ":2181"))
    parser.add_argument("--repo-root", default=os.environ.get("NEMO_REPO_ROOT", str(REPO_ROOT)))
    parser.add_argument("--beam-grpc-jar", default=os.environ.get("BEAM_GRPC_JAR", DEFAULT_BEAM_GRPC_JAR))
    parser.add_argument("--nemo-jar", default=os.environ.get("REBUILT_NEMO", str(REPO_ROOT / "client/target/nemo-client-0.2-SNAPSHOT-shaded.jar")))
    parser.add_argument("--nexmark-jar", default=os.environ.get("REBUILT_NEXMARK", str(REPO_ROOT / "examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar")))
    parser.add_argument("--vm-worker-jar", default=os.environ.get("VM_WORKER_JAR", str(REPO_ROOT / "offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar")))
    parser.add_argument("--vm-addresses", default=str(REPO_ROOT / "vm_addresses.txt"))
    parser.add_argument("--create-topic", default="")
    parser.add_argument("--create-results-topic", default="")
    parser.add_argument("--topic-partitions", type=int, default=8)
    parser.add_argument("--run-benchmark", action="store_true", help="Launch Nexmark benchmark after setup")
    parser.add_argument("--query", type=int, default=6, help="Nexmark query to run when --run-benchmark is set")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--benchmark-name", default="")
    parser.add_argument("--completion-mode", default="auto", choices=["auto", "exact_output", "source_only", "source_plus_some_output"])
    parser.add_argument("--topic", default="")
    parser.add_argument("--results-topic", default="")
    parser.add_argument("--kafka-consumer-group", default="")
    parser.add_argument("--sink-type", default="KAFKA")
    parser.add_argument("--executor-json", default="configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json")
    parser.add_argument("--benchmark-script", default="scripts/cloudlab/run_sponge_q0_benchmark.sh")
    parser.add_argument("--total-events", type=int)
    parser.add_argument("--prefill-events", type=int)
    parser.add_argument("--cpu-delay-ms", type=int)
    parser.add_argument("--stream-timeout", type=int)
    parser.add_argument("--benchmark-timeout-sec", type=int)
    parser.add_argument("--restart-yarn", action="store_true", help="Start NodeManagers on worker nodes")
    parser.add_argument("--stop-nonworker-nodemanager", action="store_true")
    parser.add_argument("--restart-kafka", action="store_true")
    parser.add_argument("--skip-kafka-check", action="store_true")
    parser.add_argument("--clean-metrics", action="store_true")
    parser.add_argument("--clean-vmworkers", action="store_true")
    parser.add_argument("--skip-start-vmworkers", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    kafka_nodes = parse_nodes(args.kafka_nodes)
    offload_nodes = parse_nodes(args.offload_nodes)
    worker_nodes = parse_nodes(args.worker_nodes)
    all_nodes = validate_topology(args.control_node, kafka_nodes, offload_nodes, worker_nodes)
    nonworker_nodes = [args.control_node] + kafka_nodes + offload_nodes


    require_local_dir(args.repo_root, "Nemo repo root")
    require_local_file(str(SCRIPT_DIR / "start_warm_pool.sh"), "start_warm_pool.sh")

    needs_artifacts = not args.skip_start_vmworkers or args.run_benchmark
    if not args.dry_run and needs_artifacts:
        require_local_file(args.vm_worker_jar, "VM worker jar")
        require_local_file(args.nemo_jar, "Nemo shaded jar")
        require_local_file(args.nexmark_jar, "Nexmark shaded jar")
        require_local_file(args.beam_grpc_jar, "Beam gRPC jar")

    runner = Runner(args.dry_run)

    print("== Nemo 14-node cluster setup ==")
    print(f"  control: {args.control_node}")
    print(f"  kafka:   {', '.join(kafka_nodes)}")
    print(f"  offload: {', '.join(offload_nodes)}")
    print(f"  workers: {', '.join(worker_nodes)}")

    check_ssh(runner, all_nodes)

    if args.clean_metrics:
        clean_metrics(runner, all_nodes)

    if args.stop_nonworker_nodemanager:
        stop_nonworker_nodemanager(runner, nonworker_nodes, args.hadoop_home)

    if args.restart_yarn:
        ensure_worker_nodemanagers(runner, worker_nodes, args.hadoop_home, len(worker_nodes))

    if args.restart_kafka:
        restart_kafka(runner, kafka_nodes, args.kafka_home)

    if not args.skip_kafka_check:
        verify_kafka(runner, kafka_nodes[0], args.kafka_home, args.kafka_bootstrap)

    write_vm_addresses(Path(args.vm_addresses), offload_nodes, args.first_port, args.workers_per_offload_node, args.dry_run)

    if not args.skip_start_vmworkers:
        start_vm_workers(
            runner,
            offload_nodes,
            args.first_port,
            args.workers_per_offload_node,
            args.java_home,
            args.vm_worker_jar,
            args.nemo_jar,
            args.nexmark_jar,
            args.beam_grpc_jar,
            args.clean_vmworkers,
        )

    if args.create_topic:
        create_topic(runner, kafka_nodes[0], args.kafka_home, args.kafka_zookeeper, args.create_topic, args.topic_partitions)
    if args.create_results_topic:
        create_topic(runner, kafka_nodes[0], args.kafka_home, args.kafka_zookeeper, args.create_results_topic, args.topic_partitions)

    print("\n== Setup complete ==")
    print(f"  Kafka bootstrap: {args.kafka_bootstrap}")
    print(f"  VM addresses:    {args.vm_addresses}")
    print(f"  VMWorkers:       {len(offload_nodes) * args.workers_per_offload_node}")
    print("  Recommended executor JSON: configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json")
    if args.run_benchmark:
        run_nexmark_benchmark(runner, args, offload_nodes, worker_nodes)
    else:
        print("  Next: run with --run-benchmark to launch Nexmark Q6, or run the shell harness directly")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
