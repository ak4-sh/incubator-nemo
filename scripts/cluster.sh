#!/usr/bin/env bash
set -euo pipefail

HADOOP_HOME=${HADOOP_HOME:-/users/akash01/hadoop}
KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
ZK_CONFIG=${ZK_CONFIG:-$KAFKA_HOME/config/zoo.cfg}
KAFKA_CONFIG=${KAFKA_CONFIG:-$KAFKA_HOME/config/server.properties}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

KAFKA_NODES=${KAFKA_NODES:-"node1 node2 node3"}
BASELINE_NODES=${BASELINE_NODES:-"node5 node9 node10 node11 node12"}
OFFLOAD_NODES=${OFFLOAD_NODES:-"node4 node6 node7 node8 node13"}
ALL_NODES="node0 $KAFKA_NODES $BASELINE_NODES $OFFLOAD_NODES"

csv() {
  echo "$1" | tr ' ' ','
}

usage() {
  cat <<USAGE
Usage: $(basename "$0") {start|stop|status|restart}

Manages core cluster services for the current CloudLab topology.

Nodes:
  node0                         ResourceManager, NameNode, job submission
  node1,node2,node3             ZooKeeper + Kafka brokers
  node5,node9,node10,node11,node12
                                Baseline YARN NodeManagers
  node4,node6,node7,node8,node13
                                VM/offload worker pool only; no YARN NMs

Notes:
  - This script does not start VMWorkers. Benchmark wrappers own warm-pool startup.
  - YARN NodeManagers are started only on baseline workers.
USAGE
}

start_zk_kafka() {
  echo "--- ZooKeeper ($KAFKA_NODES) ---"
  pdsh -w "$(csv "$KAFKA_NODES")" "$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $ZK_CONFIG"
  sleep 5
  echo "--- Kafka ($KAFKA_NODES) ---"
  pdsh -w "$(csv "$KAFKA_NODES")" "$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_CONFIG"
}

stop_zk_kafka() {
  echo "--- Kafka ($KAFKA_NODES) ---"
  pdsh -w "$(csv "$KAFKA_NODES")" "$KAFKA_HOME/bin/kafka-server-stop.sh" || true
  sleep 3
  echo "--- ZooKeeper ($KAFKA_NODES) ---"
  pdsh -w "$(csv "$KAFKA_NODES")" "$KAFKA_HOME/bin/zookeeper-server-stop.sh" || true
}

start_yarn_baseline() {
  echo "--- YARN ResourceManager (node0) ---"
  "$HADOOP_HOME/sbin/yarn-daemon.sh" start resourcemanager
  sleep 5
  echo "--- YARN NodeManagers ($BASELINE_NODES) ---"
  for node in $BASELINE_NODES; do
    ssh "$node" "$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager"
  done
}

stop_yarn_baseline() {
  echo "--- YARN NodeManagers ($BASELINE_NODES) ---"
  for node in $BASELINE_NODES; do
    ssh "$node" "$HADOOP_HOME/sbin/yarn-daemon.sh stop nodemanager || true"
  done
  echo "--- YARN ResourceManager (node0) ---"
  "$HADOOP_HOME/sbin/yarn-daemon.sh" stop resourcemanager || true
}

cluster_status() {
  echo "=== Process Summary ==="
  pdsh -w "$(csv "$ALL_NODES")" "ps aux | grep -E '[Z]ooKeeper|[K]afka| [N]ameNode|[D]ataNode|[R]esourceManager|[N]odeManager|[V]MWorker' | awk '{print \$11}' | sort | uniq -c || true" 2>/dev/null || true
}

yarn_status() {
  local running
  echo "=== YARN Nodes ==="
  "$HADOOP_HOME/bin/yarn" node -list -all 2>/dev/null || true
  running=$("$HADOOP_HOME/bin/yarn" node -list 2>/dev/null | grep -c RUNNING || true)
  echo "RUNNING NodeManagers: $running / 5 expected"

  echo ""
  echo "=== Unexpected NodeManagers ==="
  "$HADOOP_HOME/bin/yarn" node -list -all 2>/dev/null \
    | awk 'NR > 2 && $1 ~ /:/ {print $1}' \
    | grep -Ev '^(node5|node9|node10|node11|node12)-link-1:' \
    || echo "none"

  echo ""
  echo "=== Active YARN Applications ==="
  "$HADOOP_HOME/bin/yarn" application -list 2>/dev/null || true
}

kafka_status() {
  echo "=== Kafka Brokers ==="
  "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server node1:9092,node2:9092,node3:9092 2>/dev/null \
    | grep -oP 'node\d+:\d+ \(id: \d+' \
    || echo "Kafka not reachable"

  echo ""
  echo "=== ZooKeeper ==="
  pdsh -w "$(csv "$KAFKA_NODES")" "echo ruok | nc -q 2 localhost 2181" 2>/dev/null || true
}

offload_status() {
  echo "=== Offload VMWorkers ==="
  for node in $OFFLOAD_NODES; do
    ssh "$node" "printf '%s ' \$(hostname -s); ps -eo cmd= | grep -c '[o]rg.apache.nemo.offloading.workers.vm.VMWorker'" || true
  done
}

case "${1:-}" in
  start)
    echo "--- HDFS (node0 + configured Hadoop workers) ---"
    "$HADOOP_HOME/sbin/start-dfs.sh"
    start_yarn_baseline
    start_zk_kafka
    echo "Cluster start initiated"
    sleep 3
    cluster_status
    ;;
  stop)
    stop_zk_kafka
    stop_yarn_baseline
    echo "--- HDFS ---"
    "$HADOOP_HOME/sbin/stop-dfs.sh" || true
    echo "Cluster stopped"
    ;;
  status)
    echo "=== HDFS ==="
    "$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>/dev/null \
      | grep -E 'Live|Dead|Missing|Configured Capacity|DFS Used%' \
      || true
    echo ""
    yarn_status
    echo ""
    kafka_status
    echo ""
    offload_status
    ;;
  restart)
    "$0" stop
    sleep 5
    "$0" start
    ;;
  *)
    usage
    ;;
esac
