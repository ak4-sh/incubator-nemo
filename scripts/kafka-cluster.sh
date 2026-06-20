#!/usr/bin/env bash
set -euo pipefail

NODES="node1 node2 node3"
KAFKA_HOME="/opt/kafka"
ZK_CONFIG="$KAFKA_HOME/config/zoo.cfg"
KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"
ZOOKEEPER_DATA_DIR="/data/zookeeper"

usage() {
  cat <<USAGE
Usage: $(basename "$0") {start|stop|status|restart}

Manage the 3-node ZooKeeper + Kafka cluster on nodes 1-3.

Commands:
  start       Start ZooKeeper and Kafka on all nodes
  stop        Stop Kafka and ZooKeeper on all nodes
  status      Show status of ZooKeeper and Kafka on all nodes
  restart     Restart all services
USAGE
}

case "${1:-}" in
  start)
    echo "Starting ZooKeeper on $NODES"
    pdsh -w "$(echo $NODES | tr ' ' ',')" "$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $ZK_CONFIG"
    sleep 5
    echo "Starting Kafka on $NODES"
    pdsh -w "$(echo $NODES | tr ' ' ',')" "$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_CONFIG"
    echo "Cluster started"
    ;;
  stop)
    echo "Stopping Kafka on $NODES"
    pdsh -w "$(echo $NODES | tr ' ' ',')" "$KAFKA_HOME/bin/kafka-server-stop.sh" || true
    sleep 5
    echo "Stopping ZooKeeper on $NODES"
    pdsh -w "$(echo $NODES | tr ' ' ',')" "$KAFKA_HOME/bin/zookeeper-server-stop.sh" || true
    echo "Cluster stopped"
    ;;
  status)
    echo "=== ZooKeeper ==="
    pdsh -w "$(echo $NODES | tr ' ' ',')" "echo ruok | nc -q 2 localhost 2181" 2>/dev/null
    echo ""
    echo "=== Kafka brokers ==="
    ssh "$(echo $NODES | awk '{print $1}')" "$KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server ${NODES// /:9092,}:9092 2>&1 | head -5" || true
    ;;
  restart)
    "$0" stop
    sleep 3
    "$0" start
    ;;
  *)
    usage
    ;;
esac
