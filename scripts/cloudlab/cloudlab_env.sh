#!/usr/bin/env bash
# Source this file from CloudLab scripts. Override variables before sourcing if needed.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export NEMO_REPO_ROOT=${NEMO_REPO_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/users/akash01/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export YARN_CONF_DIR=${YARN_CONF_DIR:-$HADOOP_CONF_DIR}
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-node1:9092,node2:9092,node3:9092}
export KAFKA_NODE=${KAFKA_NODE:-node1}
export KAFKA_HOME=${KAFKA_HOME:-/users/akash01/kafka}

export BEAM_GRPC_JAR=${BEAM_GRPC_JAR:-/users/akash01/deps/beam-vendor-grpc-1_21_0-0.1.jar}
export REBUILT_NEMO=${REBUILT_NEMO:-$NEMO_REPO_ROOT/client/target/nemo-client-0.2-SNAPSHOT-shaded.jar}
export REBUILT_NEXMARK=${REBUILT_NEXMARK:-$NEMO_REPO_ROOT/examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar}
export VM_WORKER_JAR=${VM_WORKER_JAR:-$NEMO_REPO_ROOT/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar}
export STANDALONE_PRODUCER_OUT=${STANDALONE_PRODUCER_OUT:-$NEMO_REPO_ROOT/build/cloudlab-producer}

export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export HADOOP_CLIENT_CP=${HADOOP_CLIENT_CP:-$(JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hadoop" classpath --glob)}

export NEMO_CLIENT_CP="$BEAM_GRPC_JAR:$REBUILT_NEMO:$REBUILT_NEXMARK:$HADOOP_CLIENT_CP"
export STANDALONE_PRODUCER_CP="$STANDALONE_PRODUCER_OUT:$REBUILT_NEXMARK"
