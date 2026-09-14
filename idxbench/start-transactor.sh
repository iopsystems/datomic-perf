#!/bin/bash
# Start the Datomic transactor with JMX exposed so a benchmark harness can read
# its GC / JIT / CPU counters. Datomic indexes in THIS process, not in the peer
# that calls d/transact, so these are the only meaningful JVM metrics for an
# indexing benchmark.
set -eu
DTM=/home/xyang/dtm/datomic-pro-1.0.7705
CFG=${CFG:-$DTM/config/bench-transactor.properties}
XMX=${XMX:-4g}
JMX_PORT=${JMX_PORT:-7091}
GC_OPTS=${GC_OPTS:--XX:+UseG1GC -XX:MaxGCPauseMillis=50}
JAVA_HOME_OVERRIDE=${JAVA_HOME_OVERRIDE:-}

if [ -n "$JAVA_HOME_OVERRIDE" ]; then
  export JAVA_HOME="$JAVA_HOME_OVERRIDE"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

cd "$DTM"
exec bin/transactor \
  -Xmx$XMX -Xms$XMX \
  $GC_OPTS \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=$JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
  -Dcom.sun.management.jmxremote.local.only=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Djava.rmi.server.hostname=127.0.0.1 \
  "$CFG"
