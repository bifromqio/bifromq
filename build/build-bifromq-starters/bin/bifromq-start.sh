#! /bin/bash

#
# Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
#

if [ $# -lt 4 ]; then
  echo "USAGE: $0 -c classname -f filename [-fg]"
  exit 1
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
  -fg)
    FOREGROUND_MODE='true'
    shift 1
    ;;
  -c)
    NAME=$2
    shift 2
    ;;
  -f)
    FILE_NAME=$2
    shift 2
    ;;
  *)
    break
    ;;
  esac
done

BASE_DIR=$(
  cd "$(dirname "$0")"
  pwd
)/..

CONF_DIR="$BASE_DIR/conf"
CONF_FILE="$CONF_DIR/$FILE_NAME"
PLUGIN_DIR="$BASE_DIR/plugins"
LOG_CONF_FILE="$CONF_DIR/logback.xml"
LIB_DIR="$BASE_DIR/lib"
CLASSPATH=$(echo "$LIB_DIR/*")

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$BASE_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Data directory to use
if [ "x$DATA_DIR" = "x" ]; then
  DATA_DIR="$BASE_DIR/data"
fi
mkdir -p "$DATA_DIR"

if [ -z ${BIND_ADDR} ]; then
  BIND_ADDR=$(ifconfig -a | grep inet | grep -v 127.0.0.1 | grep -v inet6 | awk '{print $2}' | tr -d "addr:" | head -1)
fi

pid() {
  echo "$(ps -ef | grep $NAME | grep java | grep -v grep | awk '{print $2}')"
}

total_memory() {
  if [ -n "$MEM_LIMIT" ]; then
    echo $MEM_LIMIT
  elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    local total_kb=$(cat /proc/meminfo | grep 'MemTotal' | awk -F : '{print $2}' | awk '{print $1}' | sed 's/^[ \t]*//g')
    echo "$((total_kb * 1024))"
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "$(sysctl hw.memsize | awk -F : '{print $2}' | xargs)"
  else
    echo -1
  fi
}

memory_in_mb() {
  echo $(($1 / 1024 / 1024))
}

if [ -n "$(pid)" ]; then
  echo "$NAME already started: $(pid)"
  exit 1
fi

# get java version
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

CHECK_JAVA_VERSION_OUTPUT=$("$JAVA" -version 2>&1)
JAVA_VERSION=$(echo $CHECK_JAVA_VERSION_OUTPUT | awk -F\" '/version/{print $2}')
if [ -z "$JAVA_VERSION" ]; then
  echo "Check Java Version failed. $CHECK_JAVA_VERSION_OUTPUT"
  exit 1
fi

JAVA_MAJOR_VERSION=$(echo $JAVA_VERSION | awk -F "." '{print $1}')
if [ -z "$JAVA_MAJOR_VERSION" ]; then
  echo "Parse java major version failed with java version $JAVA_VERSION"
  exit 1
fi

if [[ $(expr $JAVA_MAJOR_VERSION) -lt 17 ]]; then
  echo "Too old Java version $JAVA_VERSION, at least Java 17 is required"
  exit 1
fi

echo "Using Java Version ${JAVA_VERSION} locating at '${JAVA}'"
MEMORY=$(total_memory)
echo -e "Total Memory: $(($MEMORY / 1024 / 1024 / 1024)) GB\n"

# Perf options
if [ -z "$JVM_PERF_OPTS" ]; then
  JVM_PERF_OPTS="-server -XX:MaxInlineLevel=15 -Djava.awt.headless=true"
fi

function evalOpts() {
   EVAL_JVM_GC_OPTS=("$@")
}

# GC options
if [ -z "$JVM_GC_OPTS" ]; then
  JVM_GC_OPTS="-XX:+UnlockExperimentalVMOptions \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+UseZGC \
  -XX:ZAllocationSpikeTolerance=5 \
  -Xlog:async \
  -Xlog:gc:file='${LOG_DIR}/gc-%t.log:time,tid,tags:filecount=5,filesize=50m' \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath='${LOG_DIR}' \
"
fi

eval JVM_GC=("$JVM_GC_OPTS")
# Memory options
if [ -z "$JVM_HEAP_OPTS" ]; then
  MEMORY_FRACTION=70 # Percentage of total memory to use
  HEAP_MEMORY=$(($MEMORY /100 * $MEMORY_FRACTION))
  MIN_HEAP_MEMORY=$(($HEAP_MEMORY / 2))

  # Calculate max direct memory based on total memory
  MAX_DIRECT_MEMORY_FRACTION=20 # Percentage of total memory to use for max direct memory
  MAX_DIRECT_MEMORY=$(($MEMORY /100 * $MAX_DIRECT_MEMORY_FRACTION))

  META_SPACE_MEMORY=128m
  MAX_META_SPACE_MEMORY=500m

  # Set heap options
  JVM_HEAP_OPTS="-Xms$(memory_in_mb ${MIN_HEAP_MEMORY})m -Xmx$(memory_in_mb ${HEAP_MEMORY})m -XX:MetaspaceSize=${META_SPACE_MEMORY} -XX:MaxMetaspaceSize=${MAX_META_SPACE_MEMORY} -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY}"
fi

# Generic jvm settings you want to add
if [ -z "$EXTRA_JVM_OPTS" ]; then
  EXTRA_JVM_OPTS=""
fi

# Set Debug options if enabled
if [ "x$JVM_DEBUG" != "x" ]; then

  # Use default ports
  DEFAULT_JAVA_DEBUG_PORT="8008"

  if [ -z "$JAVA_DEBUG_PORT" ]; then
    JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
  fi

  # Use the defaults if JAVA_DEBUG_OPTS was not set
  DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=*:$JAVA_DEBUG_PORT"
  if [ -z "$JAVA_DEBUG_OPTS" ]; then
    JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
  fi

  echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
  EXTRA_JVM_OPTS="$JAVA_DEBUG_OPTS $EXTRA_JVM_OPTS"
fi


if [ "x$FOREGROUND_MODE" = "xtrue" ]; then
  exec "$JAVA" $JVM_HEAP_OPTS $JVM_PERF_OPTS "${JVM_GC[@]}" $EXTRA_JVM_OPTS \
    -cp "$CLASSPATH" \
    -DLOG_DIR="$LOG_DIR" \
    -DCONF_DIR="$CONF_DIR" \
    -DDATA_DIR="$DATA_DIR" \
    -DBIND_ADDR="$BIND_ADDR" \
    -Dlogback.configurationFile="$LOG_CONF_FILE" \
    -Dpf4j.pluginsDir="$PLUGIN_DIR" \
    $NAME -c "$CONF_FILE"
else
  nohup "$JAVA" $JVM_HEAP_OPTS $JVM_PERF_OPTS "${JVM_GC[@]}" $EXTRA_JVM_OPTS \
    -cp "$CLASSPATH" \
    -DLOG_DIR="$LOG_DIR" \
    -DCONF_DIR="$CONF_DIR" \
    -DDATA_DIR="$DATA_DIR" \
    -DBIND_ADDR="$BIND_ADDR" \
    -Dlogback.configurationFile="$LOG_CONF_FILE" \
    -Dpf4j.pluginsDir="$PLUGIN_DIR" \
    $NAME -c "$CONF_FILE" >"${LOG_DIR}/stdout.log" 2>&1 </dev/null &
  PIDS=$(pid)
  echo "$NAME process started: $PIDS"
fi
