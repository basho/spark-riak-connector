#!/bin/bash
set -e
#set -x

#---------------
# Initialization
#---------------


JOB_CONFIG=$1
APP_ID=$2

if [ $APP_ID = "" ]; then
	APP_ID="app-"
fi

echo "Current Spark job ID: $APP_ID"

[ -z "$WORKER_IPS" ] && WORKER_IPS=(
'172.31.57.177'
'172.31.57.178'
'172.31.57.179'
)

[ -z "$SSH_USER_NAME" ] && SSH_USER_NAME="ubuntu"
[ -z "$SSH_KEY_PATH" ] && SSH_KEY_PATH="/home/ubuntu/.ssh/bdp.pem"
[ -z "$PERF4J_LOG_SRC" ] && PERF4J_LOG_SRC="/home/ubuntu/performance-tests/perf4j-raw-logs/perf-stats.log"
[ -z "$PERF4J_LOG_DST" ] && PERF4J_LOG_DST="/home/ubuntu/performance-tests/perf4j-collected-logs"
[ -z "$CODAHALE_LOG_SRC" ] && CODAHALE_LOG_SRC="/home/ubuntu/performance-tests/codahale-raw-logs"
[ -z "$CODAHALE_LOG_DST" ] && CODAHALE_LOG_DST="/home/ubuntu/performance-tests/codahale-collected-logs"

PERF4J_JAR="perf4j.jar"
CODAHALE_JAR="performance-metrics.jar"
COLLECT_TIME=$(date +"%Y-%m-%d_%T")

echo "Collecting performance logs from workers: ${WORKER_IPS[@]}..."


#--------------------------------------
# Collecting and processing Perf4j logs
#--------------------------------------

for WORKER_IP in "${WORKER_IPS[@]}"
do
	mkdir -p "$PERF4J_LOG_DST/$COLLECT_TIME"
	TARGET_FILE="$PERF4J_LOG_DST/$COLLECT_TIME/$WORKER_IP-worker-perf.log"
	echo "Copying performance log from $WORKER_IP..."
	scp -i "$SSH_KEY_PATH" "$SSH_USER_NAME@$WORKER_IP:/$PERF4J_LOG_SRC" "$TARGET_FILE"
	java -jar "$PERF4J_JAR" "$TARGET_FILE"  -t 900000000 > "$TARGET_FILE.stat"
	java -jar "$PERF4J_JAR" --graph "$TARGET_FILE.html" "$TARGET_FILE"
	echo "Performance log from $WORKER_IP saved to $TARGET_FILE"
	cat "$TARGET_FILE" >> "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log"
done

java -jar "$PERF4J_JAR" "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log"  -t 900000000 > "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log.stat"
java -jar "$PERF4J_JAR" --graph "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log.html" "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log"

if [ "$JOB_CONFIG" != "" ]; then
 	echo "$JOB_CONFIG" >> "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log.stat"
fi



#----------------------------------------
# Collecting and processing CodaHale logs
#----------------------------------------

for WORKER_IP in "${WORKER_IPS[@]}"
do
	TARGET_DIR="$CODAHALE_LOG_DST/$COLLECT_TIME"
        mkdir -p "$TARGET_DIR"
	SOURCE_FILES=($(ssh -i "$SSH_KEY_PATH" "$SSH_USER_NAME@$WORKER_IP" "ls $CODAHALE_LOG_SRC | grep riak-connector | grep $APP_ID"))
	for SOURCE_FILE in "${SOURCE_FILES[@]}"
	do
		echo "Copying performance log from $WORKER_IP: $SOURCE_FILE..."
		scp -i "$SSH_KEY_PATH" "$SSH_USER_NAME@$WORKER_IP:/$CODAHALE_LOG_SRC/$SOURCE_FILE" "$TARGET_DIR"
	done
  echo "Performance log from $WORKER_IP saved to $TARGET_DIR"
done

java -jar "$CODAHALE_JAR" "$CODAHALE_LOG_DST/$COLLECT_TIME" "$JOB_CONFIG"


