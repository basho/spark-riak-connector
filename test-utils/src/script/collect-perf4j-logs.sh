#!/bin/bash
set -e

[ -z "$WORKER_IPS" ] && WORKER_IPS=(
'172.31.57.177'
'172.31.57.178'
'172.31.57.179'
)

[ -z "$SSH_USER_NAME" ] && SSH_USER_NAME="ubuntu"
[ -z "$SSH_KEY_PATH" ] && SSH_KEY_PATH="/home/ubuntu/.ssh/bdp.pem"
[ -z "$PERF4J_LOG_SRC" ] && PERF4J_LOG_SRC="/home/ubuntu/perf-stats.log"
[ -z "$PERF4J_LOG_DST" ] && PERF4J_LOG_DST="/home/ubuntu/perf4j-collected-logs"
[ -z "$CODAHALE_LOG_SRC" ] && CODAHALE_LOG_SRC="/home/ubuntu/codahale-metrics"
[ -z "$CODAHALE_LOG_DST" ] && CODAHALE_LOG_DST="/home/ubuntu/codahale-collected-logs"

PERF4J_JAR="perf4j-0.9.16.jar"
COLLECT_TIME=$(date +"%Y-%m-%d_%T")

echo "Collecting performance logs from workers: ${WORKER_IPS[@]}..."

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


for WORKER_IP in "${WORKER_IPS[@]}"
do
	TARGET_DIR="$CODAHALE_LOG_DST/$COLLECT_TIME"
        mkdir -p "$TARGET_DIR"
	SOURCE_FILES=($(ssh -i "$SSH_KEY_PATH" "$SSH_USER_NAME@$WORKER_IP" "ls $CODAHALE_LOG_SRC | grep data-chunk | grep app-"))
	for SOURCE_FILE in "${SOURCE_FILES[@]}"
	do
		echo "Copying performance log from $WORKER_IP: $SOURCE_FILE..."
		scp -i "$SSH_KEY_PATH" "$SSH_USER_NAME@$WORKER_IP:/$CODAHALE_LOG_SRC/$SOURCE_FILE" "$TARGET_DIR"
	done
        echo "Performance log from $WORKER_IP saved to $TARGET_DIR"
#        cat "$TARGET_FILE" >> "$PERF4J_LOG_DST/$COLLECT_TIME/all-workers-perf.log"
done

