#!/bin/bash
set -e

APP_DIR=$(cd "$(dirname "$0")" && pwd)
INPUT_PATH=${1:-/input/data}
STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming*.jar" | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
  echo "hadoop-streaming jar was not found"
  exit 1
fi

if ! hdfs dfs -test -e "$INPUT_PATH"; then
  echo "Input path does not exist: $INPUT_PATH"
  exit 1
fi

hdfs dfs -rm -r -f /indexer >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /indexer

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline-1" \
  -D mapreduce.job.reduces=1 \
  -files "$APP_DIR/mapreduce/mapper1.py,$APP_DIR/mapreduce/reducer1.py" \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output /indexer/stats

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline-2" \
  -D mapreduce.job.reduces=1 \
  -files "$APP_DIR/mapreduce/mapper2.py,$APP_DIR/mapreduce/reducer2.py" \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input /indexer/stats \
  -output /indexer/index

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline-3" \
  -D mapreduce.job.reduces=1 \
  -files "$APP_DIR/mapreduce/mapper3.py,$APP_DIR/mapreduce/reducer3.py" \
  -mapper "python3 mapper3.py" \
  -reducer "python3 reducer3.py" \
  -input /indexer/stats \
  -output /indexer/corpus

hdfs dfs -ls /indexer
