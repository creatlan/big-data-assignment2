#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

INPUT_PATH="${1:-/input/data}"
TMP_ROOT="/tmp/indexer"
INDEX_OUTPUT="/indexer/index"
DOC_STATS_OUTPUT="/indexer/doc_stats"
VOCABULARY_OUTPUT="/indexer/vocabulary"

HADOOP_STREAMING_JAR="${HADOOP_STREAMING_JAR:-}"
if [ -z "$HADOOP_STREAMING_JAR" ]; then
	HADOOP_STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming*.jar' | head -n 1)
fi

if [ -z "${HADOOP_STREAMING_JAR:-}" ]; then
	echo "Could not locate hadoop-streaming jar"
	exit 1
fi

echo "Using input path: ${INPUT_PATH}"
echo "Using Hadoop Streaming jar: ${HADOOP_STREAMING_JAR}"

echo "Cleaning old HDFS outputs"
hdfs dfs -rm -r -f "$TMP_ROOT" >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /indexer >/dev/null 2>&1 || true

echo "Starting pipeline 1: build inverted index -> ${INDEX_OUTPUT}"
hadoop jar "$HADOOP_STREAMING_JAR" \
	-D mapreduce.job.name="big-data-assignment2-build-index" \
	-D mapreduce.job.reduces=1 \
	-files "mapreduce/mapper1.py,mapreduce/reducer1.py" \
	-input "$INPUT_PATH" \
	-output "$INDEX_OUTPUT" \
	-mapper "python3 mapper1.py" \
	-reducer "python3 reducer1.py"

echo "Starting pipeline 2: build document statistics -> ${DOC_STATS_OUTPUT}"
hadoop jar "$HADOOP_STREAMING_JAR" \
	-D mapreduce.job.name="big-data-assignment2-build-doc-stats" \
	-D mapreduce.job.reduces=1 \
	-files "mapreduce/mapper2.py,mapreduce/reducer2.py" \
	-input "$INPUT_PATH" \
	-output "$DOC_STATS_OUTPUT" \
	-mapper "python3 mapper2.py" \
	-reducer "python3 reducer2.py"

echo "Starting pipeline 3: build vocabulary -> ${VOCABULARY_OUTPUT}"
hadoop jar "$HADOOP_STREAMING_JAR" \
	-D mapreduce.job.name="big-data-assignment2-build-vocabulary" \
	-D mapreduce.job.reduces=1 \
	-files "mapreduce/mapper_vocab.py,mapreduce/reducer_vocab.py" \
	-input "$INDEX_OUTPUT" \
	-output "$VOCABULARY_OUTPUT" \
	-mapper "python3 mapper_vocab.py" \
	-reducer "python3 reducer_vocab.py"

echo "Index data created under ${INDEX_OUTPUT}, ${DOC_STATS_OUTPUT}, and ${VOCABULARY_OUTPUT}"
