#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

INDEX_HDFS_PATH="${INDEX_HDFS_PATH:-/indexer/index}"
DOC_STATS_HDFS_PATH="${DOC_STATS_HDFS_PATH:-/indexer/doc_stats}"
VOCABULARY_HDFS_PATH="${VOCABULARY_HDFS_PATH:-/indexer/vocabulary}"

SCYLLA_HOST="${SCYLLA_HOST:-${CASSANDRA_HOST:-cassandra-server}}"
SCYLLA_PORT="${SCYLLA_PORT:-${CASSANDRA_PORT:-9042}}"
SCYLLA_KEYSPACE="${SCYLLA_KEYSPACE:-${CASSANDRA_KEYSPACE:-search_index}}"

INDEX_LOCAL_PATH="${TMP_DIR}/index"
DOC_STATS_LOCAL_PATH="${TMP_DIR}/doc_stats"
VOCABULARY_LOCAL_PATH="${TMP_DIR}/vocabulary"

echo "Preparing local index files in ${TMP_DIR}"
echo "Downloading ${INDEX_HDFS_PATH}"
hdfs dfs -get -f "$INDEX_HDFS_PATH" "$INDEX_LOCAL_PATH"
echo "Downloading ${DOC_STATS_HDFS_PATH}"
hdfs dfs -get -f "$DOC_STATS_HDFS_PATH" "$DOC_STATS_LOCAL_PATH"
echo "Downloading ${VOCABULARY_HDFS_PATH}"
hdfs dfs -get -f "$VOCABULARY_HDFS_PATH" "$VOCABULARY_LOCAL_PATH"

echo "Loading data into Cassandra/ScyllaDB at ${SCYLLA_HOST}:${SCYLLA_PORT}, keyspace ${SCYLLA_KEYSPACE}"
python3 store_index.py \
	--host "$SCYLLA_HOST" \
	--port "$SCYLLA_PORT" \
	--keyspace "$SCYLLA_KEYSPACE" \
	--index_file "$INDEX_LOCAL_PATH" \
	--doc_stats_file "$DOC_STATS_LOCAL_PATH" \
	--vocabulary_file "$VOCABULARY_LOCAL_PATH"

echo "Index loading completed successfully"
