#!/bin/bash
set -e

APP_DIR=$(cd "$(dirname "$0")" && pwd)

if [ "$#" -ne 1 ]; then
  echo "Usage: bash add_to_index.sh /path/to/<doc_id>_<doc_title>.txt"
  exit 1
fi

LOCAL_FILE=$1
if [ ! -f "$LOCAL_FILE" ]; then
  echo "Document file does not exist: $LOCAL_FILE"
  exit 1
fi

if [ -f "$APP_DIR/.venv/bin/activate" ]; then
  source "$APP_DIR/.venv/bin/activate"
fi

TMP_RECORD=$(mktemp)
trap 'rm -f "$TMP_RECORD"' EXIT

python3 "$APP_DIR/add_to_index.py" --emit-input-record "$LOCAL_FILE" > "$TMP_RECORD"
python3 "$APP_DIR/add_to_index.py" "$LOCAL_FILE"

hdfs dfs -mkdir -p /data /input/data
hdfs dfs -put -f "$LOCAL_FILE" "/data/$(basename "$LOCAL_FILE")"
hdfs dfs -put -f "$TMP_RECORD" "/input/data/add_$(date +%s)_$$.tsv"
