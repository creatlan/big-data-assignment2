#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ $# -lt 1 ]; then
	echo "Usage: $0 <local_txt_file>"
	exit 1
fi

LOCAL_FILE="$1"
if [ ! -f "$LOCAL_FILE" ]; then
	echo "File does not exist: ${LOCAL_FILE}"
	exit 1
fi

TMP_DIR="$(mktemp -d)"
HDFS_TMP_ROOT="/tmp/indexer/add-$$-$RANDOM"
HDFS_INPUT_DIR="${HDFS_TMP_ROOT}/input"
trap 'rm -rf "$TMP_DIR"; hdfs dfs -rm -r -f "$HDFS_TMP_ROOT" >/dev/null 2>&1 || true' EXIT

SCYLLA_HOST="${SCYLLA_HOST:-${CASSANDRA_HOST:-cassandra-server}}"
SCYLLA_PORT="${SCYLLA_PORT:-${CASSANDRA_PORT:-9042}}"
SCYLLA_KEYSPACE="${SCYLLA_KEYSPACE:-${CASSANDRA_KEYSPACE:-search_index}}"

RAW_BASENAME="$(basename "$LOCAL_FILE")"

echo "Validating input file: ${LOCAL_FILE}"
echo "Uploading raw file to HDFS temporary directory: ${HDFS_INPUT_DIR}"
hdfs dfs -mkdir -p "$HDFS_INPUT_DIR"
hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_INPUT_DIR/"

echo "Building a one-document index batch"
python3 - "$LOCAL_FILE" "$TMP_DIR" <<'PY'
from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path


TOKEN_PATTERN = re.compile(r"[^a-z0-9']+")


def tokenize(text: str) -> list[str]:
    cleaned_text = TOKEN_PATTERN.sub(" ", text.lower())
    return [token for token in cleaned_text.split() if token]


def split_name(file_name: str) -> tuple[str, str]:
    base_name = Path(file_name).name
    if base_name.endswith(".txt"):
        base_name = base_name[:-4]

    if "_" in base_name:
        doc_id, doc_title = base_name.split("_", 1)
    else:
        doc_id, doc_title = base_name, "untitled"

    doc_id = doc_id.strip() or "untitled"
    doc_title = doc_title.strip() or "untitled"
    return doc_id, doc_title


input_path = Path(sys.argv[1])
output_dir = Path(sys.argv[2])
text = input_path.read_text(encoding="utf-8")
doc_id, doc_title = split_name(input_path.name)
normalized_text = " ".join(text.split())
tokens = tokenize(normalized_text)
if not tokens:
    raise SystemExit(f"No indexable terms found in {input_path}")

term_counts = Counter(tokens)
doc_length = len(tokens)

index_lines = []
vocabulary_lines = []
for term_id, (term, frequency) in enumerate(sorted(term_counts.items()), start=1):
    index_lines.append(f"INDEX\t{term}\t1\t{frequency}\t{doc_id}|{doc_title}|{frequency}|{doc_length}")
    vocabulary_lines.append(f"VOCAB\t{term}\t{term_id}\t1\t{frequency}")

(output_dir / "index.tsv").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
(output_dir / "doc_stats.tsv").write_text(f"STAT\t{doc_id}\t{doc_title}\t{doc_length}\n", encoding="utf-8")
(output_dir / "vocabulary.tsv").write_text("\n".join(vocabulary_lines) + "\n", encoding="utf-8")
PY

echo "Updating Cassandra/ScyllaDB at ${SCYLLA_HOST}:${SCYLLA_PORT}, keyspace ${SCYLLA_KEYSPACE}"
python3 store_index.py \
	--incremental \
	--host "$SCYLLA_HOST" \
	--port "$SCYLLA_PORT" \
	--keyspace "$SCYLLA_KEYSPACE" \
	--index_file "$TMP_DIR/index.tsv" \
	--doc_stats_file "$TMP_DIR/doc_stats.tsv" \
	--vocabulary_file "$TMP_DIR/vocabulary.tsv"

echo "Added document ${RAW_BASENAME} to the index"