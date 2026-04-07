#!/bin/bash
set -e

APP_DIR=$(cd "$(dirname "$0")" && pwd)
source "$APP_DIR/.venv/bin/activate"

python3 "$APP_DIR/app.py" \
  --stats-path /indexer/stats \
  --index-path /indexer/index \
  --corpus-path /indexer/corpus
