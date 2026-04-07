#!/bin/bash
set -e

APP_DIR=$(cd "$(dirname "$0")" && pwd)
INPUT_PATH=${1:-/input/data}

bash "$APP_DIR/create_index.sh" "$INPUT_PATH"
bash "$APP_DIR/store_index.sh"
