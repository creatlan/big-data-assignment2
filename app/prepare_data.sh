#!/bin/bash
set -e

APP_DIR=$(cd "$(dirname "$0")" && pwd)
source "$APP_DIR/.venv/bin/activate"
mkdir -p "$APP_DIR/data"

DOC_COUNT=$(find "$APP_DIR/data" -maxdepth 1 -type f -name "*.txt" | wc -l)
VALID_DOC_COUNT=$(find "$APP_DIR/data" -maxdepth 1 -type f -name "*.txt" -print0 | xargs -0 -r grep -Il "[[:alnum:]]" | wc -l)
COPY_PARQUET_TO_HDFS=${COPY_PARQUET_TO_HDFS:-0}

if [ "$DOC_COUNT" -lt 1000 ] || [ "$VALID_DOC_COUNT" -lt 1000 ]; then
  if [ ! -f "$APP_DIR/a.parquet" ]; then
    echo "a.parquet was not found"
    exit 1
  fi
  find "$APP_DIR/data" -maxdepth 1 -type f -name "*.txt" -delete
  spark-submit \
    --master local[2] \
    "$APP_DIR/prepare_data.py" \
    build-docs \
    --parquet "$APP_DIR/a.parquet" \
    --output-dir "$APP_DIR/data" \
    --count 1000
fi

hdfs dfs -rm -r -f /data >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /input >/dev/null 2>&1 || true

if [ "$COPY_PARQUET_TO_HDFS" = "1" ] && [ -f "$APP_DIR/a.parquet" ]; then
  hdfs dfs -rm -f /a.parquet >/dev/null 2>&1 || true
  hdfs dfs -put -f "$APP_DIR/a.parquet" /a.parquet
fi

hdfs dfs -put "$APP_DIR/data" /

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives /app/.venv.tar.gz#.venv \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  "$APP_DIR/prepare_data.py" \
  build-input \
  --input-dir /data \
  --output-dir /input/data

hdfs dfs -ls /data
hdfs dfs -ls /input/data
