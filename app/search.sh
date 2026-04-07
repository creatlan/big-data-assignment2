#!/bin/bash
set -e

APP_DIR=$(cd "$(dirname "$0")" && pwd)
source "$APP_DIR/.venv/bin/activate"

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
  "$APP_DIR/query.py" \
  "$@"
