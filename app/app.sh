#!/bin/bash
set -e

cd /app
service ssh restart
bash /app/start-services.sh
python3 -m venv /app/.venv
source /app/.venv/bin/activate
pip install --upgrade pip
pip install --no-cache-dir -r /app/requirements.txt
rm -f /app/.venv.tar.gz
venv-pack -o /app/.venv.tar.gz
bash /app/prepare_data.sh
bash /app/index.sh
bash /app/search.sh "history of science"
tail -f /dev/null
