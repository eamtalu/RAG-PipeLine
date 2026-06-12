#!/usr/bin/env bash
# Simple deploy: pull latest code, update deps, run migrations, restart the service.
set -e

cd /opt/RAG-Pipeline/RAG-PipeLine/

# 1. pull latest code
git pull

# 2. activate venv
source venv/bin/activate

# 3. install any new/updated dependencies
pip install -r requirements.txt

# 4. apply DB migrations
alembic upgrade head

# 5. restart the service
sudo systemctl restart fastapirag.service

echo "Deployed ✅"
