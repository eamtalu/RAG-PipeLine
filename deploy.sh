#!/usr/bin/env bash
# Simple deploy: pull latest code, run migrations, restart the service.
set -e

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="rag-api"          # <-- your systemd unit name
cd "$APP_DIR"

# 1. pull latest
git pull origin main

# 2. run DB migrations (PYTHONPATH lets alembic import app.*)
PYTHONPATH="$APP_DIR" ./venv/bin/alembic upgrade head

# 3. restart service
sudo systemctl restart "$SERVICE_NAME"

echo "Deployed ✅"
