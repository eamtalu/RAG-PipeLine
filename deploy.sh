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

# 5. restart the web tier and the background worker.
# The worker unit (fastapirag-worker.service) must be installed once beforehand and the web unit must
# carry RUN_BACKGROUND_WORKERS=false — see docs/background-workers-web-worker-split.md. Restarting a
# not-yet-installed worker unit would abort this script (set -e), so it is only restarted when present.
sudo systemctl restart fastapirag.service
if systemctl list-unit-files | grep -q '^fastapirag-worker\.service'; then
    sudo systemctl restart fastapirag-worker.service
else
    echo "⚠️  fastapirag-worker.service not installed yet — background loops are NOT running."
    echo "    Install it (see docs/background-workers-web-worker-split.md) before relying on polling."
fi

echo "Deployed ✅"
