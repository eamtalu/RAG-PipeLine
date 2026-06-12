#!/usr/bin/env bash
#
# deploy.sh — one-shot deploy for the RAG FastAPI service on Ubuntu.
#
#   What it does, in order:
#     1. git pull the latest code for the configured branch (fast-forward only)
#     2. pip install -r requirements.txt  — only if requirements.txt actually changed
#     3. alembic upgrade head             — only if the DB is behind the latest migration
#     4. systemctl restart <service>      — then verify it came back up (/health)
#
#   Each step is conditional ("if needed"), so re-running with no new commits is a cheap no-op.
#
#   Usage:   ./deploy.sh
#   First-time setup on the server:  chmod +x deploy.sh
#
#   Override any setting without editing the file, e.g.:
#     SERVICE_NAME=rag-api BRANCH=main ./deploy.sh
#
set -Eeuo pipefail

# ----------------------------------------------------------------------------------------------
# Config — adjust these to match the server (or override via env vars when calling the script).
# ----------------------------------------------------------------------------------------------
# Directory of THIS script == the repo root (so the script works regardless of where it's called).
APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
VENV="${VENV:-$APP_DIR/venv}"            # virtualenv inside the repo dir; falls back to .venv below
BRANCH="${BRANCH:-main}"                  # git branch to deploy
SERVICE_NAME="${SERVICE_NAME:-rag-api}"   # systemd unit name (systemctl restart $SERVICE_NAME)
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"  # post-restart smoke check ("" to skip)

# ----------------------------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------------------------
log()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

# Use sudo for systemctl only if we're not already root.
SUDO=""
[ "$(id -u)" -ne 0 ] && SUDO="sudo"

# Pick the venv dir (support both ./venv and ./.venv) then point PY/PIP/ALEMBIC at its binaries.
if [ ! -x "$VENV/bin/python" ] && [ -x "$APP_DIR/.venv/bin/python" ]; then
  VENV="$APP_DIR/.venv"
fi
[ -x "$VENV/bin/python" ] || die "No virtualenv python at '$VENV/bin/python'. Set VENV=... or create the venv."
PY="$VENV/bin/python"
PIP="$VENV/bin/pip"
ALEMBIC="$VENV/bin/alembic"

# alembic loads app.* during env.py, so the repo root must be importable.
export PYTHONPATH="$APP_DIR${PYTHONPATH:+:$PYTHONPATH}"

cd "$APP_DIR"

# ----------------------------------------------------------------------------------------------
# 1. Pull latest code
# ----------------------------------------------------------------------------------------------
log "Pulling latest code ($BRANCH)…"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "$APP_DIR is not a git repo."

if ! git diff --quiet || ! git diff --cached --quiet; then
  warn "Working tree has local changes — leaving them in place and attempting a fast-forward."
fi

OLD_REV="$(git rev-parse HEAD)"
git fetch --prune origin "$BRANCH"
git checkout "$BRANCH" >/dev/null 2>&1 || die "Cannot checkout branch '$BRANCH'."
git merge --ff-only "origin/$BRANCH" || die "Fast-forward failed (local commits/divergence). Resolve manually."
NEW_REV="$(git rev-parse HEAD)"

if [ "$OLD_REV" = "$NEW_REV" ]; then
  ok "Already up to date at ${NEW_REV:0:8} — no new commits."
else
  ok "Updated ${OLD_REV:0:8} → ${NEW_REV:0:8}"
fi

# ----------------------------------------------------------------------------------------------
# 2. Install deps — only if requirements.txt changed (or this is the first run)
# ----------------------------------------------------------------------------------------------
log "Checking Python dependencies…"
if [ "$OLD_REV" = "$NEW_REV" ]; then
  ok "No code change — skipping pip install."
elif ! git diff --quiet "$OLD_REV" "$NEW_REV" -- requirements.txt; then
  warn "requirements.txt changed — installing…"
  "$PIP" install -r requirements.txt
  ok "Dependencies updated."
else
  ok "requirements.txt unchanged — skipping pip install."
fi

# ----------------------------------------------------------------------------------------------
# 3. Run DB migrations — only if the DB is behind the latest head
# ----------------------------------------------------------------------------------------------
log "Checking database migrations…"
CURRENT="$("$ALEMBIC" current 2>/dev/null | grep -oE '^[0-9a-f]+' | head -1 || true)"
HEAD_REV="$("$ALEMBIC" heads 2>/dev/null | grep -oE '^[0-9a-f]+' | head -1 || true)"
[ -n "$HEAD_REV" ] || die "Could not read alembic heads — check DB connectivity / alembic config."

if [ "$CURRENT" = "$HEAD_REV" ]; then
  ok "DB already at head ($HEAD_REV) — no migration needed."
else
  warn "DB at '${CURRENT:-<none>}' → upgrading to '$HEAD_REV'…"
  "$ALEMBIC" upgrade head
  ok "Migrations applied."
fi

# ----------------------------------------------------------------------------------------------
# 4. Restart the service + verify
# ----------------------------------------------------------------------------------------------
log "Restarting service '$SERVICE_NAME'…"
$SUDO systemctl restart "$SERVICE_NAME"
sleep 2

if ! $SUDO systemctl is-active --quiet "$SERVICE_NAME"; then
  $SUDO systemctl status "$SERVICE_NAME" --no-pager -l | tail -n 30 || true
  die "Service '$SERVICE_NAME' is not active after restart. See status above / 'journalctl -u $SERVICE_NAME -e'."
fi
ok "Service is active."

if [ -n "$HEALTH_URL" ]; then
  log "Health check: $HEALTH_URL"
  for i in 1 2 3 4 5; do
    if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1; then
      ok "Health check passed."
      break
    fi
    [ "$i" -eq 5 ] && die "Health check failed after restart. Check 'journalctl -u $SERVICE_NAME -e'."
    sleep 2
  done
fi

log "Deploy complete ✅  (now at ${NEW_REV:0:8})"
