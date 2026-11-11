# Stage A deploy runbook (backend and web app on the Matrix server)

Written 10 November 2026 for the stage A steps 3 to 9 of `docs/esmart-agent-execution-plan.md`.
The edge (Teams tab) is already live on image `2501b12`; the live backend accepts its version 2 jobs, so the order below has no gap.
Run these from a terminal on the office LAN; `sudo` asks for the password.

## 1. Settings, generated on the server and never printed

```
ssh -t -o RemoteCommand=none amin@192.168.0.142
cd /opt/RAG-Pipeline/RAG-PipeLine
grep -cE '^(API_SERVICE_TOKENS|SECRETS_MASTER_KEYS)=' .env    # must print 0: not yet configured
WEB=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
OPS=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
KEY=$(python3 -c "import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
printf '\n# stage A, 2026-11-10\nAPI_SERVICE_TOKENS=web=%s,ops=%s\nAPI_ADMIN_CALLERS=ops\nAPI_AUTH_REQUIRED=false\nSECRETS_MASTER_KEYS=k1=%s\nSECRETS_ACTIVE_KEY_ID=k1\nLOG_OUTPUT=json\n' "$WEB" "$OPS" "$KEY" >> .env
printf 'API_SERVICE_TOKEN=%s\n' "$WEB" >> /var/www/matrix-log-explorer/.env.production
unset WEB OPS KEY
```

`.env` is read by the API, the worker and the Teams consumer (all run from this directory); `/etc/fastapirag/teams-consumer.env` holds only the consumer's AWS settings and needs no change.
Keep the master key out of git and out of database backups.

## 2. Backend

```
./deploy.sh      # git pull, pip install, alembic upgrade head (agent_runs, tenant_secrets), restarts api + worker + consumer
PYTHONPATH=$PWD venv/bin/python scripts/seal_channel_secrets.py   # moves existing webhook URLs into the store
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/health          # 200
curl -s -o /dev/null -w "%{http_code}\n" -H "X-Customer-Code: tmp-live" http://localhost:8000/api/v1/customers/timezones   # 200 (token optional for now)
sudo journalctl -u fastapirag-teams-consumer --since "-2 min" -o cat | tail -3      # JSON lines, no traceback
```

## 3. Web app

```
cd /var/www/matrix-log-explorer && git pull && npm ci && npm run build && pm2 restart matrix-log-explorer --update-env
```

Open the app in a browser and use a few pages (transactions, analytics, manage).

## 4. Verify, then switch the token to required

```
sudo journalctl -u fastapirag --since "-10 min" -o cat | grep -c api.auth.anonymous   # 0 while you used the web app
sed -i 's/^API_AUTH_REQUIRED=false$/API_AUTH_REQUIRED=true/' /opt/RAG-Pipeline/RAG-PipeLine/.env
sudo systemctl restart fastapirag
curl -s -o /dev/null -w "%{http_code}\n" -H "X-Customer-Code: tmp-live" http://localhost:8000/api/v1/customers/timezones   # 401 now
```

The web app keeps working because its server sends the token.
An operator call needs the `ops` token: `-H "Authorization: Bearer <ops token>"` (read it from `.env`).

## 5. Optional hardening (sudo)

In `/etc/systemd/system/fastapirag.service` change `--bind 0.0.0.0:8000` to `--bind 127.0.0.1:8000`; set `NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000` in the web app's `.env.production`; `sudo systemctl daemon-reload && sudo systemctl restart fastapirag`; rebuild and restart the web app.
Then the API is unreachable from the LAN and the token is the second lock.

## Stage B deploys

Step 12 removed the `TEAMS_AGENT` and `LOG_AGENT_*` settings; the settings class refuses unknown dotenv keys, so before the restart check `grep -nE '^(TEAMS_AGENT|LOG_AGENT_)' /opt/RAG-Pipeline/RAG-PipeLine/.env` prints nothing and delete any line it prints.
`ANTHROPIC_API_KEY` may stay.
Step 14 adds the `agent_traces` table (migration `a7c3e9d41b02`); `deploy.sh` runs `alembic upgrade head`, so after the restart `PGPASSWORD=rag psql -h localhost -U rag -d rag -c '\d agent_traces'` must list the table, and the first question from Teams or the web must leave one row in it.
Step 16 adds `quota_windows` and `agent_in_flight` (migration `b8d4f0a52c17`); the limits default to 60 questions per tenant and 20 per person an hour and 8 runs in flight, overridable with `AGENT_QUOTA_TENANT_PER_HOUR`, `AGENT_QUOTA_USER_PER_HOUR` and `AGENT_QUOTA_TENANT_IN_FLIGHT` in `.env` (docs/agent-quotas.md).
Steps 17 and 18 add no table; their settings are optional: `ANALYTICS_AGENT_FALLBACK_MODELS`, `ANALYTICS_AGENT_GATE_MODEL`, `ANALYTICS_AGENT_CALL_TIMEOUT_SECONDS`, `ANALYTICS_AGENT_PROVIDER_BULKHEAD`, `ANALYTICS_AGENT_BREAKER_FAILURES`, `ANALYTICS_AGENT_BREAKER_COOLDOWN_SECONDS` (docs/agent-router.md); without them the router behaves as before with a 60 s cap per model call.
Step 19 adds `usage_daily` (migration `c9e5a1b63d04`) and the optional `ANALYTICS_AGENT_PRICES` JSON (docs/agent-usage-accounting.md); without it every call is recorded with no cost. After the deploy `GET /api/v1/analytics/agent/usage` with an admin token and `X-Customer-Code: tmp-live` must list today's calls once a question has been asked.

## Rollback

`git checkout <previous sha>` in the backend directory and `./deploy.sh` again; the migrations are additive (two new tables) and can stay.
