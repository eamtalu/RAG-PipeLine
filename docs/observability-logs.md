# Logs: one line per event, with the request id and the tenant

Since 9 November 2026 (step 9 of `docs/esmart-agent-execution-plan.md`) every log line written by the API, the worker and the Teams consumer carries two fields: `request_id` and `tenant`.
They are held in context variables (`app/log_context.py`) and stamped on every record by the log record factory that `app.background.setup_logging` installs, so no logger can write a line without them.

## Where the values come from

| process | request_id | tenant |
| --- | --- | --- |
| API (`app/middleware/request_context.py`) | the caller's `X-Request-ID`, else minted; echoed back in the response header `X-Request-ID` | the `X-Customer-Code` header as sent |
| Teams consumer (`process_message`) | the edge's job id | the job's customer code |
| Notification dispatcher | the delivery id | the event's customer code |
| Workers, per tenant pass (at-risk, forecast, reconcile, stitch, SSH poll, cleanup) | a fresh id per pass | the tenant being worked on |

The API also writes one `api.request` line per request with the method, the path, the status and the duration in milliseconds; `/health` is left out.

## Output

`LOG_OUTPUT=json` (default): one JSON object per line.

```
{"ts": "2026-11-09T16:11:24.060+00:00", "level": "INFO", "logger": "app.middleware.request_context", "msg": "api.request method=GET path=/api/v1/customers status=200 ms=12", "request_id": "abc-123", "tenant": "tmp-live"}
```

`exc` is added when the line carries a traceback.
`LOG_OUTPUT=text` writes the same fields in a plain line for a terminal, `-` when unbound.

## Reading them on the server

```
sudo journalctl -u fastapirag -o cat --since "-1 hour" | jq -r 'select(.tenant=="tmp-live") | "\(.ts) \(.level) \(.msg)"'
sudo journalctl -u fastapirag-teams-consumer -o cat | jq -c 'select(.request_id=="<job id>")'
sudo journalctl -u fastapirag -o cat | jq -c 'select(.level=="ERROR")'
```

## Message keys

New messages use the `area.action` form, then `key=value` pairs: `api.request method=GET ...`, `api.auth.anonymous path=...`, `secrets.legacy_plaintext channel=...`.
Older messages are sentences; they are not rewritten, they simply carry the two fields now.
