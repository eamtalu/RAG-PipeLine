# API service tokens

Since 9 November 2026 every call under `/api/v1` names its caller with a bearer token (step 7 of `docs/esmart-agent-execution-plan.md`).
The check lives in `app/api/service_auth.py` and runs as a dependency of the router in `app/api/v1/router.py`; `/health` stays open.
The `X-Customer-Code` header is unchanged: the token says which system is calling, the header says which tenant the call is about.

## Settings

| setting | meaning |
| --- | --- |
| `API_SERVICE_TOKENS` | `name=token,name=token`; the name is what the log shows |
| `API_ADMIN_CALLERS` | names that may use the admin routes (`require_admin`) |
| `API_AUTH_REQUIRED` | `false` during rollout: a call without a token passes with a warning; `true` refuses it with 401 |

A wrong token is refused with 401 in both modes.
A caller that is not an admin gets 403 on an admin route.
Tokens are compared in constant time.

## Rollout

1. Generate a token per caller: `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
2. Backend: set `API_SERVICE_TOKENS=web=<token>,ops=<token>` and `API_ADMIN_CALLERS=ops`, keep `API_AUTH_REQUIRED=false`, restart `fastapirag`.
3. Web app: set `API_SERVICE_TOKEN=<the web token>` in its environment and restart it; its Next.js middleware adds the header on every `/api/v1` call.
4. Watch the backend log for `api.auth.anonymous`; when it is quiet, set `API_AUTH_REQUIRED=true` and restart `fastapirag`.
5. Recommended: bind gunicorn to `127.0.0.1:8000` in the systemd unit and point the web app's rewrite at `http://127.0.0.1:8000`, so the API is not reachable from the LAN at all.

## Rotation

Add the new token to the list beside the old one, restart the backend, move the web app to the new token, then remove the old one.

## Operators

`curl -H "Authorization: Bearer <ops token>" -H "X-Customer-Code: tmp-live" http://localhost:8000/api/v1/...`
