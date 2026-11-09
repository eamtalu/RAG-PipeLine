# Tenant secrets store

Since 9 November 2026 (step 8 of `docs/esmart-agent-execution-plan.md`) a tenant's secret values are sealed at rest in `tenant_secrets` and never returned by the API.
Today that is the webhook URL of a notification channel; data connector credentials follow when connectors arrive.

## How a value is stored

- `app/services/secrets/envelope.py`: each secret gets its own random data key (AES-256-GCM); the data key is wrapped with the active master key and stored beside the ciphertext together with the master key's id.
- `app/services/secrets/secret_store.py`: `put` replaces the secret of the same owner and kind, `reveal` requires the owning tenant's code, `delete_for_owner`, and `rewrap_all` for rotation.
- `app/services/notifications/channel_secrets.py`: a channel config's `webhook_url` is moved into the store at the API boundary and replaced by `webhook_url_secret_id`; responses carry `webhook_url_set: true|false` and never the value; the dispatcher and the test button reveal the URL right before a send.
- A tenant purge (`purge_logspace`) deletes the tenant's secrets, and deleting a channel deletes its secret.

## Settings

| setting | meaning |
| --- | --- |
| `SECRETS_MASTER_KEYS` | `kid=<urlsafe base64 of 32 random bytes>,...`; several during a rotation |
| `SECRETS_ACTIVE_KEY_ID` | the id new secrets are wrapped under; defaults to the only key when one is set |

Generate a key with `python -c "import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`.
With no key configured the store refuses to seal, so a channel cannot be created with a webhook URL: fail closed, never plain text.
Keep the master key in the service's environment file only, out of git and out of database backups; a backup without the key holds nothing readable.

## Rotation

1. Generate a new key and add it beside the old one: `SECRETS_MASTER_KEYS=k2=<new>,k1=<old>`, `SECRETS_ACTIVE_KEY_ID=k2`. Restart the API, the worker and the Teams consumer.
2. Run `python scripts/rotate_secrets_master_key.py`; it re-wraps every secret under `k2` and prints how many moved. No ciphertext changes.
3. Remove `k1` from the setting and restart again. A secret still wrapped under a removed key fails to open with a clear error naming the key, so run the script before removing.

## Rows written before this existed

A channel created before step 8 still has a plain `webhook_url` in its config.
It keeps sending, its responses are masked, and it moves into the store on its next update.
To move every such row at once run `python scripts/seal_channel_secrets.py`.

## SSH key material

Inline SSH private keys for log sources keep their own Fernet encryption under `SSH_SECRET_KEY` (`app/services/mnp_log_ingestion/remote/secrets.py`); moving them into this store is a later step.
