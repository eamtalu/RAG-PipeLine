"""The secret part of a channel's config, kept in `tenant_secrets` and never returned by the API.

`seal_config` is applied at the API boundary (create and update), `public_config` on every
response, and `reveal_config` right before a send (the dispatcher and the test button). A row
written before this existed may still carry a plain `webhook_url`; it sends as before, is masked
in responses, and moves into the store on its next update or via scripts/seal_channel_secrets.py.
"""

from __future__ import annotations

import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.notification import CustomerNotificationChannel
from app.services.secrets import secret_store

logger = logging.getLogger(__name__)

#: Config keys whose value is a secret. Each becomes `<key>_secret_id` in the stored config and
#: `<key>_set` in the public one.
SECRET_FIELDS = ("webhook_url",)


def owner_of(channel_id: uuid.UUID) -> str:
    return f"notification_channel:{channel_id}"


def _ref(field: str) -> str:
    return f"{field}_secret_id"


def _flag(field: str) -> str:
    return f"{field}_set"


def _is_bookkeeping(key: str) -> bool:
    return any(key in (field, _ref(field), _flag(field)) for field in SECRET_FIELDS)


async def seal_config(
    db: AsyncSession,
    *,
    customer_code: str,
    channel_id: uuid.UUID,
    config: dict,
    previous: dict | None,
) -> dict:
    """The config to store: plain secrets moved into the store, references kept, flags dropped.

    ({"webhook_url": "https://x", "max_per_minute": 5}, previous=None)
      -> {"max_per_minute": 5, "webhook_url_secret_id": "<id>"} ;
    ({"webhook_url_set": True}, previous with a reference) -> the same reference kept ;
    ({"webhook_url": None}, previous with a reference) -> reference and secret removed
    """
    owner = owner_of(channel_id)
    out = {k: v for k, v in (config or {}).items() if not _is_bookkeeping(k)}
    for field in SECRET_FIELDS:
        incoming = config or {}
        # Step 1: A new plain value replaces whatever was stored, so that the old secret's bytes
        # never outlive the value the person just typed.
        if isinstance(incoming.get(field), str) and incoming[field].strip():
            sid = await secret_store.put(
                db,
                customer_code=customer_code,
                kind=field,
                owner=owner,
                plaintext=incoming[field].strip(),
            )
            out[_ref(field)] = str(sid)
            continue
        # Step 2: An explicit null clears the secret, because the masked form carries no value
        # and absence alone must not be read as "remove".
        if field in incoming and not incoming[field]:
            await secret_store.delete_for_owner(db, customer_code=customer_code, owner=owner)
            continue
        # Step 3: Otherwise keep what was there, moving a legacy plain value into the store
        # on the way, so that every update leaves the row cleaner than it found it.
        prior = previous or {}
        if prior.get(_ref(field)):
            out[_ref(field)] = prior[_ref(field)]
        elif isinstance(prior.get(field), str) and prior[field].strip():
            sid = await secret_store.put(
                db,
                customer_code=customer_code,
                kind=field,
                owner=owner,
                plaintext=prior[field].strip(),
            )
            out[_ref(field)] = str(sid)
    return out


def public_config(config: dict | None) -> dict:
    """What the API returns: no secret, no reference, a `<field>_set` flag per secret field.

    >>> public_config({"max_per_minute": 5, "webhook_url_secret_id": "abc"})
    {'max_per_minute': 5, 'webhook_url_set': True}
    >>> public_config({"webhook_url": "https://legacy"})
    {'webhook_url_set': True}
    >>> public_config({})
    {'webhook_url_set': False}
    """
    out = {k: v for k, v in (config or {}).items() if not _is_bookkeeping(k)}
    for field in SECRET_FIELDS:
        has_ref = bool((config or {}).get(_ref(field)))
        has_plain = isinstance((config or {}).get(field), str) and bool(
            (config or {})[field].strip()
        )
        out[_flag(field)] = has_ref or has_plain
    return out


async def reveal_config(db: AsyncSession, row: CustomerNotificationChannel) -> dict:
    """The config a transport needs, secrets filled in from the store for this row's tenant.

    (row with webhook_url_secret_id) -> {..., "webhook_url": "https://..."} ;
    (legacy row with a plain webhook_url) -> the same, with a warning in the log
    """
    config = dict(row.config or {})
    for field in SECRET_FIELDS:
        ref = config.pop(_ref(field), None)
        if ref:
            config[field] = await secret_store.reveal(
                db, uuid.UUID(str(ref)), customer_code=row.customer_code
            )
        elif isinstance(config.get(field), str) and config[field].strip():
            logger.warning("secrets.legacy_plaintext channel=%s field=%s", row.id, field)
    return config


async def delete_for_channel(db: AsyncSession, row: CustomerNotificationChannel) -> int:
    return await secret_store.delete_for_owner(
        db, customer_code=row.customer_code, owner=owner_of(row.id)
    )
