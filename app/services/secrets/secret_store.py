"""The `tenant_secrets` rows: put, reveal to the owning tenant only, delete, re-wrap for rotation.

Callers pass a session and commit themselves, so a secret lands in the same transaction as the
row that references it (`app/services/notifications/channel_secrets.py`).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.tenant_secret import TenantSecret
from app.services.secrets import envelope


class SecretNotFound(LookupError):
    """No such secret for this tenant; an id that belongs to another tenant counts as none."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def put(
    db: AsyncSession, *, customer_code: str, kind: str, owner: str, plaintext: str
) -> uuid.UUID:
    """Seal `plaintext` for this tenant, replacing any secret of the same kind and owner.

    (acme, webhook_url, channel:1, "https://...") -> id ; put again for the same owner -> a new id,
    the old row gone
    """
    master = envelope.MasterKeys.from_settings()
    sealed = envelope.seal(plaintext.encode("utf-8"), master)
    # Replace rather than update, so that a rotated or re-set secret never keeps old bytes around.
    await db.execute(
        delete(TenantSecret).where(
            TenantSecret.customer_code == customer_code,
            TenantSecret.owner == owner,
            TenantSecret.kind == kind,
        )
    )
    row = TenantSecret(
        id=uuid.uuid4(),
        customer_code=customer_code,
        kind=kind,
        owner=owner,
        key_id=sealed.key_id,
        wrapped_key=sealed.wrapped_key,
        ciphertext=sealed.ciphertext,
        created_at=_now(),
    )
    db.add(row)
    await db.flush()
    return row.id


async def reveal(db: AsyncSession, secret_id: uuid.UUID, *, customer_code: str) -> str:
    """The plaintext, only when the secret belongs to `customer_code`.

    (id of acme's secret, "acme") -> "https://..." ; (same id, "other") -> SecretNotFound
    """
    row = (
        await db.execute(
            select(TenantSecret).where(
                TenantSecret.id == secret_id, TenantSecret.customer_code == customer_code
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise SecretNotFound(f"no secret {secret_id} for {customer_code!r}")
    sealed = envelope.Sealed(row.key_id, row.wrapped_key, row.ciphertext)
    return envelope.open_sealed(sealed, envelope.MasterKeys.from_settings()).decode("utf-8")


async def delete_for_owner(db: AsyncSession, *, customer_code: str, owner: str) -> int:
    result = await db.execute(
        delete(TenantSecret).where(
            TenantSecret.customer_code == customer_code, TenantSecret.owner == owner
        )
    )
    return result.rowcount or 0


async def rewrap_all(db: AsyncSession) -> int:
    """Re-wrap every secret not yet under the active master key; how many moved.

    (3 rows under k1, active k2) -> 3 ; (all under k2) -> 0
    """
    master = envelope.MasterKeys.from_settings()
    rows = (
        (await db.execute(select(TenantSecret).where(TenantSecret.key_id != master.active)))
        .scalars()
        .all()
    )
    for row in rows:
        moved = envelope.rewrap(
            envelope.Sealed(row.key_id, row.wrapped_key, row.ciphertext), master
        )
        row.key_id, row.wrapped_key, row.rotated_at = moved.key_id, moved.wrapped_key, _now()
    await db.flush()
    return len(rows)
