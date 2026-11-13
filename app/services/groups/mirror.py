"""The rows the edge reads for groups (stage D, step 12): `GROUPS` per customer (the groups with
their channels and members, the channels without their secrets) and `USER#<id>#GROUPS` per
person (the groups they belong to, by name). Rewritten after every command and by the consumer's
access sweep every minute (unchanged rows are skipped).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.groups import caps, routing, store
from app.services.notifications import channel_secrets
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed

logger = logging.getLogger(__name__)


async def groups_payload(db: AsyncSession, customer_code: str, *, now: datetime | None = None) -> dict:
    """The customer's groups and channels as the Admin tab draws them; no secrets."""
    now = now or datetime.now(timezone.utc)
    channels = await store.customer_channels(db, customer_code)
    names = {c.id: c.name for c in channels}
    views = await store.list_groups(db, customer_code)
    window = routing.window_start(now)
    counts = await caps.window_counts(db, customer_code, window)
    return {
        "as_of": now.isoformat(),
        "topics": ["advisory:delivery", "advisory:stock", "advisory:location", "schedule"],
        "window_start": window.isoformat(),
        "channels": [{"id": str(c.id), "name": c.name, "channel_type": c.channel_type, "enabled": c.enabled,
                      "config": channel_secrets.public_config(c.config)} for c in channels],
        "groups": [store.group_payload(v, names, counts.get(v.group.id)) for v in views],
    }


async def mirror_customer(db: AsyncSession, mirror: MirrorWriter, customer_code: str, *,
                          tracker: ChangeTracker | None = None, now: datetime | None = None,
                          people: list[str] | None = None) -> int:
    """Rewrite the customer's GROUPS row and the USER#<id>#GROUPS row of every member (or of
    `people`); rows written."""
    now = now or datetime.now(timezone.utc)
    payload = await groups_payload(db, customer_code, now=now)
    env = envelope("groups", customer_code, payload=payload, as_of=payload["as_of"], now=now.timestamp())
    written = int(await write_if_changed(mirror, tracker, env, now=now.timestamp()))
    members = set(people or [])
    for g in payload["groups"]:
        members.update(g["members"])
    for object_id in sorted(members):
        mine = [{"id": g["id"], "name": g["name"], "topics": g["topics"]} for g in payload["groups"] if object_id in g["members"]]
        env = envelope("user_groups", customer_code, payload={"object_id": object_id, "as_of": payload["as_of"], "groups": mine},
                       as_of=payload["as_of"], now=now.timestamp(), user_id=object_id)
        written += int(await write_if_changed(mirror, tracker, env, now=now.timestamp()))
    return written


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     session_factory=None, now: datetime | None = None) -> dict[str, int]:
    """Rewrite the group rows of every bound customer (or `only`); rows written per customer."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    written: dict[str, int] = {}
    async with factory() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        for code in codes:
            try:
                written[code] = await mirror_customer(db, mirror, code, tracker=tracker, now=now)
            except Exception:
                logger.exception("group rows for %s not mirrored", code)
    return written
