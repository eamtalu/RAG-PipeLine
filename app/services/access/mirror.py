"""What the edge reads of access control (stage D, step 7): one `user_access` row per person, the
`roles` row (matrix and names), the `users`, `approvals` and `audit` rows for the Admin tab, all
under EYE#<customer>. Rewritten by the consumer's sweep every minute (unchanged rows are skipped
by the change tracker) and at once after a change through the ops API or a command.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import TeamsUser, TeamsUserSite
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.services.access import approvals, audit, records, users
from app.services.access import matrix as matrix_file
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed

logger = logging.getLogger(__name__)

AUDIT_ROWS = 100
APPROVAL_ROWS = 50


async def roles_payload(db: AsyncSession, customer_code: str) -> dict:
    """The matrix and the names, as the Admin tab draws them and the role chip reads them."""
    m = matrix_file.load()
    cells = await records.customer_matrix(db, customer_code)
    return {
        "roles": [{"code": r.code, "name": r.name} for r in m.roles],
        "capabilities": [{"code": c.code, "name": c.name} for c in m.capabilities],
        "matrix": {
            c.code: {r.code: cells.get((r.code, c.code), "no") for r in m.roles}
            for c in m.capabilities
        },
    }


async def mirror_customer(db: AsyncSession, mirror: MirrorWriter, customer_code: str, *,
                          tracker: ChangeTracker | None = None, now: datetime | None = None) -> int:
    """Rewrite every access row of one customer; how many rows were written (unchanged skipped).

    ("acme" with 3 users) -> up to 7 rows: 3 user_access, roles, users, approvals, audit
    """
    now = now or datetime.now(timezone.utc)
    as_of, stamp = now.isoformat(), now.timestamp()
    written = 0
    # Step 1: One record per person, removed for a deactivated one so the edge locks them out
    # on the next request rather than on the next sweep of a stale row
    people = await users.list_users(db, customer_code)
    sites_by_user: dict = {}
    for row in (await db.execute(
            select(TeamsUserSite).where(TeamsUserSite.customer_code == customer_code))).scalars():
        sites_by_user.setdefault(row.user_id, []).append(row.site_id)
    for user in people:
        if user.status == "deactivated":
            pk, sk = _key("user_access", customer_code, user.object_id)
            await mirror.remove(pk, sk)
            continue
        record = await records.record_for_user(db, user)
        env = envelope("user_access", customer_code, payload=record.as_payload(), as_of=as_of,
                       now=stamp, user_id=user.object_id)
        written += int(await write_if_changed(mirror, tracker, env, now=stamp))
    # Step 2: The customer-wide rows the Admin tab lists
    listing = {"users": [users.user_payload(u, sorted(sites_by_user.get(u.id, []))) for u in people]}
    queue = {"pending": [approvals.approval_payload(a) for a in await approvals.pending(db, customer_code)],
             "recent": [approvals.approval_payload(a)
                        for a in await approvals.recent(db, customer_code, limit=APPROVAL_ROWS)]}
    log = {"events": [audit.event_payload(e)
                      for e in await audit.recent_events(db, customer_code, limit=AUDIT_ROWS)]}
    for row_type, payload in (("roles", await roles_payload(db, customer_code)), ("users", listing),
                              ("approvals", queue), ("audit", log)):
        env = envelope(row_type, customer_code, payload=payload, as_of=as_of, now=stamp)
        written += int(await write_if_changed(mirror, tracker, env, now=stamp))
    return written


def _key(row_type: str, customer_code: str, user_id: str) -> tuple[str, str]:
    from app.services.teams.mirror_rows import KEY_LAYOUT

    return KEY_LAYOUT[row_type].key(customer_code, user_id=user_id)


async def sweep_once(mirror: MirrorWriter, *, tracker: ChangeTracker | None = None,
                     session_factory=None) -> dict[str, int]:
    """Rewrite the access rows of every bound customer; rows written per customer."""
    from app.config.database import async_session

    factory = session_factory or async_session
    written: dict[str, int] = {}
    async with factory() as db:
        codes = sorted({
            row.customer_code for row in (await db.execute(
                select(TeamsTenantBinding).where(TeamsTenantBinding.enabled.is_(True)))).scalars()
        })
        for code in codes:
            try:
                written[code] = await mirror_customer(db, mirror, code, tracker=tracker)
                await db.commit()  # the matrix seed, if any
            except Exception:
                logger.exception("access mirror failed for %s", code)
    return written


def users_of(db_users: list[TeamsUser]) -> list[str]:
    return [u.object_id for u in db_users]
