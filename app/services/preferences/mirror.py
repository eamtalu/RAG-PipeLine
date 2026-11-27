"""The row the edge reads for one person's preferences: `USER#<id>#PREFS`.

It holds every area resolved for that person (`areas`), what they set themselves (`own`) and
each page's default view (`defaults`, their role's where the role has one), so the app can draw
at once and can switch between a saved view and the default. It also carries the limits the
screen needs (`limits`) and, for people who may manage users, which roles have a default of
their own (`role_defaults`). Rewritten after every command that could have moved it, and by
the consumer's sweep, because an Admin's change to a person's sites or role moves it too.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.preference import Preference

from app.services import access
from app.services.preferences import areas, store
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed

logger = logging.getLogger(__name__)

ROW_TYPE = "user_prefs"
MANAGE_USERS = "manage_users"


async def user_payload(db: AsyncSession, record: access.AccessRecord) -> dict:
    """One person's row: every area resolved, what they set themselves, and each page as it
    comes, so the app can show "Default" beside a saved view without asking again.

    -> {"areas": {area: values}, "own": {area: what they set}, "defaults": {page: its default},
        "limits": {...}} and, for someone who may manage users, "role_defaults": {page: [roles]}
    """
    payload = {"areas": await store.resolve(db, record), "own": await store.own(db, record),
               "defaults": await store.page_defaults(db, record),
               "limits": {"line_search_days_max": areas.MAX_LINE_SEARCH_DAYS}}
    # Which roles differ is for the people who set it; nobody else can act on it
    if access.decide(record, MANAGE_USERS).is_allowed:
        payload["role_defaults"] = await store.roles_with_a_default(db, record.customer_code)
    return payload


async def mirror_user(db: AsyncSession, writer: MirrorWriter, customer_code: str, object_id: str,
                      *, now: datetime | None = None, tracker: ChangeTracker | None = None) -> bool:
    """Write one person's row; False when nothing was written (no active record, or the
    tracker says the row is unchanged and fresh).

    (an active person) -> True, the row is put ; (unknown or deactivated) -> False
    """
    record = await access.access_record_for(db, customer_code, object_id)
    if record is None or not record.is_active:
        return False
    now = now or datetime.now(timezone.utc)
    # With a tracker, an unchanged row is not written again until it is old
    row = envelope(ROW_TYPE, customer_code, user_id=object_id,
                   payload=await user_payload(db, record), as_of=now.isoformat(),
                   now=now.timestamp())
    return await write_if_changed(writer, tracker, row, now=now.timestamp())


async def sweep_once(writer: MirrorWriter, *, only: list[str] | None = None,
                     tracker: ChangeTracker | None = None, session_factory=None,
                     now: datetime | None = None) -> dict[str, int]:
    """Write the row of everyone who has a preference of their own, through their role or
    through their customer; rows written per customer.

    A row changes without a command when an Admin changes a person's sites or role (a saved
    default site is narrowed again) or when rows are converted, so the consumer runs this on
    its tick; the tracker keeps an unchanged row from being written again.
    """
    from app.config.database import async_session

    factory = session_factory or async_session
    written: dict[str, int] = {}
    async with factory() as db:
        where = [] if only is None else [Preference.customer_code.in_(only)]
        subjects = (await db.execute(select(
            Preference.customer_code, Preference.subject_kind, Preference.subject_id
        ).where(*where).distinct())).all()
        by_customer: dict[str, tuple[set[str], set[str]]] = {}
        everyone: set[str] = set()
        for customer, kind, subject in subjects:
            people, roles = by_customer.setdefault(customer, (set(), set()))
            if kind == areas.CUSTOMER:
                everyone.add(customer)  # a value of the customer moves every person's row
            else:
                (people if kind == areas.USER else roles).add(subject)
        for customer, (people, roles) in sorted(by_customer.items()):
            try:
                if roles or customer in everyone:
                    people |= {u.object_id for u in await access.list_users(db, customer)
                               if u.object_id and (customer in everyone or u.role_code in roles)}
                written[customer] = sum([
                    int(await mirror_user(db, writer, customer, person, now=now, tracker=tracker))
                    for person in sorted(people)])
            except Exception:
                logger.exception("preference rows for %s not mirrored", customer)
    return written
