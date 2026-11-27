"""Reading and writing preference rows, and resolving them for one person.

Resolution is field by field: the area's built-in default, then the customer's row, then the
role's, then the person's own, each only where the area may be held at that level. The result is narrowed by the person's access record every time it is read, so a
stored value can never show more than layers 1 and 2 allow. Nothing here commits.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.preference import SUBJECT_KINDS, Preference
from app.services import access
from app.services.preferences import areas
from app.settings import settings as app_settings


async def save(db: AsyncSession, customer_code: str, subject_kind: str, subject_id: str,
               area: str, settings: dict, *, by: str | None, now: datetime | None = None) -> dict:
    """Set some fields of one subject's area, keeping the fields not named; what is now stored.

    (user u-7, "app", {"default_site": "BRI"}) -> {"is_history_open": False, "default_site":
    "BRI"} when the panel was stored before ; (a wrong value) -> ValueError naming the field
    """
    if subject_kind not in SUBJECT_KINDS:
        raise ValueError("a preference belongs to the customer, a role or a user, "
                         f"not {subject_kind!r}")
    stored = await _own(db, customer_code, subject_kind, subject_id)
    # Checked after merging with what is stored, because some fields only make sense together:
    # a page's `use` must name one of its saved views, whichever of the two is being sent.
    merged = areas.validate(area, {**stored.get(area, {}), **(settings or {})}, subject_kind)
    statement = pg_insert(Preference).values(
        customer_code=customer_code, subject_kind=subject_kind, subject_id=subject_id, area=area,
        settings=merged, updated_at=now or datetime.now(timezone.utc), updated_by=by)
    await db.execute(statement.on_conflict_do_update(
        constraint="uq_preferences_subject_area",
        set_={"settings": statement.excluded.settings, "updated_at": statement.excluded.updated_at,
              "updated_by": statement.excluded.updated_by}))
    return merged


async def reset(db: AsyncSession, customer_code: str, subject_kind: str, subject_id: str,
                area: str) -> None:
    """Forget what one subject chose in an area, so the level above shows through again."""
    areas.defaults(area)  # refuses an unknown area
    await db.execute(delete(Preference).where(
        Preference.customer_code == customer_code, Preference.subject_kind == subject_kind,
        Preference.subject_id == subject_id, Preference.area == area))


async def own(db: AsyncSession, record: access.AccessRecord) -> dict[str, dict]:
    """Only what the person set themselves, by area: {"app": {"default_site": "BRI"}}."""
    return await _own(db, record.customer_code, "user", record.object_id)


async def resolve(db: AsyncSession, record: access.AccessRecord) -> dict[str, dict]:
    """Every area's values for one person, complete and inside their access.

    (role says the Ask panel is closed, the person chose site LON) ->
    {"app": {"default_site": "LON", "is_history_open": False}, "advisories": {"use": ...}}
    """
    by_level = {
        areas.CUSTOMER: await _own(db, record.customer_code, "customer", record.customer_code),
        areas.ROLE: await _own(db, record.customer_code, "role", record.role_code),
        areas.USER: await _own(db, record.customer_code, "user", record.object_id)}
    resolved = {}
    for area in areas.AREAS:
        resolved[area] = areas.defaults(area)
        for level in areas.holders(area):  # widest first, so the person's own value wins
            resolved[area].update(by_level[level].get(area, {}))
    resolved["app"]["default_site"] = usable_site(resolved["app"]["default_site"], record)
    # The screen shows a number, so "the server's setting" is resolved to it here
    resolved["ask"]["line_search_days"] = (resolved["ask"]["line_search_days"]
                                           or app_settings.log_search_default_days)
    return resolved


async def page_defaults(db: AsyncSession, record: access.AccessRecord) -> dict[str, dict]:
    """What "Default" shows on each page for one person: their role's page where the role has
    one, else the page as it comes.

    (a picker whose role groups Advisories by location) -> {"advisories": {..., "group_by":
    "location"}} ; (a role with nothing set) -> the built-in pages
    """
    by_role = await _own(db, record.customer_code, "role", record.role_code)
    return {page: {**areas.page_defaults(page), **by_role.get(areas.default_area(page), {})}
            for page in areas.PAGES}


async def roles_with_a_default(db: AsyncSession, customer_code: str) -> dict[str, list[str]]:
    """The roles that have a default of their own, per page: {"advisories": ["picker"]}.

    For the people who set them, so the page can say which roles differ from the built-in one.
    """
    rows = (await db.execute(select(Preference.area, Preference.subject_id).where(
        Preference.customer_code == customer_code, Preference.subject_kind == "role",
        Preference.area.in_(list(areas.ROLE_DEFAULT_AREAS))
    ).order_by(Preference.subject_id))).all()
    found: dict[str, list[str]] = {}
    for area, role in rows:
        found.setdefault(areas.page_of(area), []).append(role)
    return found


async def line_search_days(db: AsyncSession, customer_code: str) -> int:
    """How many days a line search looks back for a customer when a question names no range:
    the customer's own value, else the server's setting.

    (a customer whose Admin chose 10) -> 10 ; (nothing chosen) -> settings.log_search_default_days
    """
    own_value = (await db.execute(select(Preference.settings).where(
        Preference.customer_code == customer_code, Preference.subject_kind == "customer",
        Preference.subject_id == customer_code, Preference.area == "ask"))).scalar_one_or_none()
    return (own_value or {}).get("line_search_days") or app_settings.log_search_default_days


def usable_site(site_id: str | None, record: access.AccessRecord) -> str | None:
    """A chosen default site the person may open, else one of their own, else nothing.

    >>> from app.services.access import AccessRecord
    >>> sam = AccessRecord("c", "u", "Sam", "supervisor", "Supervisor", "active", False, ("BRI",))
    >>> usable_site("BRI", sam), usable_site("LON", sam), usable_site("all", sam)
    ('BRI', 'BRI', 'BRI')
    >>> usable_site(None, sam) is None
    True
    """
    if site_id is None or record.may_see(site_id):
        return site_id
    return record.site_ids[0] if record.site_ids else None


async def _own(db: AsyncSession, customer_code: str, subject_kind: str,
               subject_id: str) -> dict[str, dict]:
    rows = (await db.execute(select(Preference.area, Preference.settings).where(
        Preference.customer_code == customer_code, Preference.subject_kind == subject_kind,
        Preference.subject_id == subject_id))).all()
    # A field the code no longer knows is dropped on the way out, so a retired setting in an
    # old row cannot break the page that reads it.
    known = {**areas.AREAS, **areas.ROLE_DEFAULT_AREAS}
    return {area: {k: v for k, v in (settings or {}).items() if k in areas.defaults(area)}
            for area, settings in rows if area in known}
