"""Reading and writing preference rows, and resolving them for one person.

Resolution is field by field: the area's built-in default, then the role's row, then the
person's own. The result is narrowed by the person's access record every time it is read, so a
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


async def save(db: AsyncSession, customer_code: str, subject_kind: str, subject_id: str,
               area: str, settings: dict, *, by: str | None, now: datetime | None = None) -> dict:
    """Set some fields of one subject's area, keeping the fields not named; what is now stored.

    (user u-7, "advisories_view", {"sort_by": "due"}) -> {"group_by": "area", "sort_by": "due"}
    when group_by was stored before ; (a wrong value) -> ValueError naming the field
    """
    if subject_kind not in SUBJECT_KINDS:
        raise ValueError(f"a preference belongs to a role or a user, not {subject_kind!r}")
    checked = areas.validate(area, settings)
    stored = await _own(db, customer_code, subject_kind, subject_id)
    merged = {**stored.get(area, {}), **checked}
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
    """Only what the person set themselves, by area: {"advisories_view": {"sort_by": "due"}}."""
    return await _own(db, record.customer_code, "user", record.object_id)


async def resolve(db: AsyncSession, record: access.AccessRecord) -> dict[str, dict]:
    """Every area's values for one person, complete and inside their access.

    (role says group by area, the person says sort by due) ->
    {"advisories_view": {"group_by": "area", "sort_by": "due", "reversed": False}, "app": {...}}
    """
    by_role = await _own(db, record.customer_code, "role", record.role_code)
    by_user = await _own(db, record.customer_code, "user", record.object_id)
    resolved = {area: {**areas.defaults(area), **by_role.get(area, {}), **by_user.get(area, {})}
                for area in areas.AREAS}
    resolved["app"]["default_site"] = usable_site(resolved["app"]["default_site"], record)
    return resolved


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
    return {area: {k: v for k, v in (settings or {}).items() if k in areas.defaults(area)}
            for area, settings in rows if area in areas.AREAS}
