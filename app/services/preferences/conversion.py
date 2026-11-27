"""One-off: what people saved under the two old Advisories areas becomes one saved view.

Until chunk 242 the Advisories layout (`advisories_view`) and the kinds and severities wanted
first (`advisories_focus`) were two areas saved field by field. A page now has named views
saved whole, so a person's old rows become one view called "My view", which the page opens on.
Used by `scripts/convert_old_advisory_preferences.py`; nothing in the serving path imports it.
"""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.preference import Preference

OLD_AREAS = ("advisories_view", "advisories_focus")
CONVERTED_ID, CONVERTED_NAME = "my-view", "My view"


def advisories_from_old(*, view: dict | None, focus: dict | None) -> dict | None:
    """The new page preference for what one person had saved, or None when they had nothing.

    >>> advisories_from_old(view={"group_by": "area"}, focus=None)["views"][0]["settings"]
    {'group_by': 'area'}
    >>> advisories_from_old(view=None, focus={}) is None
    True
    """
    settings = {**(view or {}), **(focus or {})}
    if not settings:
        return None
    return {"use": CONVERTED_ID,
            "views": [{"id": CONVERTED_ID, "name": CONVERTED_NAME, "settings": settings}]}


async def convert_old_advisories(db: AsyncSession, *, customer_code: str | None = None) -> int:
    """Convert every subject's old rows and remove them; how many subjects got a view.

    (one person with an old layout row) -> 1, their `advisories` row holds "My view"
    ; (run again) -> 0. A subject who already has an `advisories` row keeps it.
    """
    # Step 1: Gather the old rows per subject, because a person may have one or both
    where = [Preference.area.in_(OLD_AREAS)]
    if customer_code is not None:
        where.append(Preference.customer_code == customer_code)
    old = (await db.execute(select(Preference).where(*where))).scalars().all()
    by_subject: dict[tuple[str, str, str], dict[str, Preference]] = {}
    for row in old:
        key = (row.customer_code, row.subject_kind, row.subject_id)
        by_subject.setdefault(key, {})[row.area] = row
    converted = 0
    for (customer, kind, subject), rows in by_subject.items():
        # Step 2: Never overwrite a view saved with the new page, which is the newer choice
        has_new = await db.scalar(select(Preference.id).where(
            Preference.customer_code == customer, Preference.subject_kind == kind,
            Preference.subject_id == subject, Preference.area == "advisories"))
        view, focus = rows.get("advisories_view"), rows.get("advisories_focus")
        page = advisories_from_old(view=view.settings if view else None,
                                   focus=focus.settings if focus else None)
        if page is not None and not has_new:
            newest = max(rows.values(), key=lambda r: r.updated_at)
            db.add(Preference(customer_code=customer, subject_kind=kind, subject_id=subject,
                              area="advisories", settings=page, updated_at=newest.updated_at,
                              updated_by=newest.updated_by))
            converted += 1
    # Step 3: Remove the old rows either way, since no code reads them any more
    await db.execute(delete(Preference).where(*where))
    return converted
