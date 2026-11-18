"""Flag picked-case labels that were printed before their item was picked.

Used by the transaction feed and the transaction detail view (`app/api/v1/logs.py`) so both render
the same warning line under the REQUEST of an early `PrintPickedCaseLabel`.

Why it exists: on 17 Nov 2026 a Bulk Pick picker printed labels on the item screen before confirming
the pick, and confirming then printed the same item again. The spare labels were torn off for the
next item, so later cases got the wrong label. The check is computed at read time, not at ingest,
because stitching is asynchronous and can arrive out of order; a read-time answer is always current
and needs no backfill.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, TIMESTAMP, UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import String

from app.persistence.models.log_transaction import LogTransaction

CASE_LABEL_METHOD = "PrintPickedCaseLabel"
PICK_METHOD = "ConfirmPickLine"
# A Bulk Pick item takes a few minutes; 30 minutes is well past any real gap between a pick and its
# label, and short enough that yesterday's picks never clear today's print.
PICK_LOOKBACK = timedelta(minutes=30)


@dataclass(frozen=True)
class EarlyLabelPrint:
    """A case label printed while its item was not yet picked, with the pick that came before it."""

    item_number: str
    last_pick_item: str | None
    last_pick_at: datetime | None


def is_case_label_print(txn: LogTransaction) -> bool:
    """True for a Bulk Pick case-label print that can be checked; reprints are deliberate.

    >>> from datetime import datetime, timezone
    >>> from app.persistence.models.log_transaction import LogTransaction as T
    >>> at = datetime(2026, 11, 17, 13, 19, tzinfo=timezone.utc)
    >>> is_case_label_print(T(method="PrintPickedCaseLabel", item_number="1", user_name="U",
    ...                       started_at=at, transaction_name="Bulk Pick (Main)"))
    True
    >>> is_case_label_print(T(method="PrintPickedCaseLabel", item_number="1", user_name="U",
    ...                       started_at=at, transaction_name="Reprint PICK Label"))
    False
    """
    return (
        txn.method == CASE_LABEL_METHOD
        and bool(txn.item_number)
        and bool(txn.user_name)
        and txn.started_at is not None
        and not (txn.transaction_name or "").lower().startswith("reprint")
    )


# One round trip for the whole page: each candidate print looks up its own user's latest pick in the
# lookback window. The constant :window_lo/:window_hi bound lets the planner prune to the page's
# one or two day partitions; without it planning walks all 60 partitions (80 ms warm vs 1 ms).
_LAST_PICK_SQL = text(
    """
    SELECT p.id, lp.item_number, lp.started_at
    FROM unnest(:ids, :users, :starts) AS p(id, user_name, started_at)
    LEFT JOIN LATERAL (
        SELECT c.item_number, c.started_at
        FROM log_transactions c
        WHERE c.customer_code = :cc
          AND c.method = :pick_method
          AND c.user_name = p.user_name
          AND c.started_at >= :window_lo
          AND c.started_at < :window_hi
          AND c.started_at < p.started_at
          AND c.started_at >= p.started_at - CAST(:lookback AS interval)
        ORDER BY c.started_at DESC
        LIMIT 1
    ) lp ON true
    """
).bindparams(
    bindparam("ids", type_=ARRAY(UUID(as_uuid=True))),
    bindparam("users", type_=ARRAY(String)),
    bindparam("starts", type_=ARRAY(TIMESTAMP(timezone=True))),
    bindparam("window_lo", type_=TIMESTAMP(timezone=True)),
    bindparam("window_hi", type_=TIMESTAMP(timezone=True)),
)


async def load_early_label_prints(
    db: AsyncSession | None, customer_code: str, txns: list[LogTransaction]
) -> dict[uuid.UUID, EarlyLabelPrint]:
    """Map each early case-label print on this page to what was picked just before it.
    ([beer pick, beer print, apple print, apple pick]) -> {apple print: last pick beer} ; none -> {}
    """
    # Step 1: Keep only checkable case-label prints, so a page without them costs no query at all.
    candidates = [t for t in txns if is_case_label_print(t)]
    if not candidates:
        return {}

    # Step 2: Fetch each print's latest pick by the same user in one round trip, because the pick
    # that clears a print may sit on another page or be hidden by the feed's filters.
    starts = [t.started_at for t in candidates]
    rows = (await db.execute(_LAST_PICK_SQL, {
        "ids": [t.id for t in candidates],
        "users": [t.user_name for t in candidates],
        "starts": starts,
        "window_lo": min(starts) - PICK_LOOKBACK,
        "window_hi": max(starts),
        "cc": customer_code,
        "pick_method": PICK_METHOD,
        "lookback": PICK_LOOKBACK,
    })).all()
    last_pick = {row[0]: (row[1], row[2]) for row in rows}

    # Step 3: Flag a print whose latest pick is a different item or missing, because that label
    # came out before its own item was confirmed.
    early: dict[uuid.UUID, EarlyLabelPrint] = {}
    for t in candidates:
        pick_item, pick_at = last_pick.get(t.id, (None, None))
        if pick_item != t.item_number:
            early[t.id] = EarlyLabelPrint(item_number=t.item_number, last_pick_item=pick_item,
                                          last_pick_at=pick_at)
    return early
