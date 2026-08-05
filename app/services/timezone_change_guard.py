"""Blocks a customer timezone change that would corrupt already-ingested data.

`log_entries.timestamp` is derived at parse time by attaching the CUSTOMER's configured timezone to
the naive local wall-clock the parser yields (`parse_insert.py`). Nothing rewrites those instants
afterwards. So changing the configuration once entries exist splits the tenant's timeline in two:
everything ingested before keeps the old derivation, everything after gets the new one, and nothing in
the data records where the seam is.

Partitioning turned that from "wrong-ish" into silent duplication. The dedup key is now
`(customer_code, entry_hash, timestamp)` — `timestamp` had to join it because a unique constraint on a
partitioned table must contain every partition column — so the same raw line re-ingested under a
different zone no longer collides with the row already stored. Measured against the real database:

    identical hash, SAME tz     -> inserted 0   (dedup works)
    identical hash, CHANGED tz  -> inserted 1   (a second row, in a different partition)

Hence a guard rather than after-the-fact duplicate detection: once both rows exist there is no way to
tell which instant was right.

Two changes are deliberately NOT blocked, because blocking them would be noise rather than safety:
setting a zone on a tenant that has no entries yet (the normal post-creation case), and any change
that leaves the EFFECTIVE zone identical — filling in `null -> "Europe/London"` when the global
default already was `Europe/London` changes nothing about how any instant is derived.

This module only decides and explains. The API layer turns a reason into a 409 and owns the override,
which keeps the rule testable without HTTP and keeps the transport concern out of here.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_entry import LogEntry
from app.settings import settings

#: Query parameter an operator sets to proceed anyway. Named in the rejection message, so it lives
#: here beside the rule rather than being duplicated as a literal in the endpoint.
OVERRIDE_PARAM = "allow_mixed_timezones"


def effective_timezone(stored: str | None) -> str:
    """The zone ingestion actually applies: the customer's, or the global default when unset.

    Mirrors `get_customer_timezone`. Comparisons have to be made on the EFFECTIVE value, because
    `null` is not "no timezone" — it is "the global default", and treating it as absent would classify
    a genuine change as harmless.
    """
    return stored or settings.display_timezone


def changes_meaning(stored: str | None, new: str | None) -> bool:
    """Whether moving from `stored` to `new` changes how instants are derived.

    Compared by IANA NAME rather than by resolved offset. Two differently-named zones that happen to
    agree today can diverge on any future DST rule change, so treating them as equivalent would be a
    guess about politics; requiring the override for that rare case is the cheaper mistake.
    """
    return effective_timezone(stored) != effective_timezone(new)


def has_entries_stmt(customer_code: str):
    """Existence check for a tenant's entries — `LIMIT 1`, never a COUNT.

    This runs on an admin path against a partitioned table, so it must short-circuit on the first row
    found rather than aggregate across every partition. Exposed as a statement so a test can EXPLAIN
    it and confirm that is what actually happens.
    """
    return select(LogEntry.id).where(LogEntry.customer_code == customer_code).limit(1)


async def has_entries(db: AsyncSession, customer_code: str) -> bool:
    return (await db.scalar(has_entries_stmt(customer_code))) is not None


async def blocking_reason(db: AsyncSession, *, customer_code: str,
                          stored_tz: str | None, new_tz: str | None) -> str | None:
    """Why this timezone change must not proceed, or None when it is safe.

    Returns prose rather than a boolean because the message is the useful part: a block that does not
    name the safe remedy is just an obstacle. The order of the two checks matters only for cost — the
    cheap in-memory comparison runs first, so the common no-op change never touches the database.
    """
    if not changes_meaning(stored_tz, new_tz):
        return None
    if not await has_entries(db, customer_code):
        return None
    return (
        f"Refusing to change the timezone of {customer_code!r} from "
        f"{effective_timezone(stored_tz)!r} to {effective_timezone(new_tz)!r}: this tenant already has "
        f"ingested log entries. Stored timestamps were derived using the old zone and are NOT "
        f"rewritten, so entries ingested after this change would be derived differently — and because "
        f"the entry-dedup key includes the timestamp, re-ingesting the same file would insert "
        f"DUPLICATE rows rather than being skipped. "
        f"To change it safely: purge this tenant's log data first "
        f"(DELETE /api/v1/logs/data?confirm=true), then set the timezone, then re-ingest. "
        f"To proceed anyway and accept a split timeline, resend with {OVERRIDE_PARAM}=true."
    )
