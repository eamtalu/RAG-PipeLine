"""M1. Building a training set that can be rebuilt identically, months later.

The one property this module exists for
---------------------------------------
Read the facts AS OF an instant, from the LEDGER, not from `analytics_facts`.

`analytics_facts` holds only the latest version of each fact, so a training set built from it stops
being reproducible the moment anything is restated - and Stage 2 restates constantly. The ledger holds
every version, which is exactly why F10 insisted it exist "from day one rather than being added when ML
starts". This is the code that finally uses it.

The coordinate is an INSTANT, and that is a correction
------------------------------------------------------
The plan says "the pinned revision". There is no such thing to pin to:
`analytics_fact_ledger.revision` is PER FACT - measured 1..2 per transaction - and the ledger carries no
tenant-level revision at all. A per-fact counter is not a global coordinate.

`recorded_at` is. A fold stamps every ledger row it writes with the same instant, so a fold is atomic in
those terms and any instant is a clean cut between folds. Ties break on `revision` descending, so even a
same-microsecond collision resolves deterministically rather than by whichever row the planner returned
first.

Why the rows are not stored
---------------------------
A feature set stores its pin, its code version and a CONTENT HASH - not its rows. The rows are a pure
function of `(pinned_at, code_version)` over the ledger, so storing them would be a second copy that can
disagree with the first, and it would be the largest table in the system.

The hash is what makes the reproducibility claim testable rather than asserted: rebuild at the same pin,
recompute the hash, compare. `verify` does exactly that.
"""

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFactLedger
from app.persistence.models.analytics_ml import AnalyticsFeatureSet
from app.services.analytics import contract as c
from app.services import consumer_cursors

logger = logging.getLogger(__name__)

#: The name this pipeline publishes its retention position under. Reserved in the architecture document
#: from the start; this is the code that finally registers it.
#:
#: Registering matters for a reason that is easy to miss: `consumer_cursors` is what stops the partition
#: worker dropping source data a lagging reader has not seen. An ML pipeline that read the ledger without
#: registering would have its history dropped from under it, and its cursor would move past the gap
#: without noticing.
CONSUMER = "ml:features-v1"

#: Bumped when the transformation below changes. The same facts through different code are a DIFFERENT
#: training set and must not share an identity - otherwise a model trained last month and one trained
#: today would both claim to have used "feature set v1 at time T".
CODE_VERSION = "v1"

#: The features this version produces, in order. Part of the stored set because a training set whose
#: columns cannot be identified is not a training set, and because a reader has to be able to tell a
#: schema change from a data change.
FEATURE_NAMES: tuple[str, ...] = (
    "business_date", "item_number", "method", "transaction_name", "warehouse", "user_name",
    "quantity", "quantity_classification", "status", "duration_ms",
)

#: A hard bound, because a training set is exactly the kind of read that is unbounded by default and
#: CLAUDE.md rule 3 applies to it more than to anything else here. Exceeding it RAISES rather than
#: truncating: a silently truncated training set trains a model on a subset nobody chose.
MAX_ROWS = 500_000


def _canonical(value: Any) -> Any:
    """One value in a form that hashes identically across two builds of the same pinned set.

    Deliberately the same rules as `analytics/normalizer._canonical` and `pipeline/fingerprints`, and
    deliberately a third copy: this one belongs to the ML contract. A test asserts all three agree, so a
    divergence is caught rather than hoped against.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return f"{value.normalize():f}"
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "value"):
        return _canonical(value.value)
    return str(value)


def content_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    """A digest over the training set's contents.

    Over the ROWS AS ORDERED, because a training set whose row order varies is not reproducible in any
    sense a model cares about - two builds must produce the same file, not merely the same multiset.
    `as_of` therefore orders explicitly rather than relying on the planner.

    `CODE_VERSION` and `FEATURE_NAMES` are in the digest as well, so a set built by different code or
    with different columns cannot collide with this one even if every value happens to match.
    """
    payload = {
        "code_version": CODE_VERSION,
        "features": list(FEATURE_NAMES),
        "rows": [[_canonical(r.get(f)) for f in FEATURE_NAMES] for r in rows],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=False, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


async def as_of(db: AsyncSession, customer_code: str, pinned_at: datetime,
                limit: int = MAX_ROWS) -> list[dict]:
    """Every fact as it stood at `pinned_at`, one row per transaction, from the LEDGER.

    For each `source_transaction_id`, the row with the greatest `(recorded_at, revision)` at or before
    the pin. `DISTINCT ON` does that in one pass; the alternative - a correlated subquery per
    transaction - is the shape that turns a training-set build into an outage.

    A REVERSED fact is excluded. The ledger records a reversal as a version like any other, so a
    transaction whose newest version at the pin is a reversal did not exist then and must not appear -
    including it would train a model on rows the system had already retracted.

    Ordered explicitly and deterministically, because `content_hash` digests the rows AS ORDERED and a
    planner-dependent order would make the same pin hash differently on a different day.
    """
    if pinned_at.tzinfo is None:
        pinned_at = pinned_at.replace(tzinfo=timezone.utc)

    newest = (
        select(AnalyticsFactLedger)
        .where(AnalyticsFactLedger.customer_code == customer_code,
               AnalyticsFactLedger.recorded_at <= pinned_at)
        .order_by(AnalyticsFactLedger.source_transaction_id,
                  AnalyticsFactLedger.recorded_at.desc(),
                  AnalyticsFactLedger.revision.desc())
        .distinct(AnalyticsFactLedger.source_transaction_id)
    ).subquery()

    rows = (await db.execute(
        select(newest)
        .where(newest.c.reason != "reverse")
        .order_by(newest.c.source_transaction_id)
        .limit(limit + 1))).mappings().all()

    if len(rows) > limit:
        # RAISES rather than truncating. A silently truncated training set trains a model on a subset
        # nobody chose, and the model looks fine until it is wrong in production.
        raise ValueError(
            f"the training set for {customer_code} at {pinned_at.isoformat()} exceeds {limit} rows. "
            f"Narrow the pin or raise MAX_ROWS deliberately - it is not truncated, because a model "
            f"trained on an unchosen subset is worse than a build that refused.")

    return [{f: r.get(f) for f in FEATURE_NAMES} | {"source_transaction_id": r["source_transaction_id"]}
            for r in rows]


async def build(db: AsyncSession, customer_code: str, *, name: str,
                pinned_at: datetime) -> tuple[AnalyticsFeatureSet, list[dict]]:
    """Build (or return) the training set for `(name, pinned_at, CODE_VERSION)`. Does NOT commit.

    Idempotent by identity rather than by luck: the unique constraint makes those three a complete
    identity, so re-requesting the same set returns the STORED one instead of building a second. A
    training run that produced a new row every time it was repeated would make "the model was trained on
    feature set X" meaningless.

    Returns the record and the rows, because the caller needs the rows to train on and re-reading them
    would be a second query for something already in memory.
    """
    existing = await db.scalar(
        select(AnalyticsFeatureSet).where(
            AnalyticsFeatureSet.customer_code == customer_code,
            AnalyticsFeatureSet.name == name,
            AnalyticsFeatureSet.pinned_at == pinned_at,
            AnalyticsFeatureSet.code_version == CODE_VERSION))
    rows = await as_of(db, customer_code, pinned_at)
    if existing is not None:
        return existing, rows

    record = AnalyticsFeatureSet(
        id=uuid.uuid4(), customer_code=customer_code, name=name, pinned_at=pinned_at,
        code_version=CODE_VERSION, content_hash=content_hash(rows), row_count=len(rows),
        feature_names=list(FEATURE_NAMES), built_at=datetime.now(timezone.utc))
    db.add(record)
    await db.flush()

    # Publish the retention position, so the partition worker cannot drop ledger history this pipeline
    # still needs. The position is the PIN, not "now": everything strictly before it has been consumed,
    # and anything after it has not been looked at yet.
    await consumer_cursors.report(db, CONSUMER, position=pinned_at)
    logger.info("ML: built feature set %r for %s at %s - %d rows, hash %s",
                name, customer_code, pinned_at.isoformat(), len(rows), record.content_hash[:12])
    return record, rows


async def verify(db: AsyncSession, customer_code: str,
                 feature_set: AnalyticsFeatureSet) -> tuple[bool, str, str]:
    """Rebuild a stored feature set at its own pin and compare. Returns `(ok, stored, rebuilt)`.

    This is the acceptance criterion the plan states as Phase 1 test 11 - "build a training set at a
    revision, restate a fact, rebuild at the same revision, assert identical output" - made runnable at
    any time rather than only in a test.

    Worth having in production and not only in the suite: it is the one check that can tell a
    reproducibility guarantee has quietly stopped holding, which would otherwise surface as two models
    that disagree for no visible reason.
    """
    rebuilt = content_hash(await as_of(db, customer_code, feature_set.pinned_at))
    ok = rebuilt == feature_set.content_hash
    if not ok:
        logger.error("ML: feature set %s for %s is NOT reproducible - stored %s, rebuilt %s. Either the "
                     "ledger lost a version or the transformation changed without a CODE_VERSION bump.",
                     feature_set.id, customer_code, feature_set.content_hash[:12], rebuilt[:12])
    return ok, feature_set.content_hash, rebuilt


async def latest_pin(db: AsyncSession, customer_code: str) -> datetime | None:
    """The newest instant there is anything to pin to - the ledger's own high-water mark.

    Offered because the natural mistake is to pin to `now()`, which selects a moment the fold may be
    part-way through writing. Pinning to what has actually been recorded cannot straddle a fold.
    """
    return await db.scalar(
        select(func.max(AnalyticsFactLedger.recorded_at)).where(
            AnalyticsFactLedger.customer_code == customer_code))
