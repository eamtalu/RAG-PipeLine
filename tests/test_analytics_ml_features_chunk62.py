"""Chunk 62 (M1 of docs/analytics-ml-architecture/final_architecture.md): a training set that can be
rebuilt identically, months later.

This is the chunk F10 was written for. Its whole claim was that the fact ledger "must exist from day one
rather than being added when ML starts", because `analytics_facts` holds only the LATEST version of each
fact - so a training set built from it stops being reproducible the moment anything is restated, and
Stage 2 restates constantly.

The acceptance criterion is the plan's own Phase 1 test 11:

    build a training set at a revision, restate a fact, rebuild at the same revision, assert identical
    output

which is `test_a_training_set_survives_a_restatement` below.

The coordinate is an INSTANT, and that corrects the plan
-------------------------------------------------------
There is no revision to pin to. `analytics_fact_ledger.revision` is PER FACT - measured 1..2 per
transaction on live data - and the ledger carries no tenant-level revision at all. A per-fact counter is
not a global coordinate.

`recorded_at` is one, because a fold stamps every ledger row it writes with the same instant. So a fold
is atomic in those terms and any instant is a clean cut BETWEEN folds, never through one.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFactLedger
from app.persistence.models.analytics_ml import AnalyticsFeatureSet, AnalyticsPrediction
from app.persistence.models.consumer_cursor import ConsumerCursor
from app.services.analytics_ml import features as ml

CC = "test_chunk62"
T1 = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 8, 18, 11, 0, tzinfo=timezone.utc)
T3 = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
EVENT = datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsFactLedger, AnalyticsFeatureSet, AnalyticsPrediction):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer == ml.CONSUMER))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _ledger(txn_id, *, recorded_at, revision, quantity, reason="insert", item="101978"):
    """One ledger version of one fact."""
    async with async_session() as db:
        db.add(AnalyticsFactLedger(
            id=uuid.uuid4(), customer_code=CC, recorded_at=recorded_at, reason=reason,
            source_transaction_id=txn_id, source_started_at=EVENT,
            source_version_hash="h" * 8, revision=revision,
            event_time=EVENT, business_date=EVENT.date(), item_number=item,
            method="ConfirmPickLine", transaction_name="Pick", status="success",
            quantity=Decimal(quantity), quantity_classification="pick", attributes={}))
        await db.commit()


# =============================================================== 1. reading as of an instant
async def test_a_pin_selects_the_version_that_was_current_then():
    """The mechanism the whole chunk rests on: `analytics_facts` would give the LATEST value, and the
    latest value is the wrong answer for a training set pinned to the past."""
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    await _ledger(txn, recorded_at=T2, revision=2, quantity="99", reason="update")

    async with async_session() as db:
        at_t1 = await ml.as_of(db, CC, T1)
        at_t2 = await ml.as_of(db, CC, T2)
    assert [r["quantity"] for r in at_t1] == [Decimal("10")]
    assert [r["quantity"] for r in at_t2] == [Decimal("99")]


async def test_a_pin_before_anything_returns_nothing():
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T2, revision=1, quantity="10")
    async with async_session() as db:
        assert await ml.as_of(db, CC, T1) == []


async def test_one_row_per_transaction_not_one_per_version():
    """A training set with a row per version would weight a frequently-restated transaction more heavily
    than a stable one - which is a bias introduced by Stage 2's write pattern rather than by anything in
    the warehouse."""
    txn = uuid.uuid4()
    for rev, ts in ((1, T1), (2, T2), (3, T3)):
        await _ledger(txn, recorded_at=ts, revision=rev, quantity=str(rev), reason="update")
    async with async_session() as db:
        rows = await ml.as_of(db, CC, T3)
    assert len(rows) == 1
    assert rows[0]["quantity"] == Decimal("3")


async def test_a_reversed_fact_is_excluded():
    """A reversal is recorded as a version like any other, so a transaction whose newest version at the
    pin is a reversal DID NOT EXIST then. Including it would train a model on rows the system had already
    retracted."""
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    await _ledger(txn, recorded_at=T2, revision=2, quantity="10", reason="reverse")
    async with async_session() as db:
        assert len(await ml.as_of(db, CC, T1)) == 1, "it existed at T1"
        assert await ml.as_of(db, CC, T2) == [], "it was retracted by T2"


async def test_a_same_instant_tie_breaks_on_revision():
    """Two versions sharing a `recorded_at` is vanishingly unlikely and not impossible. Resolving by
    whichever row the planner returned first would make the same pin hash differently on a different
    day."""
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    await _ledger(txn, recorded_at=T1, revision=2, quantity="20", reason="update")
    async with async_session() as db:
        rows = await ml.as_of(db, CC, T1)
    assert [r["quantity"] for r in rows] == [Decimal("20")]


async def test_the_read_is_ordered_deterministically():
    """`content_hash` digests the rows AS ORDERED, so a planner-dependent order would make the same pin
    hash differently on a different day."""
    ids = sorted(uuid.uuid4() for _ in range(5))
    for i, txn in enumerate(reversed(ids)):
        await _ledger(txn, recorded_at=T1, revision=1, quantity=str(i))
    async with async_session() as db:
        a = [r["source_transaction_id"] for r in await ml.as_of(db, CC, T1)]
        b = [r["source_transaction_id"] for r in await ml.as_of(db, CC, T1)]
    assert a == b == ids


# =============================================================== 2. reproducibility
async def test_a_training_set_survives_a_restatement():
    """THE acceptance criterion, from the plan's Phase 1 test 11: build at a pin, restate a fact, rebuild
    at the same pin, assert identical output.

    This is the property `analytics_fact_ledger` was created for, and the reason F10 insisted it exist
    before ML did. Without it, Stage 2's constant restatement would silently change what a stored
    training set means.
    """
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")

    async with async_session() as db:
        built, rows = await ml.build(db, CC, name="consumption", pinned_at=T1)
        await db.commit()
        stored_hash, stored_rows = built.content_hash, built.row_count
    assert stored_rows == 1

    # restate it - exactly what Stage 2 does constantly
    await _ledger(txn, recorded_at=T2, revision=2, quantity="9999", reason="update")

    async with async_session() as db:
        ok, stored, rebuilt = await ml.verify(db, CC, built)
    assert ok, f"the training set is NOT reproducible: stored {stored}, rebuilt {rebuilt}"
    assert rebuilt == stored_hash


async def test_a_later_pin_genuinely_sees_the_restatement():
    """The complement. If every pin gave the same answer, reproducibility would be trivially satisfied by
    a function that ignores its argument."""
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    await _ledger(txn, recorded_at=T2, revision=2, quantity="9999", reason="update")
    async with async_session() as db:
        a, _ = await ml.build(db, CC, name="a", pinned_at=T1)
        b, _ = await ml.build(db, CC, name="b", pinned_at=T2)
        await db.commit()
    assert a.content_hash != b.content_hash


async def test_rebuilding_the_same_set_returns_the_stored_one():
    """Idempotent by IDENTITY, not by luck. A training run that produced a new row every time it was
    repeated would make "the model was trained on feature set X" meaningless."""
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    async with async_session() as db:
        first, _ = await ml.build(db, CC, name="consumption", pinned_at=T1)
        await db.commit()
        first_id = first.id
    async with async_session() as db:
        again, _ = await ml.build(db, CC, name="consumption", pinned_at=T1)
        await db.commit()
    assert again.id == first_id
    async with async_session() as db:
        n = len((await db.execute(select(AnalyticsFeatureSet).where(
            AnalyticsFeatureSet.customer_code == CC))).scalars().all())
    assert n == 1


def test_the_code_version_is_in_the_hash():
    """The same facts through different code are a DIFFERENT training set. Without this, a model trained
    last month and one trained today would both claim to have used "v1 at time T"."""
    rows = [{"item_number": "1", "quantity": Decimal("10")}]
    before = ml.content_hash(rows)
    original = ml.CODE_VERSION
    try:
        ml.CODE_VERSION = "v2"
        assert ml.content_hash(rows) != before
    finally:
        ml.CODE_VERSION = original


def test_row_order_changes_the_hash():
    """A training set whose row order varies is not reproducible in any sense a model cares about - two
    builds must produce the same FILE, not merely the same multiset."""
    a = [{"item_number": "1"}, {"item_number": "2"}]
    assert ml.content_hash(a) != ml.content_hash(list(reversed(a)))


def test_decimal_scale_does_not_change_the_hash():
    """`10` and `10.0` are the same quantity. Treating them as different would make a set fail its own
    verification for a formatting difference."""
    assert ml.content_hash([{"quantity": Decimal("10.0")}]) == \
        ml.content_hash([{"quantity": Decimal("10.000")}])


def test_all_three_canonicalisers_agree():
    """Stage 2's, analytics' and ML's are separate copies on purpose - each belongs to a different
    contract - so a change to one must not silently move the others. Asserted equal rather than shared,
    so a divergence is caught."""
    from app.services.analytics import normalizer as n2
    from app.services.mnp_log_ingestion.pipeline import fingerprints as fp
    for value in (None, True, 7, "x", Decimal("10.0"), Decimal("10.000"), T1, T1.date()):
        assert ml._canonical(value) == n2._canonical(value) == fp._canonical(value), \
            f"canonicalisers diverged on {value!r}"


# =============================================================== 3. the retention cursor
async def test_building_registers_the_reserved_cursor():
    """`consumer_cursors` is what stops the partition worker dropping source data a lagging reader has
    not seen. A pipeline that read the ledger WITHOUT registering would have its history dropped from
    under it, and its cursor would move past the gap without noticing."""
    assert ml.CONSUMER == "ml:features-v1", "the name reserved in the architecture document"
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    async with async_session() as db:
        await ml.build(db, CC, name="consumption", pinned_at=T1)
        await db.commit()
        pos = await db.scalar(select(ConsumerCursor.position).where(
            ConsumerCursor.consumer == ml.CONSUMER))
    assert pos == T1, "the position must be the PIN, not `now` - nothing after it has been read"


# =============================================================== 4. bounds
async def test_an_oversized_training_set_raises_rather_than_truncating():
    """CLAUDE.md rule 3 applies here more than anywhere: a silently truncated training set trains a model
    on a subset nobody chose, and it looks fine until it is wrong in production."""
    for _ in range(3):
        await _ledger(uuid.uuid4(), recorded_at=T1, revision=1, quantity="1")
    async with async_session() as db:
        with pytest.raises(ValueError, match="exceeds 2 rows"):
            await ml.as_of(db, CC, T1, limit=2)


async def test_latest_pin_is_the_ledgers_high_water_mark():
    """Offered because the natural mistake is to pin to `now()`, which can select a moment a fold is
    part-way through writing. Pinning to what has actually been recorded cannot straddle a fold."""
    await _ledger(uuid.uuid4(), recorded_at=T1, revision=1, quantity="1")
    await _ledger(uuid.uuid4(), recorded_at=T3, revision=1, quantity="1")
    async with async_session() as db:
        assert await ml.latest_pin(db, CC) == T3


async def test_one_tenants_ledger_is_not_anothers():
    await _ledger(uuid.uuid4(), recorded_at=T1, revision=1, quantity="1")
    async with async_session() as db:
        assert len(await ml.as_of(db, CC, T1)) == 1
        assert await ml.as_of(db, "test_chunk62_other", T1) == []


# =============================================================== 5. predictions
async def test_a_prediction_carries_its_lineage():
    """A prediction whose training data cannot be identified cannot be explained when it is wrong."""
    txn = uuid.uuid4()
    await _ledger(txn, recorded_at=T1, revision=1, quantity="10")
    async with async_session() as db:
        fs, _ = await ml.build(db, CC, name="consumption", pinned_at=T1)
        db.add(AnalyticsPrediction(
            id=uuid.uuid4(), customer_code=CC, subject="101978", subject_kind="item_number",
            horizon="7d", model_version="m1", target_at=T3, predicted_at=T1,
            value=Decimal("42"), feature_set_id=fs.id))
        await db.commit()
        got = await db.scalar(select(AnalyticsPrediction).where(
            AnalyticsPrediction.customer_code == CC))
    assert got.feature_set_id == fs.id


async def test_two_model_versions_can_predict_the_same_subject():
    """Two models disagreeing about the same subject is the comparison a model version exists FOR, not a
    conflict to be rejected."""
    async with async_session() as db:
        for version in ("m1", "m2"):
            db.add(AnalyticsPrediction(
                id=uuid.uuid4(), customer_code=CC, subject="101978", horizon="7d",
                model_version=version, target_at=T3, predicted_at=T1, value=Decimal("42")))
        await db.commit()
        n = len((await db.execute(select(AnalyticsPrediction).where(
            AnalyticsPrediction.customer_code == CC))).scalars().all())
    assert n == 2


async def test_the_same_model_cannot_predict_the_same_thing_twice():
    """Otherwise a re-run would accumulate duplicates and every read would have to guess which is
    current."""
    from sqlalchemy.exc import IntegrityError
    async with async_session() as db:
        for _ in range(2):
            db.add(AnalyticsPrediction(
                id=uuid.uuid4(), customer_code=CC, subject="101978", horizon="7d",
                model_version="m1", target_at=T3, predicted_at=T1, value=Decimal("42")))
        with pytest.raises(IntegrityError):
            await db.commit()
