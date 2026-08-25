"""Chunk 59 (S3 of docs/analytics-ml-architecture/final_architecture.md, section 18): make a Stage 2
write conditional on the row having actually changed.

S3 is the first stage with a real gain - 22.4 writes per surviving row to about 1.05 - and the first
whose failure mode is silent. A row that SHOULD have been rewritten and was not looks exactly like a
row that did not need rewriting, until somebody questions a number months later. So most of this file
is about the ways that can happen.

Two fingerprints, and why one would be wrong
--------------------------------------------
    members_fingerprint   an ORDERED digest over the entry ids
    row_fingerprint       a digest over the transaction's own columns

`entry_count` is not a substitute for the first. Swap one same-timestamped entry between two
transactions and both keep their counts, both keep `started_at` and `ended_at`, and every derived
column is identical - while `log_entry_assignment` now holds the wrong mapping and the rendered
timeline is wrong forever, because nothing will recompute a row whose fingerprint matched.

The split is also what produces the gain. Sealing changes the ROW and not the MEMBERS, so it touches
`log_transactions` alone and never rewrites an assignment row.

_DERIVE_VERSION is the one that is easy to leave out
---------------------------------------------------
Edit the derivation without bumping it and stored rows keep matching their own stale fingerprint, so
the edited logic never reaches them. No failing test, no alert, and the projection is quietly wrong
forever. The source-digest test below is what makes that impossible rather than merely discouraged.
"""

import hashlib
import inspect
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.persistence.models.log_transaction import LogTransactionStatus
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.mnp_log_ingestion.pipeline import fingerprints as fp

T0 = datetime(2026, 8, 15, 11, 0, tzinfo=timezone.utc)


#: FIXED, not generated per call. `job_id` is a real column Stage 2 writes, so it is correctly IN the
#: digest - which meant a helper that minted a fresh uuid each call made every two-call comparison
#: differ and nine tests fail for a reason that had nothing to do with what they were testing.
_JOB = uuid.UUID("00000000-0000-4000-8000-000000000c59")


def _values(**kw):
    base = {
        "customer_code": "c59", "job_id": _JOB,
        "started_at": T0, "ended_at": T0 + timedelta(seconds=2), "date": T0.date(),
        "duration_ms": 2000, "status": LogTransactionStatus.success,
        "method": "ConfirmPickLine", "transaction_name": "Full Stock Count",
        "user_name": "amin", "entry_count": 3, "attributes": {"QuantityPicked": Decimal("10.0")},
    }
    base.update(kw)
    return base


# =============================================================== 1. the members digest
def test_the_members_digest_is_ordered_over_entry_ids():
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    assert fp.members([a, b, c]) == fp.members([a, b, c])


def test_the_members_digest_is_stable_across_arrival_order():
    """Sorted before hashing, because arrival order is an artefact of which query returned the rows
    while membership is a property of the transaction. Two reads of the same unchanged transaction must
    not disagree just because the planner chose a different scan."""
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    assert fp.members([a, b, c]) == fp.members([c, a, b])


def test_swapping_one_entry_changes_the_members_digest():
    """THE case `entry_count` cannot catch. Swap one same-timestamped entry between two transactions and
    counts, timestamps and every derived column stay identical - while the assignment mapping is wrong
    forever, because nothing recomputes a row whose fingerprint matched."""
    a, b, c, d = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    before = fp.members([a, b, c])
    after = fp.members([a, b, d])
    assert before != after
    assert len([a, b, c]) == len([a, b, d]), "the counts are equal, which is the point"


def test_an_empty_membership_has_a_digest():
    assert isinstance(fp.members([]), str) and len(fp.members([])) == 64


# =============================================================== 2. the row digest
def test_the_row_digest_is_stable_for_an_unchanged_row():
    v = _values()
    assert fp.row(v, sealed=False, tenant_timezone="Europe/London") == \
        fp.row(_values(), sealed=False, tenant_timezone="Europe/London")


def test_sealing_changes_the_row_digest():
    """`sealed` MUST be in the hash. Without it the sealer's UPDATE is invisible to the diff and S1's
    fix silently stops working the moment S3 ships - which would be the worst kind of regression, since
    S1 is what made the alert fire in the first place."""
    v = _values()
    assert fp.row(v, sealed=False, tenant_timezone="UTC") != fp.row(v, sealed=True, tenant_timezone="UTC")


def test_the_tenant_timezone_changes_the_row_digest():
    """`date` is derived through the zone, so a zone change genuinely changes which day the row rolls
    up into. Passed in rather than read, which is what keeps the module pure."""
    v = _values()
    assert fp.row(v, sealed=False, tenant_timezone="UTC") != \
        fp.row(v, sealed=False, tenant_timezone="Asia/Tokyo")


@pytest.mark.parametrize("field", ["created_at", "updated_at", "row_fingerprint",
                                   "members_fingerprint", "id", "flow_id"])
def test_the_excluded_fields_do_not_affect_the_row_digest(field):
    """Each exclusion is a decision. `created_at`/`updated_at` are re-stamped on every construction, so
    including them would make every row differ from itself - the trap normalizer.py:16-19 documents.
    `id` is the key, not content. `flow_id` has a writer that is NOT Stage 2, and any column another
    component owns must be excluded or Stage 2 clobbers it back to its own idea of the value."""
    base = fp.row(_values(), sealed=False, tenant_timezone="UTC")
    polluted = fp.row(_values(**{field: uuid.uuid4() if "id" in field else T0}),
                      sealed=False, tenant_timezone="UTC")
    assert base == polluted


def test_a_real_column_does_affect_the_row_digest():
    """The complement of the exclusion test: if nothing moved the digest, the skip would never rewrite
    anything at all and every row would freeze at its first value."""
    base = fp.row(_values(), sealed=False, tenant_timezone="UTC")
    assert base != fp.row(_values(status=LogTransactionStatus.error),
                          sealed=False, tenant_timezone="UTC")
    assert base != fp.row(_values(entry_count=4), sealed=False, tenant_timezone="UTC")
    assert base != fp.row(_values(attributes={"QuantityPicked": Decimal("11.0")}),
                          sealed=False, tenant_timezone="UTC")


def test_decimal_scale_does_not_change_the_digest():
    """Live case, not hypothetical: `ExpectedQuantity` arrives as 30.000000 while `QuantityPicked`
    arrives as 30.0. Treating those as different would rewrite the row on every single pass."""
    a = fp.row(_values(attributes={"q": Decimal("10.0")}), sealed=False, tenant_timezone="UTC")
    b = fp.row(_values(attributes={"q": Decimal("10.000")}), sealed=False, tenant_timezone="UTC")
    assert a == b


def test_an_enum_hashes_by_value_not_by_repr():
    """`str()` on an Enum member gives "LogTransactionStatus.success" in some Python versions and
    "success" in others, which would make the digest depend on the interpreter."""
    assert fp._canonical(LogTransactionStatus.success) == "success"


def test_key_order_in_attributes_does_not_change_the_digest():
    a = fp.row(_values(attributes={"x": 1, "y": 2}), sealed=False, tenant_timezone="UTC")
    b = fp.row(_values(attributes={"y": 2, "x": 1}), sealed=False, tenant_timezone="UTC")
    assert a == b


# =============================================================== 3. _DERIVE_VERSION
#: The digest of every function whose output the row fingerprint depends on. Editing any of them
#: without bumping `_DERIVE_VERSION` means stored rows keep matching their own stale fingerprint, so
#: the edit never reaches them - silently, forever.
_DERIVATION = ("_group", "_is_sealed", "_anchor", "_entry_stream_order", "_stream_pos")


def _derivation_digest() -> str:
    parts = [inspect.getsource(getattr(dt, name)) for name in _DERIVATION]
    parts.append(inspect.getsource(dt._TxnBuilder.compute))
    parts.append(inspect.getsource(dt._TxnBuilder._merged_attrs))
    return hashlib.sha256("".join(parts).encode("utf-8")).hexdigest()[:16]


#: Regenerate ONLY together with a `_DERIVE_VERSION` bump. If this test fails, the question to answer
#: is "does my change alter what a transaction's columns say?" - if yes, bump the version AND this
#: constant; if no (a comment, a rename), update this constant alone.
_EXPECTED_DERIVATION = "cba942a8ebe602bb"


def test_the_derivation_is_pinned_to_the_derive_version():
    """The safeguard that makes `_DERIVE_VERSION` real rather than a convention someone has to remember.

    Not a style rule: S3 skips a rewrite when the stored fingerprint matches, so an unbumped edit to the
    derivation never reaches stored rows. There is no failing test and no alert - the projection is
    quietly wrong until somebody questions a number.
    """
    assert _derivation_digest() == _EXPECTED_DERIVATION, (
        f"the derivation changed. If it changes what a transaction's columns SAY, bump "
        f"fingerprints._DERIVE_VERSION (currently {fp._DERIVE_VERSION}) so stored rows are recomputed, "
        f"and update _EXPECTED_DERIVATION to {_derivation_digest()!r}. If the change was cosmetic, "
        f"update _EXPECTED_DERIVATION alone.")


def test_the_derive_version_is_in_the_row_digest():
    """Bumping it has to actually invalidate stored fingerprints, or it is decoration."""
    v = _values()
    before = fp.row(v, sealed=False, tenant_timezone="UTC")
    original = fp._DERIVE_VERSION
    try:
        fp._DERIVE_VERSION = original + 1
        assert fp.row(v, sealed=False, tenant_timezone="UTC") != before
    finally:
        fp._DERIVE_VERSION = original


# =============================================================== 4. the two canonicalisers agree
def test_stage2_and_analytics_canonicalise_the_same_way():
    """They are deliberately separate copies - one belongs to Stage 2's contract, the other to
    analytics' - so a future change to either must not silently move the other. Asserted equal rather
    than shared, so a divergence is caught instead of merely hoped against."""
    from app.services.analytics import normalizer as n2
    for value in (None, True, 7, "x", Decimal("10.0"), Decimal("10.000"),
                  T0, T0.date(), {"b": 1, "a": 2}, [1, "2", None]):
        assert fp._canonical(value) == n2._canonical(value), f"diverged on {value!r}"


# =============================================================== 5. the rollback flag
def test_the_flag_defaults_on_and_is_a_real_rollback():
    """False must restore the PRE-S3 behaviour exactly, not a third behaviour: with the flag off the
    window still deletes and rebuilds unconditionally, which is what makes it a rollback."""
    from app.settings import settings
    assert settings.stage2_fingerprint_skip is True
    src = inspect.getsource(dt.regroup_window)
    assert "stage2_fingerprint_skip" in src
    assert "delete_for_transactions" in src, "the pre-S3 delete path must still exist behind the flag"


def test_the_update_is_addressed_by_the_full_partition_key():
    """`started_at` is the partition key and is NULLABLE. Addressing by id alone means PostgreSQL cannot
    prune and the UPDATE considers all 95 partitions; using `= NULL` instead of `IS NULL` silently
    matches nothing for a transaction whose entries all lack a parsable timestamp (A7)."""
    src = inspect.getsource(dt._update_transaction)
    assert "LogTransaction.started_at" in src, "the update must key on the partition column"
    assert "is_(None)" in src, "the NULL started_at case needs IS NULL, not = NULL"
