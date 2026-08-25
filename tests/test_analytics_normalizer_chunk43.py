"""Chunk 43, Phase 3a: N2, the fact normaliser. One transaction row to one typed fact, or a quarantine.

    N2. Fact normaliser. One `log_transactions` row plus its `attributes` JSONB to one typed fact row,
    or a quarantine record with a reason. **Pure: no database, no clock, no configuration.** This is
    where correctness is won, which is why the module has no I/O.

That purity is the testable claim, and these tests hold it: the tenant timezone arrives as a PARAMETER,
never read from settings, and nothing here needs a database.

Two things carry more weight than the rest.

**The fingerprint decides whether anything gets written at all.** At a 98.7% rebuild rate almost every
recheck must be absorbed as a no-op by a matching fingerprint (invariant 6), or the worker produces a
constant stream of pointless aggregate writes. So it has to be stable across a rebuild that changed
nothing -- which means it must NOT include the source row's `created_at`, because that is refreshed on
every rebuild by construction. Getting that wrong makes the fingerprint useless without making anything
visibly fail.

**Absent is never zero, and a placeholder is not a rejection.** A missing quantity is quarantined, not
folded in as a zero-unit attempt. A placeholder `transaction_type` makes that DIMENSION unusable and
nothing more: all 83 live `AddStockCountLine` rows carry a real `CountedQuantity` under one, so
rejecting the row would silently drop real stock counts.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.services.analytics import contract as c
from app.services.analytics import normalizer as n2

LONDON = "Europe/London"
T_BST = datetime(2026, 8, 5, 23, 30, tzinfo=timezone.utc)   # 00:30 the NEXT day, London time
T_GMT = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)


def txn(**over) -> dict:
    """A `log_transactions` row as the worker reads it, with live-shaped defaults."""
    base = {
        "id": "11111111-1111-1111-1111-111111111111",
        "customer_code": "t1",
        "started_at": T_GMT,
        "ended_at": T_GMT,
        "duration_ms": 412,
        "method": "ConfirmPickLine",
        "transaction_name": "Pick",
        "transaction_type": "002001",
        "status": "success",
        "item_number": "101978",
        "order_number": "1000006835",
        "delivery_number": None,
        "warehouse": "BRI",
        "warehouse_id": "1",
        "user_name": "EDA",
        "device_id": None,
        "device_name": None,
        "created_at": datetime(2026, 8, 6, 3, 0, tzinfo=timezone.utc),
        "attributes": {"QuantityPicked": "10.0", "ExpectedQuantity": "10.000000",
                       "FromLocation": "H01A", "ToLocation": "BRI08-P",
                       "LotNumber": "2608031215", "MethodName": "ConfirmPickLine"},
    }
    base.update(over)
    return base


# ==================================================== purity
def test_the_module_imports_no_database_clock_or_settings():
    """The claim N2 is held to. A normaliser that reached for `datetime.now()` or `settings` could not
    be reasoned about without a running system, and its output would stop being reproducible -- which
    the ledger and the fingerprint both depend on."""
    import inspect
    src = inspect.getsource(n2)
    for forbidden in ("datetime.now", "utcnow", "from app.settings", "async def", "AsyncSession",
                      "select(", "db."):
        assert forbidden not in src, f"N2 must stay pure: found {forbidden!r}"


def test_the_tenant_timezone_is_a_parameter_not_a_lookup():
    import inspect
    assert "tenant_timezone" in inspect.signature(n2.normalise).parameters


# ==================================================== the happy path
def test_a_clean_pick_becomes_a_fact_with_every_pinned_field():
    fact, issue = n2.normalise(txn(), tenant_timezone=LONDON)
    assert issue is None
    for field in c.FACT_FIELDS:
        assert field in fact, f"{field} is in FACT_FIELDS but the normaliser did not emit it"


def test_the_quantity_is_an_exact_decimal_from_the_right_attribute():
    """`ConfirmPickLine` measures `QuantityPicked`. `ExpectedQuantity` is present in the same JSONB and
    is NOT a measure: it is mutable per instruction, so fill rate is not derivable from it."""
    fact, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    assert fact["quantity"] == Decimal("10")
    assert isinstance(fact["quantity"], Decimal)
    assert fact["quantity_classification"] == c.Classification.pick.value


def test_a_stock_count_reads_its_own_field_not_the_pick_field():
    fact, _ = n2.normalise(
        txn(method="ReportCount", attributes={"CountedQuantity": "3.415"}), tenant_timezone=LONDON)
    assert fact["quantity"] == Decimal("3.415")


def test_a_method_that_carries_no_quantity_normalises_without_one():
    """46 of 49 methods. Their measures are volume, duration, status and actor, so a fact with no
    quantity is not a defect and must not be quarantined."""
    fact, issue = n2.normalise(
        txn(method="ListPickLines", attributes={}), tenant_timezone=LONDON)
    assert issue is None
    assert fact["quantity"] is None
    assert fact["quantity_classification"] == c.Classification.non_quantity.value


# ==================================================== business_date, in the tenant's zone
def test_the_business_date_is_the_tenant_local_day_not_the_utc_one():
    """The trap the plan calls out. 23:30 UTC on 5 August is 00:30 on the 6th in London, so an operator
    asking for "the 6th" means a different set of rows than a UTC day would give."""
    fact, _ = n2.normalise(txn(started_at=T_BST), tenant_timezone=LONDON)
    assert fact["event_time"] == T_BST
    assert fact["business_date"].isoformat() == "2026-08-06", "UTC would say the 5th"


def test_the_business_date_follows_the_zone_it_is_given():
    fact_utc, _ = n2.normalise(txn(started_at=T_BST), tenant_timezone="UTC")
    assert fact_utc["business_date"].isoformat() == "2026-08-05"


def test_a_transaction_with_no_start_instant_has_no_business_date():
    """Legitimate, not a defect: a transaction all of whose entries lack a parsable timestamp has no
    instant. It still becomes a fact -- in the DEFAULT partition -- rather than being quarantined."""
    fact, issue = n2.normalise(txn(started_at=None), tenant_timezone=LONDON)
    assert issue is None
    assert fact["event_time"] is None and fact["business_date"] is None


def test_an_unusable_timezone_does_not_take_the_row_down():
    """A malformed timezone on a customer row must not stop that tenant folding. Falling back to UTC is
    wrong by at most an hour; quarantining every row is wrong entirely."""
    fact, issue = n2.normalise(txn(started_at=T_BST), tenant_timezone="Not/AZone")
    assert issue is None
    assert fact["business_date"].isoformat() == "2026-08-05", "fell back to UTC"


# ==================================================== placeholders
def test_a_placeholder_transaction_type_is_dropped_as_a_dimension_but_the_row_survives():
    """All 83 live `AddStockCountLine` rows carry a real `CountedQuantity` under a placeholder type.
    Rejecting the ROW would silently drop real stock counts; keeping the placeholder as a dimension
    value would split one item's total across `002001` and `xxxxxx`."""
    fact, issue = n2.normalise(
        txn(method="AddStockCountLine", transaction_type="xxxxxx",
            attributes={"CountedQuantity": "7"}), tenant_timezone=LONDON)
    assert issue is None
    assert fact["transaction_type"] is None, "unusable as a dimension"
    assert fact["quantity"] == Decimal("7"), "but the quantity is real"


@pytest.mark.parametrize("placeholder", ["xxxxxx", "XXXXX", "00xxxx", "0050XX", "XXXXXX"])
def test_every_live_placeholder_shape_is_dropped(placeholder):
    fact, _ = n2.normalise(txn(transaction_type=placeholder), tenant_timezone=LONDON)
    assert fact["transaction_type"] is None


# ==================================================== quarantine
def test_a_quantity_method_with_an_absent_quantity_is_quarantined_not_zeroed():
    """The most dangerous defaulting available. Zero means "the operator picked nothing", a real and
    separately counted event; absent means the question was never answered."""
    fact, issue = n2.normalise(txn(attributes={"QuantityPicked": ""}), tenant_timezone=LONDON)
    assert fact is None
    assert issue["reason"] == "unusable_quantity"
    assert "QuantityPicked" in str(issue["observed"]), "the issue must record what was seen"


def test_a_quantity_method_missing_the_field_entirely_is_quarantined():
    fact, issue = n2.normalise(txn(attributes={}), tenant_timezone=LONDON)
    assert fact is None and issue["reason"] == "unusable_quantity"


def test_a_transaction_with_no_method_still_becomes_a_fact():
    """Measured against the live server, which is why this test exists in this shape.

    25 of 397 live transactions (6.3%) have no method. They are NOT fragments: `entry_count` runs 2 to
    28, durations reach 172 seconds, 24 of 25 have `mi_program_count = 0` (so they are simply non-MI
    activity), and 9 carry `status = incomplete` -- precisely the rows an operator would want counted in
    an incomplete rate.

    Quarantining them would make 6.3% of transactions invisible to every volume, duration and status
    metric while the totals still looked plausible. And the contract already answers this: a method not
    on the quantity allow-list is `non_quantity`, and None is not on it. There is nothing undecidable
    here -- only nothing to look a quantity up by."""
    fact, issue = n2.normalise(txn(method=None, attributes={}), tenant_timezone=LONDON)
    assert issue is None
    assert fact["method"] is None
    assert fact["quantity"] is None
    assert fact["quantity_classification"] == c.Classification.non_quantity.value
    assert fact["duration_ms"] == 412 and fact["status"] == "success", "its measures survive"


def test_a_method_less_row_does_not_pick_up_a_stray_quantity_attribute():
    """The allow-list is consulted first and wins, and None is not on it. Measured leakage: 3 of 7,307
    `ListItemAlternateUnitsOfMeasure` rows carry a stray `CountedQuantity`, so trusting the presence of
    a JSONB key would invent stock counts out of listing calls."""
    fact, issue = n2.normalise(
        txn(method=None, attributes={"QuantityPicked": "99", "CountedQuantity": "99"}),
        tenant_timezone=LONDON)
    assert issue is None
    assert fact["quantity"] is None
    assert fact["quantity_classification"] == c.Classification.non_quantity.value


def test_a_quarantine_record_carries_both_identity_columns_and_survives_the_raw_row():
    """The raw entry is dropped at 60 days, so a quarantine row recording only "something went wrong"
    is useless a year later. F3's identity is both columns, because the id alone is not one."""
    _, issue = n2.normalise(txn(attributes={}), tenant_timezone=LONDON)
    assert issue["source_transaction_id"] == txn()["id"]
    assert issue["source_started_at"] == T_GMT
    assert issue["observed"], "what was seen must be preserved"


def test_quarantine_returns_a_record_rather_than_raising():
    """A1: quarantine must never halt a tenant. Raising would make one unexplained row freeze every
    metric until a human intervened -- and it is by definition the row nobody understands yet."""
    for bad in ({}, {"QuantityPicked": "abc"}, {"QuantityPicked": None}):
        fact, issue = n2.normalise(txn(attributes=bad), tenant_timezone=LONDON)
        assert fact is None and issue is not None


# ==================================================== the fingerprint
def test_the_fingerprint_is_stable_across_a_rebuild_that_changed_nothing():
    """Invariant 6, and the single most important property in this file. At a 98.7% rebuild rate almost
    every recheck must write NOTHING, and this is what makes that possible."""
    a, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    b, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    assert a["source_version_hash"] == b["source_version_hash"]


def test_the_fingerprint_ignores_the_source_rows_write_time():
    """`log_transactions.created_at` is refreshed on EVERY rebuild by construction -- Stage 2 deletes
    and re-inserts. Including it would make every fingerprint differ every cycle, so nothing would ever
    be absorbed as a no-op and the rebuild rate would turn into a write rate. Nothing would visibly
    break; the system would just churn."""
    a, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    b, _ = n2.normalise(txn(created_at=datetime(2027, 1, 1, tzinfo=timezone.utc)),
                        tenant_timezone=LONDON)
    assert a["source_version_hash"] == b["source_version_hash"]


@pytest.mark.parametrize("field,value", [
    ("method", "ListPickLines"),
    ("status", "error"),
    ("item_number", "999999"),
    ("warehouse", "LON"),
    ("user_name", "SOMEONE"),
    ("duration_ms", 999),
    ("started_at", datetime(2026, 3, 3, 9, 0, tzinfo=timezone.utc)),
])
def test_the_fingerprint_changes_when_anything_that_affects_a_measure_changes(field, value):
    a, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    b, _ = n2.normalise(txn(**{field: value}), tenant_timezone=LONDON)
    assert a["source_version_hash"] != b["source_version_hash"], f"{field} must be fingerprinted"


def test_the_fingerprint_changes_when_the_quantity_changes():
    a, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    b, _ = n2.normalise(txn(attributes={**txn()["attributes"], "QuantityPicked": "9.0"}),
                        tenant_timezone=LONDON)
    assert a["source_version_hash"] != b["source_version_hash"]


def test_the_fingerprint_changes_when_the_tenant_timezone_changes():
    """Subtle and load-bearing. The zone decides `business_date`, which decides which day a fact rolls
    up into. A zone change with an unchanged fingerprint would leave every rollup on the old day with
    nothing to trigger a recompute."""
    a, _ = n2.normalise(txn(started_at=T_BST), tenant_timezone=LONDON)
    b, _ = n2.normalise(txn(started_at=T_BST), tenant_timezone="UTC")
    assert a["business_date"] != b["business_date"]
    assert a["source_version_hash"] != b["source_version_hash"]


def test_the_fingerprint_is_not_affected_by_attribute_ordering():
    """JSONB has no guaranteed key order, so a fingerprint that depended on it would differ between two
    reads of the same unchanged row."""
    attrs = txn()["attributes"]
    a, _ = n2.normalise(txn(attributes=dict(sorted(attrs.items()))), tenant_timezone=LONDON)
    b, _ = n2.normalise(txn(attributes=dict(reversed(list(attrs.items())))), tenant_timezone=LONDON)
    assert a["source_version_hash"] == b["source_version_hash"]


def test_the_fingerprint_is_a_hex_digest_that_fits_the_column():
    fact, _ = n2.normalise(txn(), tenant_timezone=LONDON)
    h = fact["source_version_hash"]
    assert len(h) <= 64 and all(ch in "0123456789abcdef" for ch in h)


# ==================================================== the Phase 0 fixtures, through N2
def test_the_named_defect_fixtures_normalise_the_way_phase_0_pinned_them():
    """Closes the loop: Phase 0 asserted what the diff should do given fact rows; this asserts N2
    produces those rows from transaction rows."""
    cases = [
        ("zero pick", {"QuantityPicked": "0.0"}, c.Classification.attempt),
        ("short pick", {"QuantityPicked": "12.0"}, c.Classification.pick),
        ("fractional", {"QuantityPicked": "0.333333"}, c.Classification.pick),
    ]
    for name, attrs, expected in cases:
        fact, issue = n2.normalise(txn(attributes=attrs), tenant_timezone=LONDON)
        assert issue is None, name
        assert fact["quantity_classification"] == expected.value, name


def test_an_error_transaction_keeps_its_status_and_is_not_a_quantity_row():
    """An ERROR carries no units. It must not reach a quantity counter, and must not be folded in as a
    zero-unit attempt either -- its quantity is unknown, not zero."""
    fact, issue = n2.normalise(
        txn(status="error", method="ListPickLines", attributes={}), tenant_timezone=LONDON)
    assert issue is None
    assert fact["status"] == "error"
    assert fact["quantity_classification"] == c.Classification.non_quantity.value


# ==================================================== the truncation outage, 2026-08-25
#
# FOUND IN PRODUCTION, not in review. 49 analytics windows were dead-lettered over two days with
#
#     StringDataRightTruncationError: value too long for type character varying(32)
#
# on INSERT into `analytics_facts`. Those ranges have NO facts at all - silent, partial analytics data
# loss that nothing surfaced, because an abandoned ticket is not an alert.
#
# The cause is a lesson that was learned once and not carried across. Stage 2 caps promoted string
# values to their column width in `_cap_over_length`, and that function exists BECAUSE one over-length
# `ItemNumber` once stalled all stitching for a tenant - there is a postmortem,
# docs/stage2-stitching-stall-postmortem-and-fix.md. The analytics normaliser was written afterwards,
# promotes the same kind of value out of the same JSONB, and had no equivalent.
#
# Measured on the deployed database at the time of the fix:
#     ToLocation -> to_location varchar(32)   longest 37, 19 rows over   <- the real overflow
#     LotNumber  -> lot_number  varchar(64)   longest 37, fits
#
# The second line is here because a first probe of mine hardcoded a 32 cap for `lot_number` and reported
# a false overflow. The model and the deployed schema were then checked column by column and agree
# exactly - there is no drift. Only the three genuinely-32-wide columns can overflow:
# `from_location`, `to_location`, `transaction_type`.

def test_an_over_length_promoted_value_is_capped_not_raised():
    """The exact production failure. A 37-character `LotNumber` must not be able to abort the fold.

    Capped rather than quarantined, deliberately: the value is a DIMENSION, not a measure. Quarantining
    would discard a real transaction's quantity over a long label, and the full value is still on the raw
    log entry - only the queryable column is trimmed. Same trade-off Stage 2 already made.
    """
    long_loc = "BRI-ZONE-A-AISLE-12-BAY-04-LEVEL-3-POS"
    assert len(long_loc) == 38, "the measured production value was 37; this is one over"
    fact, issue = n2.normalise(
        txn(attributes={"QuantityPicked": "10", "ToLocation": long_loc}), tenant_timezone=LONDON)
    assert issue is None, "an over-length dimension must not quarantine a real transaction"
    assert len(fact["to_location"]) == 32
    assert fact["to_location"] == long_loc[:32]


def test_every_promoted_string_is_capped_to_its_own_column_width():
    """Driven by the ORM mapping rather than a hardcoded list, so a resized or added column is picked up
    for free - the property Stage 2's `_txn_str_limits` docstring calls out, and the reason this fix
    cannot rot the way a literal list would."""
    limits = n2._fact_str_limits()
    assert limits["to_location"] == 32 and limits["from_location"] == 32
    assert limits["lot_number"] == 64, "read from the schema, not from a guess about it"
    assert limits["item_number"] == 128

    over = {"QuantityPicked": "10", "LotNumber": "L" * 80, "ToLocation": "T" * 80,
            "FromLocation": "F" * 80, "ItemNumber": "I" * 300}
    fact, issue = n2.normalise(txn(attributes=over), tenant_timezone=LONDON)
    assert issue is None
    for field, limit in limits.items():
        value = fact.get(field)
        if isinstance(value, str):
            assert len(value) <= limit, f"{field} is {len(value)} long, over its {limit} column"


def test_a_value_within_its_column_is_untouched():
    """A cap that trimmed a legitimate value would silently change a dimension, splitting one item's
    total across two labels - which is worse than the crash it replaces."""
    fact, _ = n2.normalise(
        txn(attributes={"QuantityPicked": "10", "ToLocation": "BRI-A-01"}), tenant_timezone=LONDON)
    assert fact["to_location"] == "BRI-A-01"


def test_capping_is_deterministic_so_the_diff_still_settles():
    """A first version of this test claimed the cap MUST precede the fingerprint or "every such row is
    rewritten forever". That was wrong, and the test failed for the right reason - worth recording,
    because the wrong version would have passed as soon as it was loosened.

    The range diff recomputes the hash through the same `normalise` call it used originally, so the
    ORDER of capping cannot produce a mismatch. What actually matters is DETERMINISM: the same source row
    must hash identically every time, or a capped row would be rewritten on every pass.

    Two rows with different RAW attributes hash differently even when their capped columns match, and
    that is correct rather than a bug: `attributes` is deliberately in the fingerprint, so that a measure
    invented next year over the untrimmed value is still a detectable change.
    """
    row = txn(attributes={"QuantityPicked": "10", "ToLocation": "T" * 80})
    a, _ = n2.normalise(row, tenant_timezone=LONDON)
    b, _ = n2.normalise(row, tenant_timezone=LONDON)
    assert a["to_location"] == b["to_location"] == "T" * 32
    assert a["source_version_hash"] == b["source_version_hash"], \
        "the same source row must hash identically, or a capped row is rewritten on every pass"

    # The raw blob still differs from a genuinely-short value, and SHOULD.
    short, _ = n2.normalise(txn(attributes={"QuantityPicked": "10", "ToLocation": "T" * 32}),
                            tenant_timezone=LONDON)
    assert short["to_location"] == a["to_location"]
    assert short["source_version_hash"] != a["source_version_hash"], \
        "the untrimmed value is in `attributes` and is part of what the fingerprint covers"


def test_the_cap_runs_before_the_fingerprint():
    """Not for the diff's sake - see above - but so the hash DESCRIBES what is stored. An audit that
    recomputed a hash from the stored columns and got a different answer would be investigating a
    discrepancy that does not exist."""
    import inspect
    src = inspect.getsource(n2.normalise)
    assert src.index("_cap_over_length") < src.index("_fingerprint(fact)")


def test_the_cap_covers_the_response_attributes_too():
    """R3 merges response scalars into the same `attributes`, and they promote through the same path. A
    cap that only covered request-side keys would leave the identical crash reachable from the response
    half - which is now the larger source of keys."""
    fact, issue = n2.normalise(
        txn(attributes={"QuantityPicked": "10"}), tenant_timezone=LONDON,
        response_attributes={"ToLocation": "R" * 90})
    assert issue is None
    assert len(fact["to_location"]) <= 32
