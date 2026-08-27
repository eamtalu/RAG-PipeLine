"""S3. The two fingerprints that make a Stage 2 write count independent of the rebuild count.

**Pure: no database, no clock, no configuration.** Same discipline as `contract` and the normaliser.

Why TWO, and not one
--------------------
    members_fingerprint   which entries the transaction is made of. An ORDERED digest over their ids.
    row_fingerprint       what the transaction's own columns say.

One digest covering both would be simpler and wrong. `entry_count` in particular is not a substitute
for the member digest: swap one same-timestamped entry between two transactions and both keep their
counts, both keep their `started_at` and `ended_at`, and every derived column is identical - while
`log_entry_assignment` now holds the WRONG mapping and `entries_stmt` renders the wrong timeline,
forever, because nothing will ever recompute a row whose fingerprint matched.

Splitting them also makes the cheap case cheap in the right way. Sealing changes the ROW and not the
MEMBERS, so it touches `log_transactions` alone and never rewrites a single assignment row. That is
what takes assignments from 18.1 writes per surviving row to 1.0.

What is excluded from the row digest, and why each one
-----------------------------------------------------
    created_at, updated_at   re-stamped on every construction, so including them would make every
                             row differ from itself. This is the exact trap `normalizer.py:16-19`
                             documents for the analytics fingerprint.
    the fingerprints         a hash cannot include itself.
    id                       it is the KEY, not content. Two rows with the same id are the same row.
    flow_id                  a Phase-3 hook with a writer that is NOT Stage 2. Any column another
                             component owns must be excluded, or Stage 2 decides the row "changed"
                             and clobbers it back to its own idea of the value.

What is INCLUDED that might look surprising
-------------------------------------------
    sealed                   so a seal flip is a real write - exactly one per transaction lifetime.
                             Without it the sealer's UPDATE would be invisible to the diff and S1's
                             fix would silently stop working the moment S3 shipped.
    the tenant timezone      `date` is derived through it, so a zone change genuinely changes the row.
                             Passed in rather than read, which is what keeps this module pure.
    _DERIVE_VERSION          see below. This is the one that is easy to leave out and expensive to.
"""

import hashlib
import json
import uuid
from datetime import date as date_type, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

#: Bump this when ANY of the derivation changes: `_group`, `_TxnBuilder.compute`, `_is_sealed`,
#: `_anchor`, `_entry_stream_order`, `_stream_pos`, or `_merged_attrs`.
#:
#: S3 skips a rewrite when the stored fingerprint matches the recomputed one. So an edit to the
#: derivation that is NOT accompanied by a bump here never reaches the stored rows: they keep matching
#: their own stale fingerprint, and the projection is quietly wrong forever - no failing test, no
#: alert, and the only symptom is a number somebody eventually questions.
#:
#: `tests/test_stage2_fingerprints_chunk59.py` pins the derivation's source digest against this value,
#: so editing one of those functions without bumping this fails loudly instead.
_DERIVE_VERSION = 2  # 2 = 18r server-scoped grouping (chunk 67); 1 = S3 as shipped

#: Columns of `log_transactions` that the row digest deliberately ignores. See the module docstring;
#: every entry here is a decision rather than an omission.
_NOT_FINGERPRINTED: frozenset[str] = frozenset({
    "created_at", "updated_at", "row_fingerprint", "members_fingerprint", "id", "flow_id",
})


def _canonical(value: Any) -> Any:
    """A value in a form that hashes identically across two reads of the same unchanged row.

    Deliberately the same rules as `analytics/normalizer._canonical`, and deliberately a separate
    copy: that one is part of the analytics contract and this one is part of Stage 2's, so a future
    change to either must not silently move the other. The behaviours are asserted equal by a test, so
    a divergence is caught rather than merely hoped against.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        # `10` and `10.0` are the same quantity and must hash the same. Live case, not hypothetical:
        # `ExpectedQuantity` arrives as `30.000000` while `QuantityPicked` arrives as `30.0`.
        return f"{value.normalize():f}"
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date_type):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    # Enums carry `.value`; anything else falls back to its text. `str()` on an Enum member gives
    # "LogTransactionStatus.success" in some Python versions and "success" in others, which would make
    # the digest depend on the interpreter.
    return _canonical(value.value) if hasattr(value, "value") else str(value)


def _digest(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def members(entry_ids: Iterable[uuid.UUID]) -> str:
    """An ORDERED digest over the entries a transaction is made of.

    Ordered rather than a set, so a reordering is a change. Two transactions holding the same entries
    in a different sequence render different timelines, and `log_entry_assignment.seq` comes straight
    from that order (`logs.py:1059`).

    Sorted before hashing, not in arrival order: arrival order is an artefact of which query returned
    the rows, while membership is a property of the transaction. Sorting makes the digest stable
    across two reads that returned the same rows differently ordered.
    """
    return _digest([str(x) for x in sorted(str(i) for i in entry_ids)])


def row(values: Mapping[str, Any], *, sealed: bool, tenant_timezone: str | None) -> str:
    """A digest over everything about the transaction row that Stage 2 owns.

    `values` is `_TxnBuilder.compute()`'s output. `sealed` is passed separately because it is decided
    after `compute` by `_is_sealed`, and it MUST be in the hash - see the module docstring.
    """
    payload = {k: _canonical(v) for k, v in sorted(values.items()) if k not in _NOT_FINGERPRINTED}
    payload["__sealed"] = bool(sealed)
    payload["__tz"] = tenant_timezone or "UTC"
    payload["__derive_version"] = _DERIVE_VERSION
    return _digest(payload)
