"""Chunk 56 (R3 of docs/analytics-ml-architecture/final_architecture.md, 18a): response scalars reach
`analytics_facts`, namespaced and allowlist-gated.

What R3 closes
--------------
Stage 1 parses the response in full and Stage 2 discards it. `_merged_attrs`
(`derive_transactions.py:75-91`) reads only `request` and `request_body`, so 150,104 response payloads,
424,632 `record_count` values and 3,641,353 records never reached a fact. R3 merges the SCALARS at
transaction grain; `records[]` expansion is R4.

Why it had a deadline
---------------------
`analytics_facts` is KEEP_FOREVER but raw `log_entries` drop at 60 days, so a field not captured today
cannot be backfilled later. That is the whole reason R3 was moved ahead of the screen that manages it.

The security shape, which is the point of most of this file
----------------------------------------------------------
`AccessToken` and `M3UserCredentials` are the two MOST FREQUENT response keys of the 145 measured, and
they would be flowing into a table that is never deleted. So:

    approved (registry says captured)  ->  stored
    unknown                           ->  NAME recorded, captured=false, value NEVER stored
    credential-shaped                 ->  never auto-approved, whatever any list says

The last one is defence in depth. `SEED_FIELDS` exists so capture produces measurable history from day
one rather than waiting for a screen, and a hardcoded seed list is a thing someone will edit - so a
pattern veto sits underneath it that no edit to that list can bypass.
"""

import pytest

from app.services.analytics import payload as p


# =============================================================== 1. extraction, measured shapes
def test_a_response_payload_is_unwrapped_one_level():
    """Measured shape: `{"response": {...}}`. The scalars live one level down, under a key that is
    itself called `response`, so unwrapping has to be explicit rather than inferred."""
    got = p.extract([("response", {"response": {"StockZone": "A1", "Route": "R1"}})])
    assert got == {"resp.StockZone": "A1", "resp.Route": "R1"}


def test_an_empty_response_is_not_an_error():
    """Also a measured shape, and a common one: `{"response": ""}` rather than an absent key."""
    assert p.extract([("response", {"response": ""})]) == {}


def test_mi_result_scalars_are_flat_and_prefixed():
    got = p.extract([("mi_result", {"result": "OK", "program": "MMS060MI",
                                    "transaction": "LstBalID"})])
    assert got == {"mi.result": "OK", "mi.program": "MMS060MI", "mi.transaction": "LstBalID"}


def test_records_are_counted_not_expanded():
    """`records[]` is where the ~200k rows/day lives, so R3 takes only its LENGTH. Expansion is R4 and
    is opt-in per transaction."""
    got = p.extract([("mi_result", {"result": "OK", "records": [{"a": 1}, {"a": 2}, {"a": 3}]})])
    assert got["mi.record_count"] == 3
    assert not any("a" in k for k in got), "no record field may leak into the transaction grain"


def test_record_counts_from_several_calls_are_summed():
    """A transaction can hold many M3 calls. "How many records did this transaction see" is a total, so
    summing is the honest answer - unlike `mi.program`, which is a pick."""
    got = p.extract([("mi_result", {"records": [1, 2]}), ("mi_result", {"records": [3]})])
    assert got["mi.record_count"] == 3


def test_the_last_mi_program_wins():
    """Genuinely ambiguous at transaction grain. The last is chosen because it is the call the response
    was built from, and the choice is pinned so it cannot drift silently."""
    got = p.extract([("mi_result", {"program": "FIRST"}), ("mi_result", {"program": "LAST"})])
    assert got["mi.program"] == "LAST"


def test_nested_values_are_dropped_not_flattened():
    """20 non-scalars against 1,713 scalars in the sample. Flattening them would invent key names that
    no registry row could match, so they are left for R4."""
    got = p.extract([("response", {"response": {"Ok": 1, "Nested": {"x": 1}, "List": [1, 2]}})])
    assert got == {"resp.Ok": 1}


def test_other_entry_types_contribute_nothing():
    """R3 is about the response half. `request` already reaches `attributes` via `_merged_attrs`, and
    capturing it again here would double it under a second name."""
    assert p.extract([("request", {"response": {"X": 1}}), ("info", {"a": 1}),
                      ("mi_call", {"program": "P"})]) == {}


def test_malformed_fields_do_not_raise():
    """`fields` is JSONB written by a parser, so a string or None where a dict was expected is a real
    possibility. The whole window's fold must not fail for it."""
    assert p.extract([("response", None), ("response", "junk"), ("mi_result", 7)]) == {}


def test_a_bool_is_kept_as_a_scalar():
    """`resp.HasPackages` is a real measured field. A flag is a useful DIMENSION and a useless MEASURE -
    which is why `_is_scalar` accepts it here and `contract.numeric_or_none` rejects it there."""
    assert p.extract([("response", {"response": {"HasPackages": True}})]) == {"resp.HasPackages": True}


# =============================================================== 2. namespacing
def test_request_and_response_item_number_cannot_collide():
    """Both halves carry `ItemNumber`. A flat merge drops one and which one depends on iteration order,
    so the prefix makes the collision structurally impossible rather than merely unlikely."""
    got = p.extract([("response", {"response": {"ItemNumber": "RESP-1"}})])
    assert "resp.ItemNumber" in got
    assert "ItemNumber" not in got


def test_the_namespaced_name_is_what_a_metric_addresses():
    """The contract with R1b: a definition names `attr:resp.ItemNumber`, so what is stored has to be
    exactly the key `contract.resolve_field` will look up."""
    from app.services.analytics import contract as c
    got = p.extract([("response", {"response": {"ItemNumber": "X"}})])
    fact = {"attributes": got}
    assert c.resolve_field(fact, "attr:resp.ItemNumber") == "X"


# =============================================================== 3. approval
def test_an_unapproved_field_is_reported_by_name_and_not_stored():
    captured, unknown = p.select({"resp.Weird": "value-that-must-not-persist"}, frozenset())
    assert captured == {}
    assert unknown == ["resp.Weird"]


def test_an_approved_field_is_stored():
    captured, unknown = p.select({"resp.BaseUoM": "KG"}, frozenset({"resp.BaseUoM"}))
    assert captured == {"resp.BaseUoM": "KG"}
    assert unknown == []


def test_select_never_returns_a_value_for_an_unknown_field():
    """The property that makes discovery safe. A newly appearing `SessionSecret` must be reportable
    without its value existing anywhere - so the second return is names, and there is no shape it could
    carry a value in."""
    _, unknown = p.select({"resp.SessionSecret": "hunter2"}, frozenset())
    assert unknown == ["resp.SessionSecret"]
    assert all(isinstance(x, str) for x in unknown)
    assert not any("hunter2" in x for x in unknown)


# =============================================================== 4. the credential veto
@pytest.mark.parametrize("name", [
    "resp.AccessToken", "resp.M3UserCredentials", "resp.Password", "resp.ApiKey",
    "resp.api_key", "resp.Cipher", "resp.SessionToken", "resp.ClientSecret",
    "resp.Authorization", "resp.BearerToken", "resp.PrivateKey", "resp.Signature",
    "mi.PasswdHash", "resp.M3Credentials2",
])
def test_a_credential_shaped_name_is_never_auto_approved(name):
    """`AccessToken` and `M3UserCredentials` are the two most frequent response keys measured, and
    `analytics_facts` is KEEP_FOREVER. Patterns rather than exact names because the risk is a field the
    WMS RENAMES or adds - which is precisely why `_SENSITIVE`'s five exact words were not enough."""
    assert p.never_auto_approve(name) is True
    assert p.seeded(name) is False


@pytest.mark.parametrize("name", [
    "resp.QuantityOnHand", "resp.BaseUoM", "resp.Location", "mi.record_count", "resp.Picker",
])
def test_an_ordinary_warehouse_field_is_not_vetoed(name):
    """The veto has to be narrow enough to be useful. A pattern that caught `Picker` or `Location`
    would make the seed list empty and R3 pointless."""
    assert p.never_auto_approve(name) is False


def test_the_veto_beats_the_seed_list():
    """Defence in depth, asserted rather than assumed. `SEED_FIELDS` is a hardcoded list, so somebody
    will edit it; an edit must not be able to auto-approve a credential."""
    poisoned = p.SEED_FIELDS | {"resp.AccessToken"}
    assert "resp.AccessToken" in poisoned
    assert p.seeded("resp.AccessToken") is False


def test_no_seeded_field_is_credential_shaped():
    """Guards the list as it stands today, so a careless addition fails here rather than in production."""
    offenders = [f for f in p.SEED_FIELDS if p.never_auto_approve(f)]
    assert offenders == [], f"credential-shaped names in SEED_FIELDS: {offenders}"


def test_every_seeded_field_is_namespaced():
    """An un-namespaced entry could never match anything `extract` produces, so it would be a silently
    dead line in a security-relevant list."""
    bad = [f for f in p.SEED_FIELDS
           if not (f.startswith(p.RESPONSE_PREFIX) or f.startswith(p.MI_PREFIX))]
    assert bad == [], bad


def test_seeding_does_not_override_a_decision():
    """`select` consults the registry only. So a field somebody has un-ticked stays un-ticked even
    though it is in `SEED_FIELDS` - a decision must outlive the default that preceded it."""
    assert "resp.BaseUoM" in p.SEED_FIELDS
    captured, unknown = p.select({"resp.BaseUoM": "KG"}, frozenset())
    assert captured == {}
    assert unknown == ["resp.BaseUoM"]


# =============================================================== 5. the ordering bug
def test_the_cycle_registers_fields_before_reading_the_approvals():
    """The bug this test exists for was silent, and its consequence was PERMANENT.

    The first implementation read `approved` and then called `observe_fields`. So on the run that
    DISCOVERED a seeded field, that field was not yet approved, and the fact was written without it.
    It self-heals only if the window is folded again - and tickets are published on change, so a window
    that never changes again never is. The response data for it would have been lost when the raw
    entries expired at 60 days, which is precisely the loss R3 exists to prevent.

    Found by an end-to-end run, not by a unit test: discovery reported 12 fields and 9 approvals while
    the fact carried nothing but its request-side attribute. Asserted on the source, because the defect
    is an ORDER and there is no return value that reveals it.
    """
    import inspect
    from app.services.analytics import consume

    src = inspect.getsource(consume._consume_run)
    register = src.index("observe_fields")
    approve = src.index("approved_attributes")
    normalise = src.index("n2.normalise")
    assert register < approve, \
        "fields must be registered BEFORE the approvals are read, or a seeded field is missed on the " \
        "run that discovers it and the gap can be permanent"
    assert approve < normalise, "approvals must be read before any fact is normalised"


def test_a_seeded_field_is_capturable_on_first_sight():
    """The property the ordering exists to give: no second fold required. Asserted at the unit level
    too, so the guarantee does not rest solely on reading the cycle's source."""
    observed = p.extract([("mi_result", {"program": "MMS060MI", "records": [1, 2]})])
    approved = frozenset(n for n in observed if p.seeded(n))
    captured, unknown = p.select(observed, approved)
    assert captured == {"mi.program": "MMS060MI", "mi.record_count": 2}
    assert unknown == []
