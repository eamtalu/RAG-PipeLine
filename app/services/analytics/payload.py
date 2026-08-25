"""R3. Turning a transaction's response entries into measurable fact attributes.

**Pure: no database, no clock, no configuration.** Same discipline as `contract` and the normaliser,
which is what lets these rules be asserted without a pipeline. N3 reads the entries and hands them
here; nothing in this module knows a database exists.

The gap this closes
-------------------
Stage 1 parses the response in full and Stage 2 discards it. `_merged_attrs`
(`derive_transactions.py:75-91`) loops the entries and reads only `request` and `request_body`, so
150,104 response payloads, 424,632 `mi_result.record_count` values and 3,641,353 records never reach
`analytics_facts`. A metric over `QuantityOnHand` or `STQT` was not expressible at all.

The two shapes, measured rather than assumed
--------------------------------------------
    response   {"response": {"StockZone": "A1", "Route": "R1", ...}}   nested one level down
               {"response": ""}                                        empty, and common
    mi_result  {"result": "OK", "program": "MMS060MI",
                "transaction": "LstBalID", "records": [ {...}, ... ]}   flat, plus ONE array

Over 400 live entries: 1,713 scalars against 20 non-scalars, so the payloads are effectively flat and
a per-transaction merge captures nearly all of it. `records` is the exception and is deliberately NOT
expanded here - that is R4, and it is where the ~200k rows/day lives. Its LENGTH is captured as
`mi.record_count`, which is the measure most questions about a stock count actually want.

Namespacing is not cosmetic
---------------------------
Request and response both carry `ItemNumber`. A flat merge silently drops one, and which one depends on
iteration order. So response fields are prefixed `resp.` and `mi_result` fields `mi.` on the way in,
which makes a collision structurally impossible rather than merely unlikely. `contract.resolve_field`
reads them back as `attr:resp.ItemNumber`.

Approval, and why there is a hardcoded seed at all
--------------------------------------------------
`analytics_field_registry` is the authority: a field is captured only when its row says so, and an
unknown key is recorded by NAME with `captured = false` and never by value.

But an empty registry captures nothing, and R3 exists because raw entries expire at 60 days - so
shipping "discovers everything, captures nothing until somebody clicks" would spend the whole window
collecting field names and no data. `SEED_FIELDS` is therefore the set auto-approved on first sight,
and it lives in code on purpose: it is a security boundary, so it should be reviewable in a diff rather
than editable in a table.

`never_auto_approve` is defence in depth on top of it. `AccessToken` and `M3UserCredentials` are the
two most frequent response keys of the 145 measured, and `analytics_facts` is KEEP_FOREVER - so a
careless future addition to `SEED_FIELDS` must not be able to auto-approve a credential. A name that
looks credential-shaped is never seeded regardless of what any list says; it can still be approved
deliberately, by a person, one row at a time.
"""

import re

#: Prefix per entry type. The key under which a `response` entry nests its payload is also `response`,
#: which is why unwrapping is explicit below rather than inferred.
RESPONSE_PREFIX = "resp."
MI_PREFIX = "mi."

#: `mi_result`'s own bookkeeping keys. Captured because "which M3 program answered, and did it
#: succeed" is a dimension people ask for constantly, and it is three short strings per transaction.
_MI_SCALARS = ("result", "program", "transaction")

#: The array R4 will expand. Here only its length is taken.
_MI_RECORDS = "records"

#: Fields auto-approved the first time they are seen, so capture starts producing measurable history
#: immediately instead of waiting for a screen that does not exist yet.
#:
#: Chosen from what was actually measured on the live server, and restricted to fields that are
#: warehouse facts rather than protocol noise. Anything absent from here still DISCOVERS - it is
#: recorded by name for review - so this list is a head start, not a limit.
SEED_FIELDS: frozenset[str] = frozenset({
    # mi_result bookkeeping
    "mi.result", "mi.program", "mi.transaction", "mi.record_count",
    # quantities and stock state: the reason R3 exists
    "resp.QuantityOnHand", "resp.OnHandQuantity", "resp.AllocatedQuantity",
    "resp.AvailableQuantity", "resp.TotalNumberOfBalances", "resp.NumberOfLines",
    "resp.TotalLines", "resp.LinesToPick", "resp.LinesToPack", "resp.LinesPicked",
    # item and unit identity - the dimensions a per-item total needs
    "resp.ItemNumber", "resp.ItemDescription", "resp.BaseUoM", "resp.AlternateUoM",
    "resp.LotNumber", "resp.SerialNumber",
    # place
    "resp.Location", "resp.StockZone", "resp.LocationType", "resp.Warehouse",
    # order and delivery context
    "resp.DeliveryNumber", "resp.OrderNumber", "resp.CustomerNumber", "resp.CustomerName",
    "resp.Route", "resp.PickListSuffix", "resp.PickingStatus", "resp.PackingStatus",
    "resp.HasPackages", "resp.Picker", "resp.StatusBalanceID", "resp.PriorityDate",
})

#: A name matching any of these is NEVER auto-approved, whatever `SEED_FIELDS` says.
#:
#: Substring patterns rather than exact names, because the risk is a field the WMS renames or adds -
#: `SessionToken`, `ApiSecret`, `M3Credentials2`. An exact denylist is what `_SENSITIVE`
#: (`derive_transactions.py:45`) already is, and its five words are why R3 needed a different shape.
_NEVER_SEED = re.compile(
    r"token|secret|password|passwd|credential|cipher|apikey|api_key|authorization|bearer|"
    r"privatekey|private_key|signature|salt|nonce",
    re.IGNORECASE)


def never_auto_approve(name: str) -> bool:
    """Whether `name` must never be captured without a person deciding.

    Applied to the NAMESPACED name, so `resp.AccessToken` is caught by `token` exactly as a bare
    `AccessToken` would be.
    """
    return bool(_NEVER_SEED.search(name))


def seeded(name: str) -> bool:
    """Whether a newly discovered field should arrive already approved.

    The veto is checked FIRST and independently, so this stays true even if a credential-shaped name is
    added to `SEED_FIELDS` by mistake.
    """
    return name in SEED_FIELDS and not never_auto_approve(name)


def _is_scalar(value) -> bool:
    """One value, not a container.

    `bool` counts as a scalar here, unlike in `contract.numeric_or_none`, and the difference is
    deliberate: a flag is a perfectly good DIMENSION (`resp.HasPackages`) and a useless MEASURE. This
    decides what to store; that decides what can be summed.
    """
    return value is None or isinstance(value, (str, int, float, bool))


def extract(entries) -> dict:
    """Every namespaced scalar in one transaction's response and mi_result entries.

    `entries` is an iterable of `(entry_type, fields)`. Returns `{namespaced name: value}` with NOTHING
    filtered - approval happens in `select`, so this stays a pure description of what the WMS said and
    a test can assert the two steps separately.

    LAST value wins when a transaction has several `mi_result` entries carrying the same key. A
    transaction can hold many M3 calls, so `mi.program` is genuinely ambiguous at this grain; the last
    is chosen because it is the call the response was built from. `mi.record_count` is SUMMED instead,
    since "how many records did this transaction see" is a total rather than a pick.
    """
    out: dict = {}
    for entry_type, fields in entries:
        if not isinstance(fields, dict):
            continue

        if entry_type == "response":
            # The payload nests one level under a key that is itself called `response`. An empty
            # response is the string "" rather than an absent key, which is why the isinstance check
            # is on the INNER value.
            payload = fields.get("response")
            if isinstance(payload, dict):
                for k, v in payload.items():
                    if _is_scalar(v):
                        out[f"{RESPONSE_PREFIX}{k}"] = v
            # Anything else at the top level of a response entry is metadata, not warehouse data.
            for k, v in fields.items():
                if k != "response" and _is_scalar(v):
                    out[f"{RESPONSE_PREFIX}{k}"] = v

        elif entry_type == "mi_result":
            for k in _MI_SCALARS:
                if k in fields and _is_scalar(fields[k]):
                    out[f"{MI_PREFIX}{k}"] = fields[k]
            records = fields.get(_MI_RECORDS)
            if isinstance(records, list):
                key = f"{MI_PREFIX}record_count"
                out[key] = (out.get(key) or 0) + len(records)
    return out


def select(observed: dict, approved: frozenset[str]) -> tuple[dict, list[str]]:
    """Split `observed` into what may be stored and what must only be reported.

    Returns `(captured, unknown_names)`. The second is NAMES ONLY - never values - which is what makes
    the discovery record itself safe: a newly appearing `SessionSecret` is reported so somebody can
    decide, and its value never touches the database.

    A field is captured only if `approved` says so. Seeding is applied when the discovery row is
    CREATED (see `seeded`), not here, so a field a person has since un-ticked stays un-ticked even
    though it is in `SEED_FIELDS` - a decision must outlive the default that preceded it.
    """
    captured, unknown = {}, []
    for name, value in observed.items():
        if name in approved:
            captured[name] = value
        else:
            unknown.append(name)
    return captured, sorted(unknown)


# ============================================================== R4: the per-record grain
#: Prefix for a field taken from one `records[]` entry. A third namespace beside `resp.` and `mi.`, so
#: a record's `ItemNumber` can never be confused with the response's or the request's - and so a metric
#: addresses it unambiguously as `attr:rec.ITNO`.
RECORD_PREFIX = "rec."

#: A single transaction expanding beyond this is logged loudly. Measured: 2.3 records per `mi_result`
#: entry on average, 26 at most - so this is far above anything normal and only fires when somebody has
#: ticked `expand` on something that returns a catalogue. It is a WARNING rather than a cap: silently
#: truncating would produce a record count that looks complete and is not.
LOUD_EXPANSION = 500


def records(entries) -> list[dict]:
    """Every `records[]` entry across one transaction's `mi_result` entries, flattened and namespaced.

    `entries` is an iterable of `(entry_type, fields)`, the same shape `extract` takes. Returns one dict
    per record, in the order they appeared, each carrying the record's own scalars plus the `mi_program`
    and `mi_transaction` they came from - because a transaction can hold several M3 calls and a record is
    meaningless without knowing which one answered.

    NOTHING is filtered here. Approval happens in `select`, exactly as for the scalar grain, so this
    stays a pure description of what the WMS said.

    Measured on live data: 3,765 record field values, ALL of them scalar. So a nested value inside a
    record has never been observed - but one is dropped rather than flattened if it appears, because
    inventing a key name no registry row could match would make the field permanently unapprovable.
    """
    out: list[dict] = []
    for entry_type, fields in entries:
        if entry_type != "mi_result" or not isinstance(fields, dict):
            continue
        recs = fields.get(_MI_RECORDS)
        if not isinstance(recs, list):
            continue
        program = fields.get("program") if _is_scalar(fields.get("program")) else None
        transaction = fields.get("transaction") if _is_scalar(fields.get("transaction")) else None
        for rec in recs:
            if not isinstance(rec, dict):
                continue
            out.append({
                "mi_program": program, "mi_transaction": transaction,
                "attributes": {f"{RECORD_PREFIX}{k}": v for k, v in rec.items() if _is_scalar(v)},
            })
    return out
