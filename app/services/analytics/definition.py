"""The metric registry's shape, and consumption as ONE row in it. Phase 0.

The doc is emphatic, in bold: *"nothing about dimensions or measures may be hardcoded into a rollup
schema"*, and N4 requires *"a registry, not an if-chain"*. So the three consumption counters live
nowhere as constants. They are the measures of one `MetricDefinition`, and every function here reads
what to do off the definition it is handed. A second definition over the same rows produces different
counters with no code change, which is the property the whole user-configurable design rests on.

`MetricDefinition` is the in-memory twin of a future `analytics_metrics` row (Phase 1), the way
`NotificationRule` is for notifications. Phase 0 owns the shape and the validation; the table, the API
that writes it and the interface that drives it come later. Nothing here touches a database.

Additive roles, and why the columns are named for them
------------------------------------------------------
Invariant 8 says a rollup stores additive components, never finished answers, because averaging twelve
monthly averages is not the yearly average. The doc's own composition table fixes the complete set of
additive primitives: sum, count, sum of squares, min, max, and a bucketed histogram. Every legal
measure decomposes into exactly those.

So the rollup columns are named for the roles rather than numbered `measure1..measure8`. Three
consequences, in order of how much they matter:

*A finished answer becomes unrepresentable.* There is no column an average could be written to. Today
"never store a finished answer" is a convention someone has to catch in review; here the schema refuses
it. `average` is an AGGREGATION that declares sum+count and divides at read time.

*Folding becomes one uniform operation.* `add_roles` sums the sums, sums the counts, mins the mins,
maxes the maxes and adds the histograms element-wise. It never asks which measure it is looking at,
which is the registry-not-if-chain requirement satisfied structurally rather than by discipline.

*It is self-describing.* `dim3` tells a reader nothing; `sum_sq` tells them everything.

The cost, chosen deliberately: consumption needs one sum and TWO counts, which cannot share one set of
role columns. So a rollup row is keyed per (definition, measure) and consumption emits three rows per
bucket instead of one. That is roughly 3x on the hourly rollup table, about 9.6M rows at five years
against the doc's 3.2M estimate. Small enough to accept, large enough to state.
"""

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field as dc_field
from datetime import datetime
from decimal import Decimal

from app.services.analytics import contract
from app.services.analytics import histogram as hg
from app.services.analytics import hll

#: Grains a definition may ask for. `weekly` has no table of its own: ISO Monday weeks derive from
#: daily at read time, because a week is not a partition boundary anywhere.
GRAINS: tuple[str, ...] = ("hourly", "daily", "weekly", "monthly")


class Role(enum.Enum):
    """An additive primitive a rollup can store. Deliberately the ONLY things it can store.

    Nothing that fails to compose appears here, which is what makes invariant 8 structural: there is
    no column an average, rate, median or standard deviation could be written into.
    """

    sum_value = "sum_value"
    count_value = "count_value"
    sum_sq = "sum_sq"
    min_value = "min_value"
    max_value = "max_value"
    histogram = "histogram"
    #: Chunk 88: a HyperLogLog sketch, `bytes`. Unions register-wise, so a month's distinct count is
    #: folded from its days like every other role. An estimate; the catalog says so.
    distinct_sketch = "distinct_sketch"


class Aggregation(enum.Enum):
    """What a measure computes. Each declares the roles it needs stored.

    `average` and `percentile` are listed as aggregations even though neither is additive: that is the
    point. They declare additive components and are finished at READ time, so a monthly average is
    computed from a month of sums and counts rather than from thirty daily averages.
    """

    sum = "sum"
    count = "count"
    average = "average"
    stats = "stats"            # sum, count, sum_sq -> variance and stddev at read time
    extent = "extent"          # min and max -> first and last
    percentile = "percentile"  # 20-bucket log histogram -> median, p95 at read time
    distinct = "distinct"      # HyperLogLog sketch -> approximate count of different values at read time


#: The doc's composition table, executable. This is the single place that decides what a rollup row
#: has to carry for a given aggregation.
_ROLES: dict[Aggregation, frozenset[Role]] = {
    Aggregation.sum: frozenset({Role.sum_value, Role.count_value}),
    Aggregation.count: frozenset({Role.count_value}),
    Aggregation.average: frozenset({Role.sum_value, Role.count_value}),
    Aggregation.stats: frozenset({Role.sum_value, Role.count_value, Role.sum_sq}),
    Aggregation.extent: frozenset({Role.min_value, Role.max_value}),
    # Chunk 92: the exact count beside the bands, as for `distinct` - the denominator, and what
    # `_is_empty` reads.
    Aggregation.percentile: frozenset({Role.histogram, Role.count_value}),
    # The count beside the sketch is exact and free: rows that carried a value, which is the honest
    # denominator for "N different items over M picks" and what `_is_empty` reads.
    Aggregation.distinct: frozenset({Role.distinct_sketch, Role.count_value}),
}


def roles_for(aggregation: Aggregation) -> frozenset[Role]:
    return _ROLES[aggregation]


#: Aggregations that read the VALUE and do arithmetic on it, as against those that read the row.
#: `count` counts rows and `distinct` counts how many different values there were; neither cares what
#: the value means. Everything else adds, divides or orders it.
_ARITHMETIC: frozenset[Aggregation] = frozenset({
    Aggregation.sum, Aggregation.average, Aggregation.stats,
    Aggregation.extent, Aggregation.percentile,
})

#: What each KIND of field refuses (chunks 109 and 110). Keyed by the strings in
#: `analytics_field_meaning.KINDS`, named here rather than imported so this module keeps its
#: "no database" property, exactly as `_STATUSES` is. A test asserts the two agree, and asserts every
#: kind appears, so a fifth one cannot be added to the model and silently refuse nothing.
#:
#: `measure` refuses nothing: a number that says how much HAPPENED is what every aggregation is for.
#:
#: `level` refuses only the sum. A level says how much there IS at a moment, so adding readings gives
#: a number nothing ever was - 73 on-hand readings of one item add to 41,206 where 427 are on the
#: shelf. The average, the extent, the median and the spread are all real questions about a level, and
#: refusing a harmless one teaches people the rule is arbitrary so they work around the useful part.
#:
#: `slice` refuses all arithmetic. A name has no size. Measured on tmp-live: `DeliveryNumber` holds
#: 8,841 values that are 100 per cent digits and `ExpectedQuantity` holds 3,556 that are also 100 per
#: cent digits; one is a name and one is a quantity, and nothing in the values says which. Adding up
#: `PackageNumber` parses nothing and reads as no data, which is loud; adding up `DeliveryNumber`
#: gives a confident seven-figure total that is meaningless, which is not. Counting rows and counting
#: different values stay allowed, because those are the two questions a name answers.
#:
#: `noise` refuses nothing. It says "nobody will report on this", not "this is forbidden". Somebody
#: reporting on it means the marking was wrong, and the fix is to change the marking.
REFUSED_BY_KIND: dict[str, frozenset[Aggregation]] = {
    "measure": frozenset(),
    "level": frozenset({Aggregation.sum}),
    "slice": _ARITHMETIC,
    "noise": frozenset(),
}


def refuses(kind: str | None, aggregation: Aggregation) -> bool:
    """Whether a field of this kind may not be read that way.

    An unknown or absent kind refuses nothing. Nobody has decided, and a screen that refused on a
    blank would punish people for not having got to the field yet.
    """
    return aggregation in REFUSED_BY_KIND.get(kind or "", frozenset())


def refused_kinds(aggregation: Aggregation) -> tuple[str, ...]:
    """Which kinds refuse this aggregation. Published in the catalog so a screen greys out exactly
    what the server refuses and keeps no second copy of the rule."""
    return tuple(sorted(k for k, refused in REFUSED_BY_KIND.items() if aggregation in refused))


class Status(enum.Enum):
    """N4's lifecycle. A definition cannot go active until its backfill has run, or its chart shows a
    false start date: no history, drawn as though there were none to have."""

    draft = "draft"
    active = "active"
    inactive = "inactive"


@dataclass(frozen=True)
class Measure:
    """One number a definition tracks.

    Declarative on purpose: `(name, aggregation, field, only)` is storable as a registry row and
    needs no code to interpret. A user inventing "average duration of ConfirmPickLine" writes a row;
    nothing dispatches on the measure's name anywhere.

    `field` is a fact-row column, or None for a pure count. `only` restricts which classifications
    contribute, which is how `pick_count` (above zero) and `attempt_count` (every confirmation) differ
    without either being special-cased.
    """

    name: str
    aggregation: Aggregation
    field: str | None = None
    only: frozenset = dc_field(default_factory=frozenset)
    #: Which transaction statuses contribute. Empty means every status, which is what a volume or an
    #: error-rate measure wants.
    #:
    #: Measured, and the reason this field exists: an errored `ConfirmPickLine` still carries
    #: `QuantityPicked = 10.0`, because the quantity is stated on the REQUEST line and the error is
    #: whatever came after it. So does an `incomplete` one. Without this filter both summed into
    #: consumption, and the Phase 0 fixtures asserted the opposite intent -- "a hard failure carries no
    #: units" -- while modelling those rows as quantity-absent, which is not what the projection
    #: produces. The fixture agreed with itself and nothing else.
    #:
    #: Per MEASURE rather than per definition, for the same reason `only` is: one definition can then
    #: hold both a total and an error count, differing only by this set.
    statuses: frozenset = dc_field(default_factory=frozenset)
    #: Chunk 89: what the number is in ("units", "ms", "kg"). Metadata for the catalog and the chat
    #: agent; the fold never reads it. None for every pre-builder measure.
    unit: str | None = None
    #: Chunk 101: a second field, subtracted from `field` before the aggregation sees the value.
    #:
    #: The most valuable number in a warehouse is a difference, and until this existed no definition
    #: could express one. Both halves already sit on the same record: a pick carries `QuantityPicked`
    #: beside `ExpectedQuantity`, a count carries `CountedQuantity` beside `BalanceQuantity`. So pick
    #: shortfall and count variance need no new data, only the subtraction.
    #:
    #: ONE operation, deliberately, not an expression language. Every case measured is "expected
    #: against actual"; parsing, precedence and their own validation buy nothing the data asks for. A
    #: second operation can be added the day something needs it.
    #:
    #: A row missing EITHER half contributes nothing, by the same rule that skips an absent quantity:
    #: reading a missing expected quantity as zero would report every such pick as a full shortfall.
    #:
    #: Chunk 109 made this subtraction the one way a LEVEL may still be added up. `CountedQuantity`
    #: and `BalanceQuantity` are both stock readings, and adding readings is refused - but a stock
    #: minus a stock is a CHANGE, and changes add. So count variance keeps working, and needs both
    #: halves marked a level for the rule to recognise it as a difference rather than a mixture.
    minus: str | None = None

    @property
    def roles(self) -> frozenset[Role]:
        return roles_for(self.aggregation)


@dataclass(frozen=True)
class MetricDefinition:
    """The in-memory twin of an `analytics_metrics` row.

    `method_filter` empty means every method, which is what a volume or duration metric wants: 46 of
    49 methods carry no quantity, and their measures are volume, duration, status and actor.
    """

    name: str
    dimensions: tuple[str, ...]
    measures: tuple[Measure, ...]
    grains: tuple[str, ...]
    method_filter: tuple[str, ...] = ()
    #: R1. The registry's `show` switch, expressed on the definition. Needed as well as
    #: `method_filter` because the mapping is many-to-many: `ConfirmPickLine` appears under both
    #: "Brighton Stock Pick" and "JIT and Shorts Pick (Brighton)", so no method-keyed filter can say
    #: "one on, the other off". Empty means every transaction, matching `method_filter`'s convention.
    transaction_filter: tuple[str, ...] = ()
    status: Status = Status.draft
    #: 18y: which fact table this metric folds and reads - "transaction" (the default, every metric
    #: before R4b) or "record" (one row per M3 record, R4). A promoted column on the registry row,
    #: because the fold partitions definitions by it every cycle. Deliberately NOT called "grain":
    #: `grains` already means time resolution, and one name for two axes is how a thing gets built
    #: twice.
    source: str = "transaction"
    #: Chunk 86: where this metric's history starts, or None for unbounded. The fold reads no fact
    #: before it and the read layer clamps requests to it. A value, not a behaviour: this module stays
    #: free of clocks and databases, so who sets it and what it triggers live in the registry and API.
    rollups_from: datetime | None = None


#: The classifications that represent a usable confirmation on a quantity-carrying method. A row that
#: is `non_quantity` or `unusable` is outside every quantity counter, and in particular must never
#: reach a denominator as a zero.
_CONFIRMED = frozenset({contract.Classification.pick,
                        contract.Classification.attempt,
                        contract.Classification.correction})

#: The statuses that mean units are known to have moved. `log_transactions.status` has four values.
#:
#: `error` is a real ERROR-level failure and `incomplete` means the RESPONSE has not been ingested yet,
#: so neither is evidence of a completed pick -- and excluding `incomplete` is self-correcting, since a
#: later Stage 2 pass closes it and the row then counts under its real status.
#:
#: `soft` ("M3 returned not-found/needs-value but the app coped") is excluded on a DELIBERATE lack of
#: evidence rather than a judgement: if the ERP returned not-found, the confirmation did not register in
#: the system of record. Measured on the live server, ZERO soft rows carry a quantity-bearing method
#: (69 soft transactions exist, all on the other 46 methods), so this choice moves no current number --
#: which is exactly why it is safe to make it the strict one and revisit it with data.
_COMPLETED = frozenset({"success"})

#: The seed definition (F8). Note what this is NOT: a module constant listing the system's counters.
#: It is one registry row, and the interface will write others beside it.
CONSUMPTION = MetricDefinition(
    name="consumption",
    dimensions=("method", "transaction_name"),
    measures=(
        # Signed, so a correction reduces the total rather than inflating it.
        Measure("quantity", Aggregation.sum, field="quantity", only=_CONFIRMED,
                statuses=_COMPLETED),
        # Strictly above zero.
        Measure("pick_count", Aggregation.count, only=frozenset({contract.Classification.pick}),
                statuses=_COMPLETED),
        # Every usable confirmation, zero-unit ones included. The zero-pick rate is this minus
        # pick_count, over this, computed at read time. It carries the SAME status filter as
        # pick_count on purpose: a denominator drawn from a wider set than its numerator would make
        # the rate wrong in a way that looks plausible.
        Measure("attempt_count", Aggregation.count, only=_CONFIRMED, statuses=_COMPLETED),
    ),
    grains=("hourly", "daily", "monthly"),
    method_filter=tuple(contract.QUANTITY_FIELD),
)


# ============================================================== validation (what N4 enforces)
#: Every value `log_transactions.status` can hold. Named here rather than imported from the model so
#: this module keeps its "no database" property; a test asserts the two agree.
_STATUSES = frozenset({"success", "soft", "error", "incomplete"})


def validate(definition: MetricDefinition,
             known_attributes: frozenset[str] | set[str] | None = None,
             *,
             field_kinds: Mapping[str, str] | None = None) -> list[str]:
    """Problems with `definition`, empty when it is registrable.

    Returns a list rather than raising: the interface shows all of them at once, and a half-valid
    definition should not be reported one error per save.

    R1b. `known_attributes` are the `attributes` keys this tenant has APPROVED for capture, which in
    practice is `analytics_field_registry` where `captured` is true. Passed in rather than queried
    because this module has no database access and Phase 0 tests it without one - giving it a database
    would end that property. The registry stays the authority; this module never learns it exists.

    Omitting it refuses every `attr:` path. That is failing CLOSED, and it is deliberate: a caller who
    forgot the argument must not accidentally accept any attribute path at all, because that would make
    the allowlist optional for a table that is KEEP_FOREVER.

    Chunks 109 and 110. `field_kinds` maps an attribute key to what a PERSON said it IS: a measure, a
    level, a slice or noise. Supplied the same way and for the same reason: this module never learns
    that `analytics_field_meanings` exists. `REFUSED_BY_KIND` says what each kind refuses.

    Its default is the exact INVERSE of `known_attributes`', and the inversion is the whole design.
    Omitting `known_attributes` refuses everything, because an unapproved field would write to a table
    kept forever. Omitting `field_kinds` refuses NOTHING, because the caller that omits it is the
    fold, and a fold that stopped folding a metric somebody has been reading for months is a far worse
    failure than a wrong label on a chart. This refusal is a gate on the way IN - preview, create, a
    reshaped metric, a draft first going live - never a re-argument of a decision already taken.

    An undecided field also refuses nothing. Absent is not a decision, and refusing on a blank would
    punish people for not yet having reached the field.
    """
    problems: list[str] = []
    known = frozenset(known_attributes or ())
    kinds = dict(field_kinds or {})

    def _kind_of(name: str | None) -> str | None:
        """What a person said this field IS, or None when nobody has said.

        Matched on the KEY, because a metric addresses a field as `attr:resp.QuantityOnHand` while the
        meaning row stores `resp.QuantityOnHand`; comparing the two spellings would match nothing and
        refuse nothing, which is a silent failure rather than a loud one. Namespaced in full, because
        `resp.QuantityOnHand` being a level says nothing about a request field spelled
        `QuantityOnHand`, and inheriting a marking nobody made is how the wrong field gets trusted.

        A typed column such as `quantity` has no kind: no meaning row can describe one.
        """
        if not name or not contract.is_attr_path(name):
            return None
        return kinds.get(contract.attr_key(name))

    record_grain = definition.source == "record"
    if definition.source not in ("transaction", "record"):
        problems.append(f"source {definition.source!r} is not 'transaction' or 'record'")

    def _bad_field(name: str) -> str | None:
        """Why `name` is not usable, or None when it is. Shared by dimensions and measure fields so
        the two cannot drift into accepting different things. Source-aware since 18y, failing closed
        in BOTH directions: a transaction metric naming `attr:rec.*` used to validate fine and chart
        silently empty (record attributes never appear on a fact row), and a record metric naming a
        fact-only column or a `resp.`/`mi.` attribute would do the same one table over."""
        if contract.is_attr_path(name):
            key = contract.attr_key(name)
            if record_grain and not key.startswith("rec."):
                return (f"{name!r} is not a record attribute: a record metric reads "
                        f"`analytics_record_facts`, whose attributes are all `rec.`-prefixed")
            if not record_grain and key.startswith("rec."):
                return (f"{name!r} is a record attribute, which never appears on a transaction "
                        f"fact row - define the metric with source='record' instead")
            if key not in known:
                return (f"{name!r} names an attribute that is not approved: {key!r} is absent from "
                        f"the field registry, so it is either a typo or a field nobody has ticked "
                        f"for capture. Reading it would be silently empty rather than an error")
            return None
        fields = contract.RECORD_FIELDS if record_grain else contract.FACT_FIELDS
        if name not in fields:
            return (f"{name!r} is not a field on the {definition.source} row: reading it would be "
                    f"silently empty rather than an error")
        return None

    for dim in definition.dimensions:
        bad = _bad_field(dim)
        if bad:
            problems.append(f"dimension {bad}")

    # Deliberately NOT validated against known transaction names. Unlike `dimensions`, which must name
    # a real fact field or the chart is silently empty, a transaction filter naming something not yet
    # seen is legitimate: a metric can be registered before its transaction first appears in the logs,
    # and the registry discovers names rather than declaring them.
    for grain in definition.grains:
        if grain not in GRAINS:
            problems.append(f"grain {grain!r} is not one of {', '.join(GRAINS)}")

    for m in definition.measures:
        if m.field:
            bad = _bad_field(m.field)
            if bad:
                problems.append(f"measure {m.name!r}: {bad}")
        if m.aggregation is not Aggregation.count and not m.field:
            problems.append(f"measure {m.name!r} is a {m.aggregation.value} but names no field")
        if m.minus:
            # Validated exactly like `field`, and for a sharper reason: an unapproved or misspelled
            # second half makes EVERY row contribute nothing, which reads as "no shortfall" - the
            # most dangerous wrong answer this measure could give.
            bad = _bad_field(m.minus)
            if bad:
                problems.append(f"measure {m.name!r} subtracts {bad}")
            if not m.field:
                problems.append(f"measure {m.name!r} subtracts {m.minus!r} but names no field to "
                                f"subtract it from")
            if m.aggregation is Aggregation.count:
                problems.append(f"measure {m.name!r} is a count, which reads no value, so it cannot "
                                f"subtract {m.minus!r}")
            if m.aggregation is Aggregation.distinct:
                problems.append(f"measure {m.name!r} is a distinct count, whose field is an identity "
                                f"rather than a quantity, so it cannot subtract {m.minus!r}")
        # Chunks 109 and 110. What a person said the field IS decides how it may be read.
        #
        # A SLICE is checked first, because it is the stronger refusal and the quieter failure. A
        # name has no size: the average of delivery 24960 and delivery 27946 is not a delivery.
        # `PackageNumber` is never numeric so adding it up reads as no data, which is loud; every
        # `DeliveryNumber` on the live tenant IS numeric, so adding those up gives a confident
        # seven-figure total that is meaningless, which is not.
        slice_names = [n for n in (m.field, m.minus) if _kind_of(n) == "slice"]
        if slice_names and refuses("slice", m.aggregation):
            problems.append(
                f"measure {m.name!r} does arithmetic on {', '.join(repr(n) for n in slice_names)}, "
                f"which is a NAME you group by, not a quantity - it has no size, so a total, an "
                f"average and a highest are all meaningless. Group by it instead, or count the rows "
                f"with `count`, or count how many different ones there were with `distinct`.")
        # A LEVEL is how much there IS at a moment. Adding readings of the same shelf produces a
        # number nothing ever was: 73 on-hand readings of item 104353 add to 41,206 where 427 are on
        # the shelf. Only `sum` is refused, and only when there is no second level to subtract,
        # because a stock minus a stock is a CHANGE and changes add - that difference is count
        # variance, which is the whole reason `minus` exists.
        elif refuses("level", m.aggregation) and "level" in (_kind_of(m.field), _kind_of(m.minus)):
            if _kind_of(m.field) != "level" or _kind_of(m.minus) != "level":
                if m.minus:
                    is_level = _kind_of(m.field) == "level"
                    stock, flow = (m.field, m.minus) if is_level else (m.minus, m.field)
                    problems.append(
                        f"measure {m.name!r} adds up {stock!r}, which is a LEVEL - how much there is "
                        f"at a moment - against {flow!r}, which is an amount. A stock minus a flow is "
                        f"neither, so the total means nothing. Subtract a second level to get a "
                        f"change, or use average, extent or percentile.")
                else:
                    problems.append(
                        f"measure {m.name!r} adds up {m.field!r}, which is a LEVEL: how much there is "
                        f"at a moment, not how much happened. Adding readings produces a number "
                        f"nothing ever was - 73 on-hand readings of one item add to 41,206 where "
                        f"427 are on the shelf. Use average, extent or percentile, or subtract a "
                        f"second level to get a change.")
        if record_grain and m.statuses:
            problems.append(f"measure {m.name!r} filters on status, but record rows carry no "
                            f"status - the filter would exclude every row and look like no data")
        if record_grain and m.only:
            problems.append(f"measure {m.name!r} filters on quantity classification, but record "
                            f"rows carry none - the filter would exclude every row")
        for st in sorted(m.statuses):
            if not record_grain and st not in _STATUSES:
                problems.append(f"measure {m.name!r} filters on status {st!r}, which log_transactions "
                                f"never emits; it would contribute nothing and look like no data")
        # N4's first rule. Summing a quantity over methods that carry none yields a confident zero.
        if m.field == "quantity" and not record_grain:
            methods = definition.method_filter or ()
            if not methods or any(not contract.carries_quantity(x) for x in methods):
                problems.append(
                    f"measure {m.name!r} sums 'quantity', so method_filter must name only methods that "
                    f"carry one ({', '.join(sorted(contract.QUANTITY_FIELD))}); "
                    f"got {list(methods) or 'no filter at all'}")
    return problems


# ============================================================== folding
def _empty_roles(measure: Measure) -> dict:
    """A zero bucket for one measure. min/max start as None so the first real value wins rather than
    competing with a sentinel that could never be exceeded."""
    zero = {Role.sum_value: Decimal(0), Role.count_value: 0, Role.sum_sq: Decimal(0),
            Role.min_value: None, Role.max_value: None, Role.histogram: hg.EMPTY,
            Role.distinct_sketch: hll.EMPTY}
    return {r: zero[r] for r in measure.roles}


def empty(definition: MetricDefinition) -> dict:
    """A zero fold for every measure. The identity for `add`, so a rollup can start from nothing."""
    return {m.name: _empty_roles(m) for m in definition.measures}


def _contributes(row: dict, definition: MetricDefinition, measure: Measure) -> bool:
    """Whether `row` is inside this definition's filter, this measure's classifications, and its
    statuses."""
    if definition.method_filter and row.get("method") not in definition.method_filter:
        return False
    # R1. Checked on `transaction_name`, which is on the fact row, so this needs no join and no new
    # column. A row whose name is NULL is outside every transaction filter: the unnamed rows are the
    # connectivity probes, which capture keeps but never shows.
    if definition.transaction_filter and row.get("transaction_name") not in definition.transaction_filter:
        return False
    if measure.only and row.get("quantity_classification") not in measure.only:
        return False
    if measure.statuses and row.get("status") not in measure.statuses:
        return False
    return True


def fold(rows, definition: MetricDefinition) -> dict:
    """Fold fact rows into `{measure name: {role: value}}`, driven entirely by `definition`.

    One bucket per MEASURE rather than one per definition, because a definition with a sum and two
    counts cannot share one set of role columns. That is what makes the rollup row key
    (definition, measure, dimensions, bucket).
    """
    out = empty(definition)
    for row in rows:
        for m in definition.measures:
            if not _contributes(row, definition, m):
                continue
            bucket = out[m.name]
            if Role.distinct_sketch in bucket:
                # Chunk 88: the field is an IDENTITY, not a quantity. A user name or item number must
                # not be dropped for failing numeric coercion, so this path never goes near it.
                key = contract.distinct_key(contract.resolve_field(row, m.field))
                if key is None:
                    continue      # absent is never a value, same rule as below
                bucket[Role.distinct_sketch] = hll.add(bucket[Role.distinct_sketch], key)
                if Role.count_value in bucket:
                    bucket[Role.count_value] += 1
                continue
            # R1b: resolved rather than read, so a measure may name `attr:resp.QuantityOnHand`, and
            # coerced because a JSONB value is whatever the WMS logged - the live M3 records carry
            # `"STQT": "624"`, a STRING, which would raise TypeError on `+=` below.
            #
            # A value that cannot be coerced is skipped by the SAME rule a NULL quantity already is.
            # Skipping rather than counting zero is the load-bearing half: a denominator drawn from
            # rows that contributed no value makes every rate wrong in a plausible-looking way.
            value = contract.numeric_or_none(contract.resolve_field(row, m.field)) if m.field else None
            if m.field and value is None:
                continue          # absent is never zero, so it contributes to nothing at all
            if m.minus:
                # Chunk 101. Subtracted BEFORE the aggregation, so a difference composes with sum,
                # average, stats, extent and percentile rather than being a special kind of sum. The
                # same "absent is never zero" rule applies to the second half.
                other = contract.numeric_or_none(contract.resolve_field(row, m.minus))
                if other is None:
                    continue
                value = value - other
            if Role.count_value in bucket:
                bucket[Role.count_value] += 1
            if Role.sum_value in bucket:
                bucket[Role.sum_value] += value
            if Role.sum_sq in bucket:
                bucket[Role.sum_sq] += value * value
            if Role.min_value in bucket:
                cur = bucket[Role.min_value]
                bucket[Role.min_value] = value if cur is None else min(cur, value)
            if Role.max_value in bucket:
                cur = bucket[Role.max_value]
                bucket[Role.max_value] = value if cur is None else max(cur, value)
            if Role.histogram in bucket:
                # Chunk 92: one band count per value. Band counts add across hours and days, which is
                # the only reason a percentile is storable in a rollup at all.
                bucket[Role.histogram] = hg.add(bucket[Role.histogram], value)
    return out


def add_roles(a: dict, b: dict) -> dict:
    """Merge two role buckets. Uniform: it never asks which measure it is looking at.

    That uniformity IS the registry requirement. Any per-measure branch here would be the if-chain the
    doc rules out, and would have to be extended for every new metric.
    """
    out = {}
    for role in set(a) | set(b):
        x, y = a.get(role), b.get(role)
        if role in (Role.sum_value, Role.count_value, Role.sum_sq):
            out[role] = (x or 0) + (y or 0)
        elif role is Role.min_value:
            out[role] = min([v for v in (x, y) if v is not None], default=None)
        elif role is Role.max_value:
            out[role] = max([v for v in (x, y) if v is not None], default=None)
        elif role is Role.distinct_sketch:
            out[role] = hll.union(x or hll.EMPTY, y or hll.EMPTY)
        else:  # histogram: bucket counts add, which is why percentiles are stored this way
            xs, ys = x or (), y or ()
            width = max(len(xs), len(ys))
            out[role] = tuple((xs[i] if i < len(xs) else 0) + (ys[i] if i < len(ys) else 0)
                              for i in range(width))
    return out


def add(a: dict, b: dict) -> dict:
    """Sum two folds measure by measure. This is how hourly composes into daily into monthly."""
    return {name: add_roles(a.get(name, {}), b.get(name, {})) for name in set(a) | set(b)}


# ============================================================== finished answers, at READ time
def plain_number(value: Decimal) -> str:
    """A Decimal as plain digits: trailing zeros trimmed, and NEVER in scientific notation.

    Chunk 98. The obvious spelling, `str(value.normalize())`, is wrong in a way that only shows on
    round numbers: `normalize` trims trailing zeros by RAISING the exponent, so `Decimal("40")`
    becomes `4E+1` and a chart draws "4E+1" where it means forty. Seen live on a stock move preview.
    The `f` presentation type is the fixed-point spelling, so it tidies without ever going
    exponential.
    """
    return format(value.normalize(), "f")


def public_roles(roles: dict, *, number=plain_number) -> dict:
    """A role bucket as it leaves the service: JSON-safe, and the sketch finished into an integer.

    The ONE place a `distinct_sketch` becomes a number and a `histogram` becomes p50 and p95. Every
    reader - `/series`, the preview sample, the agent tools - goes through here, so none of them can
    leak 4 KB of bytes or a band list into a response or present an estimate under a name that hides
    what it is. Decimals go through `number`, because
    two callers already format them differently and this is not the chunk to unify that.
    """
    out: dict = {}
    for role, value in roles.items():
        if role is Role.distinct_sketch:
            if value:
                out["distinct_estimate"] = hll.estimate(value)
        elif role is Role.histogram:
            # Chunk 92: finished here, once. The band list never leaves the service; p50 and p95 are
            # what a chart, the wizard and the chat agent need, and a band is a factor of two wide,
            # so the catalog calls the aggregation approximate.
            p50 = hg.percentile(value, 0.5)
            if p50 is not None:
                out["p50"] = p50
                out["p95"] = hg.percentile(value, 0.95)
        elif isinstance(value, Decimal):
            out[role.value] = number(value)
        else:
            out[role.value] = value
    return out


def average(bucket: dict) -> Decimal | None:
    """Divided here, never stored. None when there was nothing to average, because 0 would read as
    "the average was zero" rather than "there was no data"."""
    n = bucket.get(Role.count_value) or 0
    if not n:
        return None
    return bucket[Role.sum_value] / Decimal(n)


def zero_pick_rate(folded: dict) -> Decimal | None:
    """Share of confirmations that picked nothing (F8), from the two stored counters.

    Derived rather than stored, so it composes: twelve monthly rates cannot be averaged into a yearly
    one, but twelve pairs of counts can be added and divided once.
    """
    total = folded.get("attempt_count", {}).get(Role.count_value) or 0
    if not total:
        return None
    picked = folded.get("pick_count", {}).get(Role.count_value) or 0
    return Decimal(total - picked) / Decimal(total)
