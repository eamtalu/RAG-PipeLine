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
from dataclasses import dataclass, field as dc_field
from decimal import Decimal

from app.services.analytics import contract

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


#: The doc's composition table, executable. This is the single place that decides what a rollup row
#: has to carry for a given aggregation.
_ROLES: dict[Aggregation, frozenset[Role]] = {
    Aggregation.sum: frozenset({Role.sum_value, Role.count_value}),
    Aggregation.count: frozenset({Role.count_value}),
    Aggregation.average: frozenset({Role.sum_value, Role.count_value}),
    Aggregation.stats: frozenset({Role.sum_value, Role.count_value, Role.sum_sq}),
    Aggregation.extent: frozenset({Role.min_value, Role.max_value}),
    Aggregation.percentile: frozenset({Role.histogram}),
}


def roles_for(aggregation: Aggregation) -> frozenset[Role]:
    return _ROLES[aggregation]


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
             known_attributes: frozenset[str] | set[str] | None = None) -> list[str]:
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
    """
    problems: list[str] = []
    known = frozenset(known_attributes or ())

    def _bad_field(name: str) -> str | None:
        """Why `name` is not usable, or None when it is. Shared by dimensions and measure fields so
        the two cannot drift into accepting different things."""
        if contract.is_attr_path(name):
            key = contract.attr_key(name)
            if key not in known:
                return (f"{name!r} names an attribute that is not approved: {key!r} is absent from "
                        f"the field registry, so it is either a typo or a field nobody has ticked "
                        f"for capture. Reading it would be silently empty rather than an error")
            return None
        if name not in contract.FACT_FIELDS:
            return (f"{name!r} is not a field on the fact row: reading it would be silently empty "
                    f"rather than an error")
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
        for st in sorted(m.statuses):
            if st not in _STATUSES:
                problems.append(f"measure {m.name!r} filters on status {st!r}, which log_transactions "
                                f"never emits; it would contribute nothing and look like no data")
        # N4's first rule. Summing a quantity over methods that carry none yields a confident zero.
        if m.field == "quantity":
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
            Role.min_value: None, Role.max_value: None, Role.histogram: ()}
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
