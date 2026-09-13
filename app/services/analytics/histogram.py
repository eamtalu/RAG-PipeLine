"""The 20-band log histogram behind the `percentile` aggregation (chunk 92).

Why a histogram. A percentile does not compose: the median of twelve monthly medians is not the yearly
median. Band COUNTS do compose - two hours' histograms add band by band, a day is the sum of its hours -
so what the rollup stores is how many values fell into each band, and the percentile is read out at the
end by walking the cumulative count. Same shape as every other role: additive parts in the table, the
finished answer computed once at read time.

Why powers of two. Warehouse durations span from a few milliseconds to minutes and quantities from one
to hundreds of thousands; a linear scale would waste every band on one end. Band 0 holds everything
below 1 (zero and negatives, which a correction can produce), band i holds [2^(i-1), 2^i), and the top
band clamps everything from 2^18 = 262,144 upwards. A band is therefore a factor of two wide, which is
the resolution of the answer: p95 = 3,000 ms means "somewhere in [2,048, 4,096), interpolated". The
catalog marks the aggregation approximate for exactly this reason.

In memory a histogram is a tuple of 20 ints; in the table it is a JSONB array of 20 ints; the empty
histogram is stored as NULL by `_rows_for`, which is why `EMPTY` is a value here and never written.
"""

from decimal import Decimal
from typing import Sequence

#: Number of bands. Fixed: changing it would make stored histograms and new ones un-addable.
BANDS = 20
#: Values at or above this land in the last band.
_TOP = 2 ** (BANDS - 2)

#: The histogram of no values.
EMPTY: tuple[int, ...] = (0,) * BANDS


def band_of(value: Decimal | int | float) -> int:
    """Which band `value` falls in. Band 0 below 1; band i is [2^(i-1), 2^i); the top band clamps."""
    v = float(value)
    if v < 1:
        return 0
    if v >= _TOP:
        return BANDS - 1
    band = 1
    edge = 2.0
    while v >= edge:
        band += 1
        edge *= 2
    return band


def lower_edge(band: int) -> int:
    return 0 if band == 0 else 2 ** (band - 1)


def upper_edge(band: int) -> int:
    """Exclusive upper edge. For the top band this is a nominal 2^19; values above it are clamped in."""
    return 2 ** band


def add(histogram: Sequence[int] | None, value: Decimal | int | float) -> tuple[int, ...]:
    """`histogram` with `value` counted into its band. A new tuple; histograms are values."""
    counts = list(histogram or EMPTY)
    if len(counts) < BANDS:
        counts += [0] * (BANDS - len(counts))
    counts[band_of(value)] += 1
    return tuple(counts)


def percentile(histogram: Sequence[int] | None, q: float) -> float | None:
    """The value below which a share `q` (0..1) of the counted values fall, estimated from the bands.

    Walks the cumulative count to the first band that exceeds q times the total, then interpolates
    linearly inside that band by how far into its count the target falls. The result always sits
    inside the crossing band's edges. None when nothing was counted: "no observations" and "a
    percentile of zero" are different facts.
    """
    if not histogram:
        return None
    total = sum(histogram)
    if total <= 0:
        return None
    target = q * total
    cumulative = 0
    for band, count in enumerate(histogram):
        if count and cumulative + count > target:
            fraction = (target - cumulative) / count
            lo, hi = lower_edge(band), upper_edge(band)
            return round(lo + fraction * (hi - lo), 3)
        cumulative += count
    # q at or above 1, or floating error at the very end: the top populated band's upper edge.
    last = max(i for i, c in enumerate(histogram) if c)
    return float(upper_edge(last))
