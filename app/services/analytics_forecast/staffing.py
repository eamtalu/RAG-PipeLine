"""From lines to people: how many pickers an hour's forecast needs."""

from __future__ import annotations

import math


def pickers_needed(lines: float, lines_per_picker_hour: float | None, *, buffer_pct: float) -> int | None:
    """Whole pickers to clear `lines` in the hour, with `buffer_pct` headroom, rounded up.

    None when the throughput is unknown or zero: a number from nothing would still be rostered."""
    if not lines_per_picker_hour or lines_per_picker_hour <= 0:
        return None
    if lines <= 0:
        return 0
    return int(math.ceil(lines / lines_per_picker_hour * (1.0 + buffer_pct)))
