"""Post-processors over the tool trace, one set per presentation name a profile points at.

`evidence.py` draws the analytics table and its card data; `logspace_evidence.py` the logspace
variant. `presentation_for(name)` is what the runtime calls with `profile.presentation`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from app.agent.presentation import blocks as answer_blocks
from app.agent.presentation import evidence, logspace_evidence
from app.agent.presentation.links import at_risk_link, page_link


@dataclass(frozen=True, slots=True)
class Presentation:
    """How a profile's answers are finished: the table, the card data, its own tables cut, and
    the typed blocks its presentation calls asked for (step 5)."""

    name: str
    render: Callable[[list[dict]], str | None]
    structured: Callable[[list[dict], str], dict | None]  # (trace, public_base_url)
    strip: Callable[[str], str]
    blocks: Callable[[list[dict]], list[dict]] = lambda trace: []


def _analytics_structured(trace: list[dict], public_base_url: str) -> dict | None:
    return evidence.structured(
        trace, link=page_link(public_base_url), at_risk_link=at_risk_link(public_base_url)
    )


_PRESENTATIONS = {
    "analytics": Presentation(
        "analytics",
        evidence.render,
        _analytics_structured,
        evidence.strip_tables,
        answer_blocks.blocks_from_trace,
    ),
    "logspace": Presentation(
        "logspace",
        logspace_evidence.render,
        lambda trace, base: None,
        logspace_evidence.strip_own_tables,
    ),
}


def presentation_for(name: str) -> Presentation:
    """The presentation a profile names.

    >>> presentation_for("logspace").name
    'logspace'
    >>> presentation_for("nope")
    Traceback (most recent call last):
    ...
    ValueError: no presentation 'nope'; the presentations are ['analytics', 'logspace']
    """
    try:
        return _PRESENTATIONS[name]
    except KeyError:
        raise ValueError(
            f"no presentation {name!r}; the presentations are {sorted(_PRESENTATIONS)}"
        ) from None
