"""The areas a preference can be about, each with the shape of its settings.

An area is a pydantic model whose every field has a default, so "nothing chosen" is always a
complete answer. A new setting is a new field with a default or a new area in `AREAS`; it never
needs a new table. Used by the store, the resolver and the commands of this package.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError


class _Area(BaseModel):
    """Unknown fields are refused, otherwise a typo would be stored and silently do nothing."""

    model_config = ConfigDict(extra="forbid")


class App(_Area):
    """How the app opens: the site it starts on, and whether Ask shows past conversations."""

    #: A site id, "all" for the all-sites view, or None for "the app decides".
    default_site: str | None = None
    #: Open until the person hides it, as the app has always started.
    is_history_open: bool = True


class AdvisoriesView(_Area):
    """How the Advisories list is laid out; the same values the app's view menu offers."""

    group_by: Literal["kind", "area", "item", "location", "severity", "agent", "holder",
                      "none"] = "kind"
    sort_by: Literal["severity", "newest", "oldest", "due", "confidence", "item",
                     "location"] = "severity"
    reversed: bool = False


#: An advisory kind as the agents name it: "stock", "delivery", "short_pick".
Kind = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,31}$")]


class AdvisoriesFocus(_Area):
    """What a person wants first on the Advisories list. It opens the list on that selection;
    "Everything" stays one tap away and the counts still count every advisory they may see."""

    #: The kinds to show first; empty means every kind. A kind the agents no longer use is
    #: simply never matched.
    kinds: list[Kind] = Field(default_factory=list, max_length=20)
    #: The severities to show first; at least one, or the list would open empty for ever.
    severities: list[Literal["high", "medium", "low"]] = Field(
        default_factory=lambda: ["high", "medium", "low"], min_length=1, max_length=3)


AREAS: dict[str, type[_Area]] = {"app": App, "advisories_view": AdvisoriesView,
                                 "advisories_focus": AdvisoriesFocus}


def defaults(area: str) -> dict:
    """The built-in values of an area, every field present.

    >>> defaults("advisories_view")
    {'group_by': 'kind', 'sort_by': 'severity', 'reversed': False}
    """
    return _shape(area)().model_dump()


def validate(area: str, settings: dict) -> dict:
    """The given fields, checked; fields not given stay out, so they fall through when read.

    >>> validate("advisories_view", {"sort_by": "due"})
    {'sort_by': 'due'}
    >>> validate("advisories_view", {"sort_by": "smell"})
    Traceback (most recent call last):
    ...
    ValueError: sort_by: Input should be 'severity', 'newest', 'oldest', 'due', 'confidence', \
'item' or 'location'
    """
    shape = _shape(area)
    try:
        checked = shape.model_validate(settings or {})
    except ValidationError as exc:
        first = exc.errors()[0]
        field = ".".join(str(part) for part in first["loc"]) or area
        raise ValueError(f"{field}: {first['msg']}") from None
    return checked.model_dump(include=set(settings or {}))


def _shape(area: str) -> type[_Area]:
    if area not in AREAS:
        raise ValueError(f"unknown area {area!r}; the areas are {', '.join(sorted(AREAS))}")
    return AREAS[area]
