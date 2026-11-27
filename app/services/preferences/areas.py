"""The areas a preference can be about, each with the shape of its settings.

An area is a pydantic model whose every field has a default, so "nothing chosen" is always a
complete answer. `app` holds what the whole app remembers by itself. `ask` holds how far back
a line search looks. Every page with a view switch is an area too (`PAGES`): a short list of
named views saved whole, and `use`, the one the page opens on. A new page is one entry in
`PAGES`; nothing needs a new table.

Each area also says who may hold it (`holders`): a person's saved views are theirs alone, and
a role holds a page's default as one whole page under `<page>_default`. Used by the store, the
resolver and the commands of this package.
"""

from __future__ import annotations

from typing import Annotated, Generic, Literal, TypeVar

from pydantic import (BaseModel, ConfigDict, Field, StringConstraints, ValidationError,
                      model_validator)


class _Area(BaseModel):
    """Unknown fields are refused, otherwise a typo would be stored and silently do nothing."""

    model_config = ConfigDict(extra="forbid")


class App(_Area):
    """How the app opens: the site it starts on, and whether Ask shows past conversations."""

    #: A site id, "all" for the all-sites view, or None for "the app decides".
    default_site: str | None = None
    #: Open until the person hides it, as the app has always started.
    is_history_open: bool = True


#: The most days an Admin may set for a line search. A week of lines limited to a site took
#: over 30 s before the search was bounded, and the server ends a statement at 30 s.
MAX_LINE_SEARCH_DAYS = 14


class Ask(_Area):
    """How Ask searches when the question does not say."""

    #: Days a line search looks back when the question names no time range; None for the
    #: server's own setting.
    line_search_days: int | None = Field(default=None, ge=1, le=MAX_LINE_SEARCH_DAYS)


#: An advisory kind or agent as the backend names it: "count_gap", "delivery_at_risk".
Kind = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,31}$")]
#: A saved view's id, made by the page; short and plain so it can sit in a key or a link.
ViewId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]{0,23}$")]
ViewName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=40)]
#: `use` when the page opens as it comes.
DEFAULT_VIEW = "default"
MAX_VIEWS = 5

PageT = TypeVar("PageT", bound=_Area)


class AdvisoriesPage(_Area):
    """Everything a person can set on the Advisories page: the rail, the kind buttons, the
    grouping and the order. A view saves all of it at once."""

    status: Literal["open", "acknowledged", "resolved"] = "open"
    #: At least one, or the list would open empty for ever.
    severities: list[Literal["high", "medium", "low"]] = Field(
        default_factory=lambda: ["high", "medium", "low"], min_length=1, max_length=3)
    #: The agents kept; empty means every agent, so a new agent shows without a new save.
    agents: list[Kind] = Field(default_factory=list, max_length=20)
    holder: Literal["anyone", "me", "unassigned"] = "anyone"
    #: The kinds kept; empty means every kind.
    kinds: list[Kind] = Field(default_factory=list, max_length=20)
    group_by: Literal["kind", "area", "item", "location", "severity", "agent", "holder",
                      "none"] = "kind"
    sort_by: Literal["severity", "newest", "oldest", "due", "confidence", "item",
                     "location"] = "severity"
    reversed: bool = False


class SavedView(BaseModel, Generic[PageT]):
    """One named view of a page, saved whole."""

    model_config = ConfigDict(extra="forbid")

    id: ViewId
    name: ViewName
    settings: PageT


class PageViews(_Area, Generic[PageT]):
    """A person's views of one page, and the one it opens on.

    The page shows a switch: "Default" and each saved view by name. `use` remembers which
    one the person used last, so nothing has to be removed to look at the default again.
    """

    use: str = DEFAULT_VIEW
    views: list[SavedView[PageT]] = Field(default_factory=list, max_length=MAX_VIEWS)

    @model_validator(mode="after")
    def _use_names_a_saved_view(self) -> "PageViews[PageT]":
        ids = [view.id for view in self.views]
        if len(set(ids)) != len(ids):
            raise ValueError("id: two views have the same id")
        if self.use != DEFAULT_VIEW and self.use not in ids:
            raise ValueError(f"use: {self.use!r} is not a saved view of this page")
        return self


#: The pages that have views, each with the shape of what a view holds.
PAGES: dict[str, type[_Area]] = {"advisories": AdvisoriesPage}

#: What a person's values are resolved for: the app, Ask, and each page's views.
AREAS: dict[str, type[_Area]] = {"app": App, "ask": Ask,
                                 **{page: PageViews[shape] for page, shape in PAGES.items()}}

_DEFAULT_SUFFIX = "_default"
#: What "Default" shows on a page for a role: one whole page, held by the role only.
ROLE_DEFAULT_AREAS: dict[str, type[_Area]] = {
    f"{page}{_DEFAULT_SUFFIX}": shape for page, shape in PAGES.items()}

USER, ROLE, CUSTOMER = "user", "role", "customer"


def default_area(page: str) -> str:
    """The area that holds a role's default of one page.

    >>> default_area("advisories")
    'advisories_default'
    """
    if page not in PAGES:
        raise ValueError(f"unknown page {page!r}; the pages are {', '.join(sorted(PAGES))}")
    return f"{page}{_DEFAULT_SUFFIX}"


def page_of(area: str) -> str | None:
    """The page an area belongs to, for an area that is about one page.

    >>> page_of("advisories"), page_of("advisories_default"), page_of("app")
    ('advisories', 'advisories', None)
    """
    if area in PAGES:
        return area
    return area[:-len(_DEFAULT_SUFFIX)] if area in ROLE_DEFAULT_AREAS else None


def holders(area: str) -> tuple[str, ...]:
    """Who may hold values of an area, widest first.

    >>> holders("ask"), holders("app"), holders("advisories"), holders("advisories_default")
    (('customer', 'role', 'user'), ('role', 'user'), ('user',), ('role',))
    """
    _shape(area)
    if area in ROLE_DEFAULT_AREAS:
        return (ROLE,)
    if area in PAGES:
        return (USER,)
    return (CUSTOMER, ROLE, USER) if area == "ask" else (ROLE, USER)


def page_defaults(page: str) -> dict:
    """The page as it comes, every field present: what "Default" shows.

    >>> page_defaults("advisories")["group_by"], page_defaults("advisories")["kinds"]
    ('kind', [])
    """
    if page not in PAGES:
        raise ValueError(f"unknown page {page!r}; the pages are {', '.join(sorted(PAGES))}")
    return PAGES[page]().model_dump()


def defaults(area: str) -> dict:
    """The built-in values of an area, every field present.

    >>> defaults("advisories")
    {'use': 'default', 'views': []}
    """
    return _shape(area)().model_dump()


_HOLDER_WORDS = {USER: "a person", ROLE: "a role", CUSTOMER: "the whole customer"}


def validate(area: str, settings: dict, subject_kind: str = USER) -> dict:
    """The given fields, checked; fields not given stay out, so they fall through when read.

    >>> validate("app", {"is_history_open": False})
    {'is_history_open': False}
    >>> validate("advisories", {"use": "default"}, "role")
    Traceback (most recent call last):
    ...
    ValueError: advisories belongs to a person, not to a role
    >>> validate("app", {"shoe_size": 9})
    Traceback (most recent call last):
    ...
    ValueError: shoe_size: Extra inputs are not permitted
    """
    shape = _shape(area)
    if subject_kind not in holders(area):
        owners = " or ".join(_HOLDER_WORDS[kind] for kind in holders(area))
        raise ValueError(f"{area} belongs to {owners}, "
                         f"not to {_HOLDER_WORDS.get(subject_kind, subject_kind)}")
    try:
        checked = shape.model_validate(settings or {})
    except ValidationError as exc:
        first = exc.errors()[0]
        field = ".".join(str(part) for part in first["loc"]) or area
        raise ValueError(f"{field}: {first['msg']}") from None
    return checked.model_dump(include=set(settings or {}))


def _shape(area: str) -> type[_Area]:
    known = {**AREAS, **ROLE_DEFAULT_AREAS}
    if area not in known:
        raise ValueError(f"unknown area {area!r}; the areas are {', '.join(sorted(known))}")
    return known[area]
