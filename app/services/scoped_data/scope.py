"""`DataScope`: which customer, sites and data categories one run may read."""

from __future__ import annotations

from dataclasses import dataclass

from app.services import access

#: Everyone who may ask reads operational data; the other categories arrive with their capabilities.
DEFAULT_CATEGORIES = ("operations",)


@dataclass(frozen=True, slots=True)
class DataScope:
    """The limit a run reads within, fixed before the run starts."""

    customer_code: str
    all_sites: bool = False
    site_ids: tuple[str, ...] = ()
    categories: tuple[str, ...] = DEFAULT_CATEGORIES

    @classmethod
    def from_record(cls, record: access.AccessRecord | None, *,
                    customer_code: str | None = None) -> "DataScope":
        """The scope a person's record gives; no record gives a scope that covers nothing.

        (supervisor of BRI) -> DataScope(sites=("BRI",)) ; (None, customer_code="c") -> no sites
        """
        if record is None:
            return cls(customer_code=customer_code or "")
        return cls(customer_code=record.customer_code, all_sites=record.all_sites,
                   site_ids=tuple(record.site_ids))

    @classmethod
    def for_system(cls, customer_code: str) -> "DataScope":
        """Every site of one customer, for a caller that has no person behind it.

        >>> DataScope.for_system("acme").covers("LON")
        True
        """
        return cls(customer_code=customer_code, all_sites=True)

    def covers(self, site_id: str | None) -> bool:
        """Whether a row of this site may be read; a row with no site needs every site.

        >>> bri = DataScope("c", site_ids=("BRI",))
        >>> bri.covers("BRI"), bri.covers("LON"), bri.covers(None)
        (True, False, False)
        >>> DataScope("c", all_sites=True).covers(None)
        True
        """
        return self.all_sites or (site_id is not None and site_id in self.site_ids)

    def category_is_closed(self, category: str) -> bool:
        """Whether this scope may not read one category of data.

        >>> scope = DataScope("c")
        >>> scope.category_is_closed("people"), scope.category_is_closed("operations")
        (True, False)
        """
        return category not in self.categories

    @property
    def sites_text(self) -> str:
        """The sites as a person reads them, for a tool result and a refusal sentence.

        >>> DataScope("c", site_ids=("BRI", "LON")).sites_text
        'BRI, LON'
        >>> DataScope("c", all_sites=True).sites_text, DataScope("c").sites_text
        ('all sites', 'no sites')
        """
        return "all sites" if self.all_sites else ", ".join(self.site_ids) or "no sites"

    @property
    def note(self) -> str:
        """What the model is told about a limited scope, so it names the limit instead of "no data".

        >>> DataScope("c", site_ids=("BRI",)).note
        '(This person may see data for: BRI. Data of any other site is outside their access; if \
they ask about another site, say that it is outside their sites.)'
        >>> DataScope("c", all_sites=True).note
        ''
        """
        if self.all_sites:
            return ""
        return (f"(This person may see data for: {self.sites_text}. Data of any other site is "
                "outside their access; if they ask about another site, say that it is outside "
                "their sites.)")
