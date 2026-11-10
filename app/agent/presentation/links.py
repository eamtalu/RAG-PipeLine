"""Links a card can open, from the app's public address; pure, so any presentation can use them."""


def page_link(public_base_url: str) -> str | None:
    """The pick releases page, when the app has a public address.

    >>> page_link("http://eye/")
    'http://eye/matrix/releases'
    >>> page_link("") is None
    True
    """
    base = (public_base_url or "").rstrip("/")
    return f"{base}/matrix/releases" if base else None


def at_risk_link(public_base_url: str) -> str | None:
    """The deliveries at risk page, when the app has a public address.

    >>> at_risk_link("http://eye")
    'http://eye/matrix/at-risk'
    """
    base = (public_base_url or "").rstrip("/")
    return f"{base}/matrix/at-risk" if base else None
