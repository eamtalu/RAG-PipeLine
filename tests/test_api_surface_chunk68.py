"""Chunk 68: the API surface sheds its one broken endpoint, and a misleading label.

Both facts were proven operationally on 2026-08-27, the hard way:

**`POST /logs/regroup` is removed.** It ran a FULL tenant rebuild inline in the web request. On a
real tenant (184k transactions, 3.2M entries) that takes the better part of an hour; gunicorn kills
the web worker at 120 s, the client gets ECONNRESET, and - because the rebuild commits in phases -
the tenant is left PARTIALLY rebuilt with no run id to resume from. The repair that discovered this
had to be re-run four times. Nothing called the endpoint (no frontend caller, no test, no script);
the library functions live on (`regroup_incremental` under `DELETE /logs/data`, `regroup_all` as the
scripted repair) until the tracked asynchronous replacement ships (`POST /logs/regroup/full`,
planned as the maintenance-screen work). Removing the endpoint is what makes the next person build
the safe one instead of reaching for the footgun.

**`POST /analytics/reconcile` says 200, because it IS 200.** It declared `status_code=202` while
running the checks inline and returning the complete report in the same response - a contract that
promises "poll later" and delivers "here it is". A 202 with nothing to poll is worse than either
honest option.
"""

from app.main import app


def _surface() -> dict[tuple[str, str], dict]:
    """(METHOD, path) -> operation spec, via the OpenAPI schema.

    The schema rather than `app.routes`, because the installed FastAPI keeps included routers as
    lazy `_IncludedRouter` entries - walking `app.routes` sees six top-level routes and nothing
    else, which is exactly the kind of silently-empty assertion this file must not contain."""
    return {(m.upper(), p): spec
            for p, methods in app.openapi()["paths"].items()
            for m, spec in methods.items()}


def test_the_inline_full_regroup_endpoint_is_gone():
    """The footgun stays removed: a full rebuild must go through a tracked background run, never an
    HTTP request that cannot survive its own duration."""
    assert ("POST", "/api/v1/logs/regroup") not in _surface(), (
        "POST /logs/regroup is the inline full rebuild that dies at the gunicorn timeout and leaves "
        "a partially rebuilt tenant; the replacement is the tracked async run")


def test_the_windowed_finalize_survives():
    """The guard for the other direction: the GOOD regroup endpoints must not be caught in the
    sweep."""
    surface = _surface()
    assert ("POST", "/api/v1/logs/regroup/finalize") in surface
    assert ("POST", "/api/v1/logs/regroup/reset-abandoned") in surface
    assert ("GET", "/api/v1/logs/regroup/status") in surface


def test_reconcile_declares_the_status_code_it_actually_has():
    """The endpoint runs inline and returns the full report; its declared code must say so."""
    spec = _surface()[("POST", "/api/v1/analytics/reconcile")]
    assert "200" in spec["responses"] and "202" not in spec["responses"], (
        "202 promises a poll that does not exist; the report is in this very response")
