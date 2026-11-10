"""Chunk 132: `POST /logs/debug/ask` runs the logspace profile over the feed's scope.

The frontend sends the feed's current filters as `scope`; none means today. Since step 12 the
route is an adapter: it puts the raw filters, the day and the zone on the request and hands the
logspace runtime to the gateway. A model or provider failure is a clear 503, never a bare 500.
"""

from datetime import date as date_type

import pytest
from fastapi import HTTPException

from app.agent.contracts import AgentResult
from app.api.v1 import logs as logs_api


class _ReleasedSession:
    """Stands in for the request session: the handler ends its transaction before the run."""

    async def commit(self):
        pass


class _Seen:
    def __init__(self):
        self.request = None
        self.runtime = None
        self.prior_turns = None


@pytest.fixture
def seen(monkeypatch):
    s = _Seen()

    async def spy(request, *, runtime, prior_turns=(), run_settings=None, trace_store=None,
              admission=None):
        s.request, s.runtime, s.prior_turns = request, runtime, list(prior_turns)
        assert trace_store is not None  # the endpoint passes the process's trace store (step 14)
        assert admission is not None  # and its admission (step 16)
        return AgentResult(answer="ok", stop_reason="end_turn", model="m")

    monkeypatch.setattr(logs_api, "run_request", spy)
    monkeypatch.setattr(logs_api, "local_today", lambda tz: date_type(2026, 9, 30))
    return s


async def test_no_scope_means_today(seen):
    out = await logs_api.debug_ask(
        logs_api.DebugAskRequest(question="what failed?"),
        customer="tmp-live",
        db=_ReleasedSession(),
        pending={"pending": False},
    )
    assert seen.request.scope is None and seen.request.today == date_type(2026, 9, 30)
    assert seen.request.tz_name and seen.request.tenant == "tmp-live"
    assert seen.runtime.profile.name == "logspace"
    assert seen.runtime.profile.namespaces == ("logspace",)
    assert seen.request.text == "what failed?" and seen.prior_turns == []
    assert (
        out["answer"] == "ok" and out["refs"] == [] and out["pending_regroup"] == {"pending": False}
    )


async def test_the_feed_filters_travel_as_the_raw_scope(seen):
    filters = {"date": "2026-09-29", "user": "PEVANS", "orderNumber": "O1", "limit": 300}
    body = logs_api.DebugAskRequest(question="has D1 been picked?", scope=filters)
    await logs_api.debug_ask(body, customer="tmp-live", db=_ReleasedSession(), pending={})
    assert seen.request.scope == filters


async def test_history_is_passed_through(seen):
    body = logs_api.DebugAskRequest(
        question="and why?",
        history=[{"role": "user", "content": "D1?"}, {"role": "assistant", "content": "Picked."}],
    )
    await logs_api.debug_ask(body, customer="tmp-live", db=_ReleasedSession(), pending={})
    assert seen.prior_turns == [
        {"role": "user", "content": "D1?"},
        {"role": "assistant", "content": "Picked."},
    ]


async def test_a_provider_failure_is_a_clear_503(monkeypatch):
    async def broken(request, *, runtime, prior_turns=(), run_settings=None, trace_store=None,
                 admission=None):
        raise RuntimeError("Error code: 400 - Your credit balance is too low")

    monkeypatch.setattr(logs_api, "run_request", broken)
    with pytest.raises(HTTPException) as exc:
        await logs_api.debug_ask(
            logs_api.DebugAskRequest(question="x"),
            customer="tmp-live",
            db=_ReleasedSession(),
            pending={},
        )
    assert exc.value.status_code == 503
    assert "could not answer right now" in exc.value.detail and "credit balance" in exc.value.detail
