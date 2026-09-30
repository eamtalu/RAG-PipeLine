"""Chunk 132: `POST /logs/debug/ask` runs the logspace agent over the feed's scope.

The frontend sends the feed's current filters as `scope`; none means today. A model or provider
failure (for example an exhausted account) is a clear 503, never a bare 500.
"""

from datetime import date as date_type

import pytest
from fastapi import HTTPException

from app.api.v1 import logs as logs_api
from app.services.log_feed.scope import FeedScope
from app.services.logspace_agent.agent import LogspaceAgent


class _Seen:
    def __init__(self):
        self.kwargs = None
        self.asked = None


@pytest.fixture
def seen(monkeypatch):
    s = _Seen()

    def fake_init(self, db, customer_code, *, scope, tz_name, today, model=None, gate=None):
        s.kwargs = dict(customer=customer_code, scope=scope, tz_name=tz_name, today=today)

    async def fake_ask(self, question, history=None):
        s.asked = (question, history)
        return {"answer": "ok", "tool_calls": [], "stop_reason": "end_turn"}

    monkeypatch.setattr(LogspaceAgent, "__init__", fake_init)
    monkeypatch.setattr(LogspaceAgent, "ask", fake_ask)
    monkeypatch.setattr(logs_api, "local_today", lambda tz: date_type(2026, 9, 30))
    return s


async def test_no_scope_means_today(seen):
    out = await logs_api.debug_ask(logs_api.DebugAskRequest(question="what failed?"), customer="tmp-live",
                                   db=None, pending={"pending": False})
    assert seen.kwargs["scope"] == FeedScope(day=date_type(2026, 9, 30), explicit=False)
    assert seen.kwargs["customer"] == "tmp-live" and seen.asked == ("what failed?", [])
    assert out["answer"] == "ok" and out["refs"] == [] and out["pending_regroup"] == {"pending": False}


async def test_the_feed_filters_become_the_scope(seen):
    body = logs_api.DebugAskRequest(question="has D1 been picked?",
                                    scope={"date": "2026-09-29", "user": "PEVANS", "orderNumber": "O1", "limit": 300})
    await logs_api.debug_ask(body, customer="tmp-live", db=None, pending={})
    assert seen.kwargs["scope"] == FeedScope(day=date_type(2026, 9, 29), user="PEVANS", order_number="O1", explicit=True)


async def test_history_is_passed_through(seen):
    body = logs_api.DebugAskRequest(question="and why?", history=[{"role": "user", "content": "D1?"},
                                                                  {"role": "assistant", "content": "Picked."}])
    await logs_api.debug_ask(body, customer="tmp-live", db=None, pending={})
    assert seen.asked[1] == [{"role": "user", "content": "D1?"}, {"role": "assistant", "content": "Picked."}]


async def test_a_provider_failure_is_a_clear_503(monkeypatch):
    def fake_init(self, *a, **kw):
        pass

    async def broken(self, question, history=None):
        raise RuntimeError("Error code: 400 - Your credit balance is too low")

    monkeypatch.setattr(LogspaceAgent, "__init__", fake_init)
    monkeypatch.setattr(LogspaceAgent, "ask", broken)
    with pytest.raises(HTTPException) as exc:
        await logs_api.debug_ask(logs_api.DebugAskRequest(question="x"), customer="tmp-live", db=None, pending={})
    assert exc.value.status_code == 503
    assert "could not answer right now" in exc.value.detail and "credit balance" in exc.value.detail
