"""Step 3 of docs/esmart-agent-execution-plan.md: a session lives only as long as one tool call.

Before this step a caller opened one session and handed it to the agent for the whole run, so every
run held a pooled connection through every model call. The pool is 5 + 10 overflow per process,
so the 16th parallel run waited 30 s and failed (measured on 2026-11-09). Now the agent holds no
session at all: each tool call opens a short one and closes it before the model is asked again,
and a request handler ends the transaction its tenant lookup began before the run starts.
"""

import asyncio
import inspect
from contextlib import asynccontextmanager
from typing import Any

from langchain_core.messages import AIMessage
from sqlalchemy import text

from app.agent.runtime.grounded import GroundedAgent
from app.api.v1 import analytics_agent as agent_api
from app.api.v1 import logs as logs_api
from app.config.database import async_session, engine
from app.agent.runtime import grounded
from app.services.analytics_agent.tool_set import AnalyticsToolSet
from app.services.logspace_agent.tool_set import LogspaceToolSet
from tests.agent_helpers import analytics_runtime, ask, logspace_runtime
from tests.test_analytics_agent_chunk124 import CC, ScriptedModel, _plant, clean  # noqa: F401
from tests.test_logspace_agent_chunk132 import TODAY, cc  # noqa: F401 - fixture


def _describe(call_id: str = "c1") -> AIMessage:
    """A fresh tool-call message each time: the graph keys messages by id, one serves once."""
    return AIMessage(content="",
                     tool_calls=[{"id": call_id, "name": "describe_releases", "args": {}}])


def _trace_d1() -> AIMessage:
    return AIMessage(content="", tool_calls=[{"id": "c1", "name": "trace",
                                              "args": {"key": "delivery_number", "value": "D1"}}])


class SessionSpy:
    """A session factory that counts how many of its sessions are open right now."""

    def __init__(self):
        self.open_now = 0
        self.opened = 0

    @asynccontextmanager
    async def __call__(self):
        self.open_now += 1
        self.opened += 1
        try:
            async with async_session() as db:
                yield db
        finally:
            self.open_now -= 1


class WatchingModel(ScriptedModel):
    """The scripted model, noting at every call how many sessions are open and checked out."""

    spy: Any = None
    open_sessions: list = []
    checked_out: list = []

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.open_sessions.append(self.spy.open_now if self.spy is not None else None)
        self.checked_out.append(engine.pool.checkedout())
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def _watching(*script: AIMessage, spy: SessionSpy | None = None) -> WatchingModel:
    return WatchingModel(script=list(script), seen=[], spy=spy, open_sessions=[], checked_out=[])


# ==================================================== 1. the agents take no session

def test_no_agent_constructor_takes_a_session():
    for cls in (GroundedAgent, AnalyticsToolSet, LogspaceToolSet):
        assert "db" not in inspect.signature(cls.__init__).parameters, cls.__name__


# ==================================================== 2. a run holds no session between model calls

async def test_the_analytics_agent_opens_a_session_per_tool_call_and_none_while_the_model_thinks():
    await _plant()
    spy = SessionSpy()
    model = _watching(_describe("c1"), _describe("c2"), AIMessage(content="Two tool calls done."),
                      spy=spy)
    result = await ask(analytics_runtime(CC, model=model, session_factory=spy), "what is there?")
    assert [t["tool"] for t in result["tool_calls"]] == ["describe_releases", "describe_releases"]
    assert spy.opened == 2, "one session per tool call"
    assert model.open_sessions == [0, 0, 0], "no session open while the model is asked"
    assert spy.open_now == 0


async def test_the_logspace_agent_opens_a_session_per_tool_call_and_none_while_the_model_thinks(cc):
    spy = SessionSpy()
    model = _watching(_trace_d1(), AIMessage(content="D1 was picked."), spy=spy)
    agent = logspace_runtime(cc, model=model, session_factory=spy)
    result = await ask(agent, "has D1 been picked?")
    assert [t["tool"] for t in result["tool_calls"]] == ["trace"]
    assert spy.opened == 1 and model.open_sessions == [0, 0] and spy.open_now == 0


# ==================================================== 3. twenty runs at once on the default pool

async def test_twenty_runs_in_parallel_finish_on_the_default_pool():
    await _plant()
    agents = [analytics_runtime(CC, model=_watching(_describe(), AIMessage(content=f"run {i}")))
              for i in range(20)]
    results = await asyncio.gather(*(ask(a, "what is there?") for a in agents))
    assert sorted(r["answer"] for r in results) == sorted(f"run {i}" for i in range(20))
    assert all(len(r["tool_calls"]) == 1 for r in results)
    assert engine.pool.checkedout() == 0


# ==================================================== 4. handlers release their connection first

async def test_the_analytics_ask_endpoint_holds_no_connection_during_the_run(monkeypatch):
    await _plant()
    model = _watching(AIMessage(content="in_scope"), _describe(), AIMessage(content="Done."))
    monkeypatch.setattr(grounded, "init_chat_model", lambda name, **kw: model)
    async with async_session() as db:
        await db.execute(text("select 1"))  # the tenant lookup already began a transaction here
        assert engine.pool.checkedout() == 1
        out = await agent_api.ask(agent_api.AskRequest(question="what is there?"), customer=CC,
                                  db=db)
    assert out["answer"] == "Done."
    assert model.checked_out == [0, 0, 0], "the request connection is back in the pool first"


async def test_the_logspace_ask_endpoint_holds_no_connection_during_the_run(monkeypatch, cc):
    model = _watching(AIMessage(content="in_scope"), _trace_d1(), AIMessage(content="Picked."))
    monkeypatch.setattr(grounded, "init_chat_model", lambda name, **kw: model)
    monkeypatch.setattr(logs_api, "local_today", lambda tz: TODAY)
    async with async_session() as db:
        await db.execute(text("select 1"))
        assert engine.pool.checkedout() == 1
        out = await logs_api.debug_ask(logs_api.DebugAskRequest(question="has D1 been picked?"),
                                       customer=cc, db=db, pending={"pending": False})
    assert out["answer"].startswith("Picked.")
    assert model.checked_out == [0, 0, 0]
