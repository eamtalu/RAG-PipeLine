"""Step 4 of docs/esmart-agent-execution-plan.md: every agent run has a deadline.

A provider that never answers used to hold a consumer slot for ever: the heartbeat kept extending
the message's visibility, so the run neither failed nor finished. Now the whole run, gate call
included, sits under one deadline shorter than the queue's visibility window; a run that reaches
it is cancelled and answers with a fixed sentence the person can act on.
"""

import asyncio
import time

import pytest
from langchain_core.messages import AIMessage

from app.agent.runtime.guards import TopicGate
from app.services.analytics_agent.agent import AnalyticsAgent
from app.services.teams import consumer as c
from app.settings import settings
from tests.test_analytics_agent_chunk124 import CC, ScriptedModel, _scripted, clean  # noqa: F401
from tests.test_teams_consumer_chunk123 import FakePoster, FakeSqs, Harness, Q, job


class HangingModel(ScriptedModel):
    """A provider that never answers; it records whether the run cancelled its call."""

    was_cancelled: bool = False

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.was_cancelled = True
            raise


def _hanging() -> HangingModel:
    return HangingModel(script=[], seen=[])


# ==================================================== 1. a run that hangs answers with the sentence

async def test_a_hanging_provider_yields_the_timeout_answer_and_is_cancelled():
    model = _hanging()
    started = time.monotonic()
    result = await AnalyticsAgent(CC, model=model, run_timeout_seconds=0.2).ask("what is there?")
    assert time.monotonic() - started < 2
    assert result["stop_reason"] == "timeout" and result["answer"] == AnalyticsAgent.TIMED_OUT
    assert result["tool_calls"] == [] and result["evidence"] is None
    assert model.was_cancelled, "the hanging model call must be cancelled, not left running"


async def test_the_deadline_covers_the_topic_gate_too():
    model = _hanging()
    agent = AnalyticsAgent(CC, model=model, gate=TopicGate(model, "x"), run_timeout_seconds=0.2)
    result = await agent.ask("hi")
    assert result["stop_reason"] == "timeout" and model.was_cancelled


async def test_a_run_inside_the_deadline_is_unchanged():
    result = await AnalyticsAgent(CC, model=_scripted(AIMessage(content="Fine.")),
                                  run_timeout_seconds=5).ask("hi")
    assert result["stop_reason"] == "end_turn" and result["answer"] == "Fine."


def test_the_deadline_defaults_to_the_setting_and_sits_below_the_visibility_window():
    agent = AnalyticsAgent(CC, model=_scripted())
    assert agent.run_timeout_seconds == settings.analytics_agent_run_timeout_seconds
    assert (settings.analytics_agent_run_timeout_seconds
            < settings.teams_sqs_visibility_seconds)


# ==================================================== 2. the consumer answers and frees the slot

async def test_the_consumer_posts_the_timeout_answer_and_frees_the_slot():
    async def hanging_agent(code, question, history):
        return await AnalyticsAgent(code, model=_hanging(), run_timeout_seconds=0.1).ask(question)

    h = Harness([job(job_id="j1"), job(job_id="j2", conversation_id="c2")], agent=hanging_agent,
                concurrency=1)
    await h.drain()
    assert [p.answer for p in h.poster.posted] == [AnalyticsAgent.TIMED_OUT] * 2
    assert [p.status for p in h.poster.posted] == ["ok", "ok"]
    assert len(h.sqs.deleted) == 2 and h.consumer.in_flight == 0


def test_the_consumer_refuses_a_deadline_that_outlives_the_visibility_window():
    with pytest.raises(ValueError, match="visibility"):
        c.TeamsQuestionConsumer(sqs=FakeSqs([]), queue_url=Q, poster=FakePoster(), run_agent=None,
                                load_history=None, record_exchange=None, resolve_customer=None,
                                concurrency=1, visibility_seconds=60, run_timeout_seconds=60)
