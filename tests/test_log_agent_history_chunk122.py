"""Chunk 122: the debugging agent accepts prior turns, and its fixed prompt is cache-marked.

    history     prior user/assistant turns are prepended before the dated question, oldest first;
                malformed turns are dropped; same-role runs are merged; a history that would not
                alternate correctly against the new question is trimmed
    unchanged   ask(question) with no history builds exactly the one-message list it always did
    caching     the system prompt and the last tool definition carry a cache_control marker; the
                client is constructed with the configured retry count
"""

from types import SimpleNamespace

import pytest

from app.services.log_agent import agent as agent_mod
from app.services.log_agent.agent import LogDebugAgent, _history_as_messages
from app.services.log_agent.tools import TOOLS


class _FakeMessages:
    def __init__(self):
        self.calls: list[dict] = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(stop_reason="end_turn",
                               content=[SimpleNamespace(type="text", text="the answer")])


@pytest.fixture
def agent(monkeypatch):
    monkeypatch.setattr(agent_mod.settings, "anthropic_api_key", "test-key")
    a = LogDebugAgent(db=None, customer_code="test_c122")
    fake = _FakeMessages()
    a.client = SimpleNamespace(messages=fake)
    return a, fake


def test_history_normalisation():
    assert _history_as_messages(None) == []
    turns = [
        {"role": "assistant", "content": "orphan leading assistant"},
        {"role": "user", "content": "q1"},
        {"role": "assistant", "content": "a1"},
        {"role": "assistant", "content": "a1 continued"},
        {"role": "tool", "content": "ignored"},
        {"role": "user", "content": "   "},
        {"role": "user", "content": "q2"},
    ]
    assert _history_as_messages(turns) == [
        {"role": "user", "content": "q1"},
        {"role": "assistant", "content": "a1\n\na1 continued"},
    ]


async def test_ask_without_history_is_unchanged(agent):
    a, fake = agent
    result = await a.ask("why?")
    assert result["answer"] == "the answer" and result["tool_calls"] == []
    messages = fake.calls[0]["messages"]
    assert len(messages) == 1 and messages[0]["role"] == "user"
    assert messages[0]["content"].endswith("why?") and "(Today is" in messages[0]["content"]


async def test_ask_with_history_prepends_turns_before_the_dated_question(agent):
    a, fake = agent
    await a.ask("and yesterday?", history=[{"role": "user", "content": "errors today?"},
                                           {"role": "assistant", "content": "three"}])
    messages = fake.calls[0]["messages"]
    assert [m["role"] for m in messages] == ["user", "assistant", "user"]
    assert messages[0]["content"] == "errors today?" and messages[1]["content"] == "three"
    assert messages[2]["content"].endswith("and yesterday?")


async def test_prompt_and_tools_are_cache_marked(agent):
    a, fake = agent
    await a.ask("q")
    call = fake.calls[0]
    assert call["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert call["system"][0]["text"] == agent_mod._SYSTEM_PROMPT
    assert call["tools"][-1]["cache_control"] == {"type": "ephemeral"}
    assert call["tools"][-1]["name"] == TOOLS[-1]["name"]
    assert "cache_control" not in TOOLS[-1], "the module-level TOOLS list must not be mutated"
    assert len(call["tools"]) == len(TOOLS)


def test_client_gets_the_configured_retry_count(monkeypatch):
    monkeypatch.setattr(agent_mod.settings, "log_agent_max_retries", 7)
    a = LogDebugAgent(db=None, customer_code="test_c122")
    assert a.client.max_retries == 7
