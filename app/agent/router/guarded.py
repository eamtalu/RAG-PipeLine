"""The guard as a chat model: every call of the gate and of the tool-bound loop goes through it.

LangChain's `create_agent` requires a `BaseChatModel` and calls `bind_tools` on it, then
`ainvoke` on what comes back, once per loop step; the topic gate calls `ainvoke` on the model
itself. `GuardedChatModel` wraps the real model for both paths and `GuardedRunnable` wraps the
bound runnable, so no call reaches a provider without the provider's guard. The sync path
delegates unguarded: the agent only runs async.
"""

from __future__ import annotations

from typing import Any, Sequence

from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable, RunnableConfig

from app.agent.router.health import ProviderGuard


class GuardedRunnable(Runnable[LanguageModelInput, AIMessage]):
    """The tool-bound model of a run, each `ainvoke` under the provider's guard."""

    def __init__(self, inner: Runnable, guard: ProviderGuard, timeout_seconds: float):
        self.inner, self.guard, self.timeout_seconds = inner, guard, timeout_seconds

    def invoke(
        self, input: LanguageModelInput, config: RunnableConfig | None = None, **kwargs: Any
    ):
        return self.inner.invoke(input, config, **kwargs)

    async def ainvoke(
        self, input: LanguageModelInput, config: RunnableConfig | None = None, **kwargs: Any
    ) -> AIMessage:
        return await self.guard.call(
            lambda: self.inner.ainvoke(input, config, **kwargs),
            timeout_seconds=self.timeout_seconds,
        )


class GuardedChatModel(BaseChatModel):
    """The pinned model of a run with its provider's guard around every async call."""

    inner: Any
    guard: Any
    timeout_seconds: float

    @property
    def _llm_type(self) -> str:
        return f"guarded-{self.inner._llm_type}"

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs):
        message = self.inner.invoke(messages, stop=stop, **kwargs)
        return ChatResult(generations=[ChatGeneration(message=message)])

    async def _agenerate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs):
        message = await self.guard.call(
            lambda: self.inner.ainvoke(messages, stop=stop, **kwargs),
            timeout_seconds=self.timeout_seconds,
        )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def bind_tools(self, tools: Sequence, **kwargs: Any) -> Runnable[LanguageModelInput, AIMessage]:
        """The inner model bound to the tools, still under the guard."""
        return GuardedRunnable(
            self.inner.bind_tools(tools, **kwargs), self.guard, self.timeout_seconds
        )
