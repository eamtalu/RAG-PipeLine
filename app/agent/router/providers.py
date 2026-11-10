"""One adapter per model provider: what it can do, and the arguments its chat model is built with.

The provider is the prefix of the model string ("ollama:qwen3:8b", "bedrock_converse:<id>",
"anthropic:<id>", "openai:<id>"). The arguments are the ones `build_model` has always passed to
LangChain's `init_chat_model`, moved here unchanged so a run on any provider is built the same
way as before; the capabilities are what the router matches a profile's needs against.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.agent.contracts import ModelSettings

#: what the hosted providers accept today, wide enough for every profile; refined per model later
HOSTED_CONTEXT_TOKENS = 128_000


def _ollama_arguments(settings: ModelSettings) -> dict[str, Any]:
    # a small model invents less at zero temperature, and a run can be repeated
    return {
        "base_url": settings.ollama_base_url,
        "reasoning": settings.think,
        "num_ctx": settings.context_tokens,
        "temperature": 0,
    }


def _bedrock_arguments(settings: ModelSettings) -> dict[str, Any]:
    return {
        "region_name": settings.bedrock_region,
        "temperature": 0,
        "max_tokens": settings.max_tokens,
    }


def _no_arguments(settings: ModelSettings) -> dict[str, Any]:
    return {}


def _ollama_context(settings: ModelSettings) -> int:
    return settings.context_tokens


def _hosted_context(settings: ModelSettings) -> int:
    return HOSTED_CONTEXT_TOKENS


@dataclass(frozen=True, slots=True)
class ProviderAdapter:
    """A provider by prefix: whether it calls tools, how wide its context is, how it is built."""

    name: str
    prefixes: tuple[str, ...]
    calls_tools: bool
    _context: Callable[[ModelSettings], int]
    _arguments: Callable[[ModelSettings], dict[str, Any]]

    def context_tokens(self, settings: ModelSettings) -> int:
        return self._context(settings)

    def arguments(self, settings: ModelSettings) -> dict[str, Any]:
        """The keyword arguments `init_chat_model` gets for this provider.

        (ollama adapter, settings) -> {"base_url", "reasoning", "num_ctx", "temperature": 0}
        """
        return self._arguments(settings)


ADAPTERS: tuple[ProviderAdapter, ...] = (
    ProviderAdapter("ollama", ("ollama",), True, _ollama_context, _ollama_arguments),
    ProviderAdapter(
        "bedrock", ("bedrock_converse", "bedrock"), True, _hosted_context, _bedrock_arguments
    ),
    ProviderAdapter("anthropic", ("anthropic",), True, _hosted_context, _no_arguments),
    ProviderAdapter("openai", ("openai",), True, _hosted_context, _no_arguments),
)
_BY_PREFIX = {prefix: adapter for adapter in ADAPTERS for prefix in adapter.prefixes}


def adapter_for(model_name: str) -> ProviderAdapter | None:
    """The adapter of a model string's prefix, or None when no provider owns it.

    >>> adapter_for("ollama:qwen3:8b").name, adapter_for("nope:x"), adapter_for("bare")
    ('ollama', None, None)
    """
    prefix, sep, _ = model_name.partition(":")
    return _BY_PREFIX.get(prefix) if sep else None
