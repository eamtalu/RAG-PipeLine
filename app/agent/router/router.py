"""The router: the candidates the settings name, filtered by what the profile needs, pinned.

`pick` walks the primary and then the fallbacks, skips a candidate no adapter knows or that cannot
serve the needs (one `agent.router.skipped` line each), pins the first that qualifies and keeps
the rest as its fallbacks. The runtime runs the pinned model and, when its provider fails
mid-run, reruns once from the start on the next fallback. The breaker, the bulkhead and the
tenant's budget are the checks later steps add here. Nothing in this module reads settings: the
candidates come from the snapshot the gateway built.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from app.agent.contracts import ModelSettings
from app.agent.router.policy import ModelNeeds, ModelPolicy
from app.agent.router.providers import adapter_for

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ModelChoice:
    """What a run gets: the pinned model, the fallbacks in order, and the gate's model."""

    model: str
    fallbacks: tuple[str, ...]
    gate_model: str


class ModelRouter:
    """Stateless today; breakers and bulkheads make it a process-wide object later."""

    def pick(self, policy: ModelPolicy, settings: ModelSettings) -> ModelChoice:
        """The choice for one run, or a ValueError naming the needs no candidate can serve.

        (needs tools+16000, primary ollama with num_ctx 16384, fallback bedrock)
        -> ModelChoice("ollama:...", ("bedrock_converse:...",), gate "ollama:...") ;
        (primary "nope:x") -> the fallback pinned, one skipped line
        """
        candidates = (settings.name, *settings.fallback_models)
        # Step 1: Keep only the candidates that can serve the profile, because a model that
        # cannot call tools or hold the schemas fails at the first question, not at pick time.
        usable = [c for c in candidates if self._serves(c, policy.needs, settings)]
        if not usable:
            raise ValueError(
                f"no model can serve the profile's needs {policy.needs}; "
                f"the candidates were {list(candidates)}"
            )
        # Step 2: The gate runs on the cheap model only when the profile allows it and one is
        # configured, so a profile cannot be screened by a model its author did not accept.
        gate_model = (
            settings.gate_model
            if policy.gate == "cheap" and settings.gate_model
            else usable[0]
        )
        return ModelChoice(model=usable[0], fallbacks=tuple(usable[1:]), gate_model=gate_model)

    @staticmethod
    def _serves(candidate: str, needs: ModelNeeds, settings: ModelSettings) -> bool:
        adapter = adapter_for(candidate)
        why = None
        if adapter is None:
            why = "no adapter for its prefix"
        elif needs.tools and not adapter.calls_tools:
            why = "it cannot call tools"
        elif adapter.context_tokens(settings) < needs.context_tokens:
            why = (
                f"its context of {adapter.context_tokens(settings)} tokens is below the "
                f"{needs.context_tokens} the profile needs"
            )
        if why is not None:
            logger.warning("agent.router.skipped model=%s why=%s", candidate, why)
            return False
        return True
