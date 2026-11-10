"""What a profile needs of a model, as data: the `model` key of a profile file."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

GATE_CHOICES = ("primary", "cheap")
POLICY_KEYS = frozenset({"tools", "context_tokens", "gate"})


@dataclass(frozen=True, slots=True)
class ModelNeeds:
    """The capabilities a candidate must have to serve the profile."""

    tools: bool
    context_tokens: int


@dataclass(frozen=True, slots=True)
class ModelPolicy:
    """The needs, and whether the topic gate may run on a cheaper model when one is configured."""

    needs: ModelNeeds
    gate: str  # "primary" | "cheap"

    @classmethod
    def from_dict(cls, raw: Any, where: str) -> ModelPolicy:
        """The policy of a profile's `model` mapping, refused with a named reason when off.

        >>> ModelPolicy.from_dict({"tools": True, "context_tokens": 16000, "gate": "cheap"}, "p")
        ModelPolicy(needs=ModelNeeds(tools=True, context_tokens=16000), gate='cheap')
        >>> ModelPolicy.from_dict({"tools": True, "context_tokens": 16000}, "p")
        Traceback (most recent call last):
        ...
        ValueError: p: model must have exactly the keys ['context_tokens', 'gate', 'tools']
        """
        if not isinstance(raw, Mapping) or set(raw) != POLICY_KEYS:
            raise ValueError(f"{where}: model must have exactly the keys {sorted(POLICY_KEYS)}")
        if not isinstance(raw["tools"], bool):
            raise ValueError(f"{where}: model.tools must be true or false")
        tokens = raw["context_tokens"]
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
            raise ValueError(f"{where}: model.context_tokens must be a positive integer")
        if raw["gate"] not in GATE_CHOICES:
            raise ValueError(f"{where}: model.gate must be one of {list(GATE_CHOICES)}")
        return cls(needs=ModelNeeds(tools=raw["tools"], context_tokens=tokens), gate=raw["gate"])
