"""The agent's view of a failed provider call: re-exported from `app.services.model_failures`,
which the Teams consumer uses too (the services side may not import the agent package)."""

from app.services.model_failures import FailureKind, ModelCallTimeout, classify

__all__ = ["FailureKind", "ModelCallTimeout", "classify"]
