"""Evaluator interfaces + the factory that turns a NotificationRule row into a code evaluator.

Two shapes:
  - StreamingEvaluator: judged against ONE finalized transaction at a time (status/text rules).
  - WindowEvaluator:    aggregates a time window into a single event (digest rules).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.notification import NotificationRule, RuleType
from app.services.notifications.events import NotificationEvent


class StreamingEvaluator(ABC):
    def __init__(self, rule: NotificationRule):
        self.rule = rule

    @abstractmethod
    def evaluate(self, txn: LogTransaction) -> NotificationEvent | None:
        """Return an event if `txn` matches this rule, else None."""
        raise NotImplementedError


class WindowEvaluator(ABC):
    def __init__(self, rule: NotificationRule):
        self.rule = rule

    @property
    @abstractmethod
    def interval_seconds(self) -> int:
        raise NotImplementedError

    @abstractmethod
    async def evaluate_window(self, db: AsyncSession, window_start: datetime,
                              window_end: datetime, dedup_key: str) -> NotificationEvent | None:
        """Summarize [window_start, window_end) into one event (or None if nothing matched)."""
        raise NotImplementedError


def build_evaluator(rule: NotificationRule) -> StreamingEvaluator | WindowEvaluator | None:
    # Imported here to avoid a circular import (evaluators import from this module).
    from app.services.notifications.rules.evaluators import (
        StatusMatchEvaluator, TextMatchEvaluator, ErrorDigestEvaluator,
    )

    if rule.rule_type == RuleType.status_match.value:
        return StatusMatchEvaluator(rule)
    if rule.rule_type == RuleType.text_match.value:
        return TextMatchEvaluator(rule)
    if rule.rule_type == RuleType.digest.value:
        return ErrorDigestEvaluator(rule)
    return None


def is_streaming(rule: NotificationRule) -> bool:
    return rule.rule_type in (RuleType.status_match.value, RuleType.text_match.value)
