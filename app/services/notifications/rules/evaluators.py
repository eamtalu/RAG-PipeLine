"""Concrete rule evaluators.

  - StatusMatchEvaluator: txn.status ∈ match["statuses"] (+ optional match["methods"] filter).
  - TextMatchEvaluator:   substring/regex over match["fields"] (default error_text), optional status filter.
  - ErrorDigestEvaluator: one summary event per window of matching transactions.

All build events from a stable serializer so a retry renders identically without the original row.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.notification import NotificationRule
from app.services.notifications.events import NotificationEvent
from app.services.notifications.rules.base import StreamingEvaluator, WindowEvaluator
from app.services.mnp_log_ingestion.timefmt import iso_display

logger = logging.getLogger(__name__)


def _txn_status(txn: LogTransaction) -> str | None:
    return txn.status.value if txn.status is not None else None


def transaction_payload(txn: LogTransaction) -> dict:
    """Curated, channel-agnostic context for a transaction event (ordered facts + ids for links)."""
    facts = {
        "Customer": txn.customer_code,
        "Status": _txn_status(txn),
        "Method": txn.method,
        "Warehouse": txn.warehouse,
        "User": txn.user_name,
        "Req ID": txn.reqid,
        # display-only: show the start in UK time to match the logs (storage/window math stay UTC)
        "Started": iso_display(txn.started_at),
        "Duration (ms)": txn.duration_ms,
        "Error": txn.error_text,
    }
    facts = {k: v for k, v in facts.items() if v not in (None, "")}
    return {"transaction_id": str(txn.id), "facts": facts}


def _target_ids(rule: NotificationRule) -> list[str] | None:
    if not rule.target_channel_ids:
        return None
    return [str(c) for c in rule.target_channel_ids]


def _txn_event(rule: NotificationRule, txn: LogTransaction, event_type: str) -> NotificationEvent:
    status = _txn_status(txn) or "event"
    return NotificationEvent(
        event_type=event_type,
        customer_code=txn.customer_code,
        severity=rule.severity,
        title=f"[{txn.customer_code}] {status}: {txn.method or 'transaction'}",
        summary=txn.error_text or None,
        # stable per (rule, transaction, STATUS) → an unchanged transaction alerts exactly once no
        # matter how often the worker polls, while a status CHANGE - the correction the alert exists
        # for, e.g. incomplete that later errors - mints a new key and re-alerts. Version-blind
        # (rule, txn) was the accepted residual risk in stability.py while every rebuild re-inserted
        # rows; S3's update-in-place made the status flip observable, so the key can carry it.
        dedup_key=f"rule:{rule.id}:txn:{txn.id}:status:{status}",
        payload=transaction_payload(txn),
        target_channel_ids=_target_ids(rule),
        rule_id=str(rule.id),
    )


class StatusMatchEvaluator(StreamingEvaluator):
    def evaluate(self, txn: LogTransaction) -> NotificationEvent | None:
        statuses = set(self.rule.match.get("statuses") or ["error"])
        if _txn_status(txn) not in statuses:
            return None
        methods = self.rule.match.get("methods")
        if methods and txn.method not in set(methods):
            return None
        return _txn_event(self.rule, txn, "transaction_error")


class TextMatchEvaluator(StreamingEvaluator):
    def __init__(self, rule: NotificationRule):
        super().__init__(rule)
        self._pattern = rule.match.get("pattern") or ""
        self._fields = rule.match.get("fields") or ["error_text"]
        self._is_regex = bool(rule.match.get("is_regex"))
        self._ignore_case = rule.match.get("ignore_case", True)
        self._statuses = set(rule.match.get("statuses") or [])
        self._regex = None
        if self._is_regex and self._pattern:
            try:
                self._regex = re.compile(self._pattern, re.IGNORECASE if self._ignore_case else 0)
            except re.error:
                logger.warning("notification rule %s has an invalid regex %r — it will never match",
                               rule.id, self._pattern)

    def evaluate(self, txn: LogTransaction) -> NotificationEvent | None:
        if not self._pattern:
            return None
        if self._statuses and _txn_status(txn) not in self._statuses:
            return None
        haystack = " ".join(str(getattr(txn, f, None) or "") for f in self._fields)
        if self._is_regex:
            if self._regex is None or not self._regex.search(haystack):
                return None
        else:
            hay = haystack.lower() if self._ignore_case else haystack
            needle = self._pattern.lower() if self._ignore_case else self._pattern
            if needle not in hay:
                return None
        return _txn_event(self.rule, txn, "transaction_match")


class ErrorDigestEvaluator(WindowEvaluator):
    @property
    def interval_seconds(self) -> int:
        try:
            return max(60, int(self.rule.match.get("interval_seconds") or 3600))
        except (TypeError, ValueError):
            return 3600

    async def evaluate_window(self, db: AsyncSession, window_start: datetime,
                              window_end: datetime, dedup_key: str) -> NotificationEvent | None:
        statuses = self.rule.match.get("statuses") or ["error"]
        rows = (await db.execute(
            select(LogTransaction).where(
                LogTransaction.customer_code == self.rule.customer_code,
                LogTransaction.started_at >= window_start,
                LogTransaction.started_at < window_end,
                LogTransaction.status.in_(statuses),
            ).order_by(LogTransaction.started_at.asc())
        )).scalars().all()
        if not rows:
            return None

        # tally by method for a compact summary
        by_method: dict[str, int] = {}
        for r in rows:
            by_method[r.method or "(unknown)"] = by_method.get(r.method or "(unknown)", 0) + 1
        top = sorted(by_method.items(), key=lambda kv: kv[1], reverse=True)[:5]

        facts = {
            "Customer": self.rule.customer_code,
            "Total": len(rows),
            "Window": f"{iso_display(window_start)} → {iso_display(window_end)}",
            "Statuses": ", ".join(statuses),
        }
        for method, count in top:
            facts[f"• {method}"] = count

        return NotificationEvent(
            event_type="error_digest",
            customer_code=self.rule.customer_code,
            severity=self.rule.severity,
            title=f"[{self.rule.customer_code}] Error digest: {len(rows)} transactions",
            summary=f"{len(rows)} matching transactions between {iso_display(window_start)} and "
                    f"{iso_display(window_end)}.",
            dedup_key=dedup_key,
            payload={"facts": facts},
            target_channel_ids=_target_ids(self.rule),
            rule_id=str(self.rule.id),
        )
