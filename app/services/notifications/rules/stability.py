"""Whether a transaction has settled enough to alert on.

Stage 2 rebuilds its unsealed tail on every cycle, so a transaction's status is not final the moment
it appears. The one that moves is `incomplete`: a REQUEST with no RESPONSE yet. Minutes later the
RESPONSE lands and it becomes `success`.

Alerting on the in-flight version is worse than merely premature. `dedup_key` is stable per
(rule, transaction), so once that alert is out **no correction ever follows** — the channel keeps a
permanent record of a problem that resolved itself. Measured on production 7 Aug: 81 of 412
`incomplete` transactions were unsealed, i.e. still able to change.

The gate is on stability, not on age:

    incomplete + unsealed  ->  wait. It is in flight and will probably become `success`.
    incomplete + SEALED    ->  alert. Past the abandon window, so the response is never coming —
                               and *that* is the genuinely useful alert.
    error / soft / success ->  alert immediately.

The last line is a deliberate trade. Requiring a seal for every status would delay a real error by up
to the 15-minute seal window, which defeats the point of alerting. The residual risk is that a late
error entry joins an already-responded transaction inside that window and flips `success` to `error`
— rare, but real, so `notification_alert_only_sealed` lets an operator choose accuracy over latency.

This lives beside the rules, NOT in `cursor.py`. The cursor is generic machinery that ML feature
extraction and analytics are expected to reuse; "is this worth alerting on" is a notification
question, and burying it in the shared reader would impose one subsystem's semantics on all of them.
The engine passes the predicate in.
"""

from sqlalchemy import or_

from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.settings import settings

#: Statuses that can still change while a transaction is unsealed. Only `incomplete` genuinely churns
#: — the others already carry a RESPONSE and are terminal in the ordinary case.
UNSTABLE_WHILE_UNSEALED = frozenset({LogTransactionStatus.incomplete})


def is_alertable(status: LogTransactionStatus | None, *, sealed: bool) -> bool:
    """Whether this transaction's status can be trusted enough to alert on.

    In-memory twin of `alertable_predicate`, for the evaluators and for tests that should not need a
    database to state the rule.
    """
    if status is None:
        return False          # mid-write or malformed: nothing meaningful to match on
    if sealed:
        return True           # Stage 2 will never recompute it
    if settings.notification_alert_only_sealed:
        return False          # strict mode: accuracy over latency, for every status
    return status not in UNSTABLE_WHILE_UNSEALED


def alertable_predicate():
    """The same rule as a SQL predicate, for the engine's window query.

    Applied in SQL rather than after fetching on purpose. Every Stage 2 rebuild refreshes
    `created_at`, so an in-flight transaction re-enters the cursor's feed on *every* rebuild until it
    seals. Filtering in Python would mean paying for that churn on every tick; filtering here means
    never fetching it.
    """
    if settings.notification_alert_only_sealed:
        return LogTransaction.sealed.is_(True)
    return or_(
        LogTransaction.sealed.is_(True),
        LogTransaction.status.notin_(list(UNSTABLE_WHILE_UNSEALED)),
    )
