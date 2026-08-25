"""R1. ONE definition of "is this transaction captured", used by every reader that needs it.

Why this module exists at all
----------------------------
Three separate queries decide, independently, what analytics thinks should exist:

    consume._read_source        what the fold reads FROM `log_transactions`
    consume._read_stored        what the fold compares it against, in `analytics_facts`
    reconcile.facts_vs_transactions   what the auditor expects to find a fact for

If any two of those disagree about one transaction, the disagreement is PERMANENT and LOUD. A
transaction the fold skips but the auditor expects is reported as a missing fact on every single run,
forever - and a permanently red check is worse than no check, because it teaches everyone to ignore
the one thing that would have caught a real divergence.

So the predicate is written once, here, and the three callers import it. There is no version of this
that is safe to inline "just for now".

Why it is applied to the STORED side too
----------------------------------------
The fold is a range diff: whatever is in stored and absent from source is REVERSED. So applying the
predicate to source alone would make un-ticking `capture` DELETE every fact that transaction already
has - "stop capturing" silently meaning "destroy the history you have".

Applying it to both sides instead makes those facts invisible to the diff: they are neither compared
nor reversed, so they simply stay as they are. Un-ticking capture stops new facts and keeps old ones,
which is what the words mean. Re-ticking it brings them back into the comparison, where the fingerprint
decides whether anything actually changed.

What is NOT here
----------------
`show`. That gates whether facts reach charts and rollups, and it is already expressible: it lives on
the metric definition (`definition._contributes`) and is evaluated at FOLD time, over facts that were
captured all along. That is why turning `show` on is instant and retroactive while turning `capture` on
can only ever fill in from now.

The unnamed transactions
------------------------
`transaction_name IS NULL` is 57 rows - `CheckOperator`, `CheckServer` - and they cannot be keyed by
name. The rule is fixed in code: always captured, never shown. Captured because a probe that starts
failing is exactly what someone will want to measure later and the entries are gone in 60 days; never
shown because they are not warehouse activity and would distort every default chart.
"""

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.log_transaction import LogTransaction
from app.services.analytics import payload as pl

logger = logging.getLogger(__name__)

#: A transaction seen for the first time is captured but not shown. Neither default can do harm:
#: capture-on cannot lose data, show-off cannot surprise a reader with an unreviewed number.
DEFAULT_CAPTURE = True
DEFAULT_SHOW = False
DEFAULT_EXPAND = False


async def suppressed_names(db: AsyncSession, customer_code: str) -> frozenset[str]:
    """The transaction names this tenant has explicitly turned capture OFF for.

    Returns the EXCLUSIONS rather than the inclusions, which is the direction that makes the default
    safe. An inclusion list would have to enumerate every name analytics has ever seen, so a name
    missing from it - a brand-new transaction, a tenant with no registry rows yet - would silently not
    be captured. Returning exclusions means anything unknown is captured, matching `DEFAULT_CAPTURE`,
    and an empty registry captures everything.
    """
    rows = (await db.execute(
        select(AnalyticsTransactionRegistry.transaction_name).where(
            AnalyticsTransactionRegistry.customer_code == customer_code,
            AnalyticsTransactionRegistry.capture.is_(False)))).scalars().all()
    return frozenset(rows)


def source_predicate(suppressed: frozenset[str]):
    """The capture gate as a SQLAlchemy predicate on `log_transactions`.

    Returns None when nothing is suppressed, so the common case adds no clause at all and the query
    plan is byte-identical to before R1. That matters: this runs on every fold of every window.

    `notin_` on a nullable column is NULL-safe in the direction that matters here, but not obviously,
    so it is made explicit: a NULL `transaction_name` must always pass, because the unnamed probes are
    always captured. `x NOT IN (...)` evaluates to NULL for a NULL x and a row is only kept when the
    predicate is TRUE, so the NULL rows would be silently DROPPED - the exact opposite of the rule.
    """
    if not suppressed:
        return None
    return (LogTransaction.transaction_name.is_(None)
            | LogTransaction.transaction_name.notin_(sorted(suppressed)))


def fact_predicate(suppressed: frozenset[str], model):
    """The same gate on the fact side, so the diff never sees a half of a suppressed transaction.

    `model` is passed rather than imported so this works for `AnalyticsFact` and for the ledger without
    this module needing to know which table a caller is reading. `transaction_name` is a real column on
    both (`analytics_fact.py:85`), which is what makes the two sides expressible in the same terms.
    """
    if not suppressed:
        return None
    return (model.transaction_name.is_(None)
            | model.transaction_name.notin_(sorted(suppressed)))


def is_captured(transaction_name: str | None, suppressed: frozenset[str]) -> bool:
    """The in-memory twin, for callers holding rows rather than building a query.

    Exists so the rule can be asserted without a database, and so a test can prove the SQL and the
    Python agree - the same discipline `_is_sealed` and the sealer's SQL are held to.
    """
    if transaction_name is None:
        return True
    return transaction_name not in suppressed


async def observe_names(db: AsyncSession, customer_code: str,
                        names: set[str | None]) -> list[str]:
    """Make sure every named transaction seen in this window has a registry row. Does NOT commit.

    Returns the names newly registered, so the caller can log them - a transaction appearing for the
    first time is worth a line in the log, since somebody has to review it.

    `ON CONFLICT DO NOTHING`, so an existing row's switches are never overwritten by observation. That
    is the whole point: this discovers, it does not decide. A row a person has set to `capture = false`
    must survive the transaction being seen again on the very next tick.

    NULL is skipped rather than registered. It cannot be keyed by name, and its rule lives in code.
    """
    real = sorted(n for n in names if n)
    if not real:
        return []
    now = datetime.now(timezone.utc)
    stmt = pg_insert(AnalyticsTransactionRegistry).values([
        {"id": uuid.uuid4(), "customer_code": customer_code, "transaction_name": n,
         "capture": DEFAULT_CAPTURE, "show": DEFAULT_SHOW, "expand": DEFAULT_EXPAND,
         "first_seen_at": now, "created_at": now, "updated_at": now}
        for n in real
    ]).on_conflict_do_nothing(constraint="uq_analytics_txn_registry_name")
    stmt = stmt.returning(AnalyticsTransactionRegistry.transaction_name)
    added = list((await db.execute(stmt)).scalars().all())
    if added:
        logger.info("Analytics: %d transaction(s) seen for the first time for %s and registered "
                    "capture=on/show=off, awaiting review: %s",
                    len(added), customer_code, ", ".join(sorted(added)))
    return added


async def approved_attributes(db: AsyncSession, customer_code: str) -> frozenset[str]:
    """The `attributes` keys this tenant has ticked for capture (R1b).

    Fed to `definition.validate` as `known_attributes`, so a metric may group by or measure
    `attr:resp.BaseUoM` only once somebody has approved that field. The registry is the authority and
    `definition.py` never learns it exists - it takes the set as an argument, which is what keeps that
    module free of database access.

    Returns the FULL key including its namespace prefix (`resp.QuantityOnHand`, `mi.STQT`), because
    that is what a definition names after `attr:` and matching a bare name across namespaces would let
    an approved request field silently authorise an unapproved response one.
    """
    rows = (await db.execute(
        select(AnalyticsFieldRegistry.field).where(
            AnalyticsFieldRegistry.customer_code == customer_code,
            AnalyticsFieldRegistry.captured.is_(True)))).scalars().all()
    return frozenset(rows)


async def observe_fields(db: AsyncSession, customer_code: str,
                         seen: dict[str, set[str]]) -> list[str]:
    """Register every response field observed, at its seeded approval. Does NOT commit.

    `seen` maps method -> the namespaced field names seen on it. Returns the names newly registered, so
    the caller can log them: a field appearing for the first time is something somebody has to review.

    A row arrives with `captured = payload.seeded(name)`, which is TRUE only for a name on the
    hardcoded seed list AND not credential-shaped. Everything else arrives `captured = false`: recorded
    by NAME so it is reviewable, never by value.

    `ON CONFLICT DO NOTHING` for the same reason `observe_names` uses it - observation must never
    overwrite a decision. A field somebody has un-ticked has to stay un-ticked even though it is on the
    seed list, and it is seen again on every single tick.

    `source` is derived from the namespace rather than passed, so the stored row cannot disagree with
    the prefix the field is addressed by.
    """
    rows = []
    now = datetime.now(timezone.utc)
    for method, names in seen.items():
        for name in sorted(names):
            rows.append({
                "id": uuid.uuid4(), "customer_code": customer_code,
                "method": method or "(none)",
                "source": "mi_result" if name.startswith(pl.MI_PREFIX) else "response",
                "field": name, "captured": pl.seeded(name),
                "first_seen_at": now, "last_seen_at": now, "seen_count": 1,
                "created_at": now, "updated_at": now,
            })
    if not rows:
        return []
    stmt = pg_insert(AnalyticsFieldRegistry).values(rows).on_conflict_do_nothing(
        constraint="uq_analytics_field_registry_key")
    added = list((await db.execute(
        stmt.returning(AnalyticsFieldRegistry.field))).scalars().all())
    if added:
        unapproved = sorted(set(added) - {a for a in added if pl.seeded(a)})
        logger.info("Analytics: %d response field(s) seen for the first time for %s; %d auto-approved "
                    "from the seed list, %d recorded by NAME ONLY awaiting review: %s",
                    len(added), customer_code, len(added) - len(unapproved), len(unapproved),
                    ", ".join(unapproved) or "none")
    return added
