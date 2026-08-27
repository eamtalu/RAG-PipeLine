"""Chunk 69. The per-tenant maintenance flag: a RUNNING full-rebuild run pauses that tenant's workers.

The 2026-08-27 backlog repair collided with the live stitch worker because pausing it was a manual
step someone had to remember (and the collision cost the repair four attempts). This module makes the
pause automatic and scoped: while a `log_regroup_runs` row with `kind='full'` and `status='running'`
is FRESH, both tenant sweeps - Stage 2's and analytics' `customers_with_due_work` - exclude that
tenant. Its tickets simply wait; nothing fails, nothing dead-letters, other tenants keep flowing.

Freshness is the safety valve. A crashed subprocess or a service restart mid-rebuild leaves a
`running` row behind, and a flag that never expires would silently freeze the tenant's pipelines
forever - strictly worse than a rebuild that needs re-running. Past
`settings.log_regroup_full_run_ttl_seconds` the flag stops pausing and is logged CRITICAL, once per
run per process (the sweeps run every second or two; per-tick CRITICALs would train everyone to
ignore the one alarm that matters).
"""

import logging
import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_regroup_run import LogRegroupRun, LogRegroupRunStatus
from app.settings import settings

logger = logging.getLogger(__name__)

#: Run ids already alarmed as stale by THIS process, so the CRITICAL fires once per incident rather
#: than once per sweep tick. Module state is fine here: the alarm is advisory, and a restart
#: re-alarming once is the desired behaviour, not a bug.
_alarmed_stale: set[uuid.UUID] = set()


def _fresh_full_runs():
    """Subquery: tenants with a fresh RUNNING full rebuild - the ones the sweeps must skip."""
    cutoff = func.clock_timestamp() - func.make_interval(
        0, 0, 0, 0, 0, 0, settings.log_regroup_full_run_ttl_seconds)
    return (select(LogRegroupRun.customer_code)
            .where(LogRegroupRun.kind == "full",
                   LogRegroupRun.status == LogRegroupRunStatus.running,
                   LogRegroupRun.created_at > cutoff))


def not_under_maintenance(customer_code_column):
    """Predicate for a tenant sweep: exclude tenants with a fresh running full rebuild.

    `NOT IN (subquery)` is NULL-safe here because `customer_code` is NOT NULL on both sides."""
    return customer_code_column.not_in(_fresh_full_runs())


async def alarm_on_stale(db: AsyncSession) -> None:
    """Log CRITICAL for every STALE running full run, once per run per process.

    Called from the sweeps because that is the moment the flag's meaning changes - the tenant is
    about to resume despite a row claiming a rebuild is in flight."""
    cutoff = func.clock_timestamp() - func.make_interval(
        0, 0, 0, 0, 0, 0, settings.log_regroup_full_run_ttl_seconds)
    rows = (await db.execute(
        select(LogRegroupRun.id, LogRegroupRun.customer_code, LogRegroupRun.created_at)
        .where(LogRegroupRun.kind == "full",
               LogRegroupRun.status == LogRegroupRunStatus.running,
               LogRegroupRun.created_at <= cutoff))).all()
    for run_id, cc, created_at in rows:
        if run_id in _alarmed_stale:
            continue
        _alarmed_stale.add(run_id)
        logger.critical(
            "Full rebuild run %s for %s has been 'running' since %s - past the %ds TTL, so it is "
            "treated as STALE (crashed subprocess or service restart?) and the tenant's workers "
            "RESUME. Check the run, mark it failed, and re-run the rebuild if it did not complete.",
            run_id, cc, created_at, settings.log_regroup_full_run_ttl_seconds)
