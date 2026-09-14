"""One-off: keep only the London days from 13 September 2026 onward; drop everything older.

Why this exists (2026-09-14): the server's disk has bad blocks in three files of August partitions
(a log_entries index, a log_entry_assignment index, and earlier log_entries data). A full regroup
died on one of them, and every analytics fold then died on another. The tenant is not live, so the
decision was a fresh start from the last two days rather than a repair.

What it does, in one transaction:
  - DROPS the daily partitions of log_entries, log_entry_assignment and log_transactions dated before
    2026-09-13. A dropped partition is never read, so the bad blocks go without being touched.
  - DELETES bookkeeping rows older than the cutoff: stitch ranges, regroup runs, ingested source
    objects, notification events and their deliveries, and jobs nothing references any more.
  - TRUNCATES every analytics projection (facts, ledger, record rows, roll-ups, quality issues,
    tickets, tenant state, feature sets, predictions) and both discovery registries, then resets
    `backfilled_through` on the metric definitions and publishes one ticket per tenant over the kept
    range so the worker rebuilds two days of facts in its next cycles.
Kept: customers, SSH sources and their per-file checkpoints (so the poller resumes from the current
file tails and never re-ingests history), metric definitions, notification rules, saved views, the
open-stream and stitch-checkpoint state for the live tail.

Run on the server with the worker STOPPED:
    cd /opt/RAG-Pipeline/RAG-PipeLine
    venv/bin/python scripts/keep_recent_days.py            # dry run: prints what would happen
    venv/bin/python scripts/keep_recent_days.py --apply    # does it
"""
import asyncio
import re
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from app.config.database import async_session
from app.services.analytics import pending_windows

APPLY = "--apply" in sys.argv
CUTOFF = datetime(2026, 9, 12, 23, 0, tzinfo=timezone.utc)   # 00:00 London on 13 Sep, as asyncpg wants it
KEEP_FROM_SUFFIX = "2026_09_13"            # partition name suffix of the first kept day
DAILY_PARENTS = ("log_entries", "log_entry_assignment", "log_transactions")
TRUNCATE = ("analytics_facts", "analytics_fact_ledger", "analytics_record_facts", "analytics_hourly_rollups",
            "analytics_daily_rollups", "analytics_monthly_rollups", "analytics_quality_issues",
            "analytics_pending_windows", "analytics_tenant_state", "analytics_feature_sets",
            "analytics_predictions", "analytics_field_registry", "analytics_transaction_registry")
DELETES = {
    "log_regroup_pending": "DELETE FROM log_regroup_pending WHERE range_end < :c",
    "log_regroup_runs": "DELETE FROM log_regroup_runs WHERE created_at < :c",
    "log_source_objects (ingested/abandoned)":
        "DELETE FROM log_source_objects WHERE created_at < :c AND status IN ('ingested','abandoned')",
    "notification_deliveries":
        "DELETE FROM notification_deliveries WHERE event_id IN (SELECT id FROM notification_events WHERE created_at < :c)",
    "notification_events": "DELETE FROM notification_events WHERE created_at < :c",
}
JOBS = """DELETE FROM jobs j WHERE j.created_at < :c
          AND NOT EXISTS (SELECT 1 FROM log_transactions t WHERE t.job_id = j.id)
          AND NOT EXISTS (SELECT 1 FROM log_entries e WHERE e.job_id = j.id)
          AND NOT EXISTS (SELECT 1 FROM log_source_objects o WHERE o.job_id = j.id)"""


async def main() -> None:
    async with async_session() as db:
        await db.execute(text("SET LOCAL statement_timeout = 0"))
        drops = []
        for parent in DAILY_PARENTS:
            names = (await db.execute(text(
                "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                "WHERE i.inhparent = CAST(:p AS regclass) ORDER BY 1"), {"p": parent})).scalars().all()
            for n in names:
                m = re.search(r"(\d{4}_\d{2}_\d{2})$", n)
                if m and m.group(1) < KEEP_FROM_SUFFIX:
                    drops.append(n)
        print(f"partitions to drop: {len(drops)}  ({drops[0]} .. {drops[-1]})")
        for label, q in DELETES.items():
            n = (await db.execute(text(q.replace("DELETE FROM", "SELECT count(*) FROM", 1)
                                       if not q.startswith("DELETE FROM notification_deliveries")
                                       else "SELECT count(*) FROM notification_deliveries WHERE event_id IN "
                                            "(SELECT id FROM notification_events WHERE created_at < :c)"),
                                  {"c": CUTOFF})).scalar()
            print(f"  delete {label:44s} {n}")
        print(f"  delete jobs < cutoff (unreferenced after the drops)  "
              f"{(await db.execute(text('SELECT count(*) FROM jobs WHERE created_at < :c'), {'c': CUTOFF})).scalar()} candidates")
        for t in TRUNCATE:
            print(f"  truncate {t:36s} {(await db.execute(text(f'SELECT count(*) FROM {t}'))).scalar()}")
        kept = (await db.execute(text("SELECT count(*) FROM log_transactions WHERE started_at >= :c"),
                                 {"c": CUTOFF})).scalar()
        print(f"transactions kept (started >= {CUTOFF.isoformat()}): {kept}")
        if not APPLY:
            print("DRY RUN, nothing changed")
            return
        for n in drops:
            await db.execute(text(f"DROP TABLE {n}"))
        for q in DELETES.values():
            await db.execute(text(q), {"c": CUTOFF})
        r = await db.execute(text(JOBS), {"c": CUTOFF})
        print("jobs deleted:", r.rowcount)
        await db.execute(text("TRUNCATE " + ", ".join(TRUNCATE)))
        await db.execute(text("UPDATE analytics_metrics SET backfilled_through = NULL"))
        now = datetime.now(timezone.utc)
        for cc in ("tmp-live", "tmp-test"):
            n = await pending_windows.publish(db, cc, lo=CUTOFF, hi=now)
            print(f"tickets published for {cc}: {n}")
        await db.commit()
        print("APPLIED")


if __name__ == "__main__":
    asyncio.run(main())
