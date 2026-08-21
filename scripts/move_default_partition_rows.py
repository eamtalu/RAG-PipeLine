"""Move rows out of the DEFAULT partitions into the dated partitions they belong in.

Run (DRY RUN, the default -- prints every statement, writes nothing):

    PYTHONPATH="$PWD" DATABASE_URL=... python scripts/move_default_partition_rows.py

Run for real (all four flags required, see below):

    PYTHONPATH="$PWD" DATABASE_URL=... python scripts/move_default_partition_rows.py \
        --execute --confirm-target 192.168.0.142 --confirm-backup

Why this exists
---------------
The partition runway is built FORWARD only -- `partitioning.coverage_days(today, ahead=N)` covers
`[today, today + N]` and nothing provisions a past day. So when historical log files are ingested
after partitioning was introduced, no partition exists for their days and PostgreSQL routes every one
of those rows into the DEFAULT partition. Measured on the live server 2026-08-21: 294,747 such rows
across three co-partitioned tables, 448 MB, spanning 16 UTC days before the oldest real partition.

Rows there are not lost and reads still find them, but:
  - retention can never reclaim them (`drop_partition_sql` only ever names a DATED partition), and
  - every range query scans all of them, because a DEFAULT partition cannot be pruned from a range
    predicate -- which is the single-heap cost partitioning exists to remove.

This script creates the missing partitions and moves the rows into them, after which the ordinary
retention machinery reclaims them progressively through its existing gates.

Design notes that are load-bearing
----------------------------------
*The DDL comes from `partitioning.create_partition_sql`, never from string-building here.* That module
is the single source of truth for the partition name and for the explicit `+00` bounds. A hand-written
bound off by one hour would file rows into the neighbouring partition, silently.

*Day lists are computed in UTC, explicitly.* `timestamptz::date` uses the SESSION timezone. On a BST
session an instant at `2026-08-04 23:52 UTC` reports as `2026-08-05`, which would provision a
partition for a day with no rows and leave the real rows in DEFAULT. Every date here goes through
`AT TIME ZONE 'UTC'`.

*One transaction per (table, day).* 48 small steps rather than one 448 MB transaction: bounded WAL,
resumable after any failure, and observable as it goes. This host has a documented degraded disk, so a
single enormous transaction is the wrong shape.

*`DELETE ... RETURNING` feeding an `INSERT`, in one statement.* The DDL is visible inside its own
transaction, so the INSERT routes to the partition created two statements earlier. Uniqueness is
enforced per partition (PostgreSQL requires the partition key inside a partitioned unique index
precisely so it can be), so the old index entry in DEFAULT and the new one in the dated partition
cannot collide.

*There is no in-place rollback.* Once a partition exists for a day, its rows can never be routed back
to DEFAULT, so undoing means dropping the partition -- which deletes them. `--confirm-backup` exists
because a verified dump is the only way back.

Idempotent: partition creation is `IF NOT EXISTS` and a day with nothing left to move is a no-op, so
re-running after a failure resumes cleanly.
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import date as date_type

from sqlalchemy import text

from app.config.database import async_session
from app.persistence import partitioning as pt
from app.settings import settings


@dataclass(frozen=True)
class Target:
    """One partitioned table and the column it is cut on."""
    table: str
    key: str


#: Processed in this order. There are no foreign keys between these rows (assignments have none by
#: design), so the order is for observability rather than correctness: entries first, then the links
#: that point at them, then the transactions those links point at.
TARGETS: tuple[Target, ...] = (
    Target("log_entries", "timestamp"),
    Target("log_entry_assignment", "entry_ts"),
    Target("log_transactions", "started_at"),
)


# ============================================================== reads (safe in both modes)
async def connection_identity(db) -> tuple[str, str]:
    """(host, database) actually connected to.

    Printed on every run and matched against `--confirm-target` before any write. The classic way to
    lose data here is to fire a correct script at the wrong database.
    """
    row = (await db.execute(text(
        "SELECT COALESCE(inet_server_addr()::text, 'local-socket'), current_database()"))).one()
    return row[0], row[1]


async def orphan_days(db, t: Target) -> list[tuple[date_type, int]]:
    """(UTC day, row count) for every day sitting in `t`'s DEFAULT partition with a non-NULL key.

    NULL-key rows are excluded and stay where they are: they have no day, so no partition can hold
    them. They are the genuine parser failures and a separate decision.
    """
    default = pt.default_partition_name(t.table)
    rows = (await db.execute(text(f"""
        SELECT ("{t.key}" AT TIME ZONE 'UTC')::date AS utc_day, count(*) AS n
        FROM {default}
        WHERE "{t.key}" IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """))).all()
    return [(d, int(n)) for d, n in rows]


async def counts(db, t: Target) -> tuple[int, int, int]:
    """(grand total, rows in DEFAULT, of those with a NULL key).

    The grand total is the invariant this whole script is judged on: it must not change.
    """
    default = pt.default_partition_name(t.table)
    row = (await db.execute(text(f"""
        SELECT (SELECT count(*) FROM {t.table}),
               (SELECT count(*) FROM {default}),
               (SELECT count(*) FROM {default} WHERE "{t.key}" IS NULL)
    """))).one()
    return int(row[0]), int(row[1]), int(row[2])


async def preflight(db) -> list[str]:
    """Conditions that must hold before writing. Returns a list of problems, empty when clear."""
    problems: list[str] = []

    open_windows = int(await db.scalar(text(
        "SELECT count(*) FROM log_regroup_pending "
        "WHERE consumed_at IS NULL AND abandoned_at IS NULL")) or 0)
    if open_windows:
        problems.append(
            f"{open_windows} open stitch window(s): Stage 2 has unfinished work and may be writing "
            f"to these tables. Let it drain first.")

    # A duplicate inside DEFAULT would fail the re-insert halfway through a day. Checked here rather
    # than discovered at statement 30 of 48.
    dupes = int(await db.scalar(text("""
        SELECT count(*) FROM (
            SELECT 1 FROM log_entries_default
            GROUP BY customer_code, entry_hash, timestamp HAVING count(*) > 1) d""")) or 0)
    if dupes:
        problems.append(f"{dupes} duplicate (customer_code, entry_hash, timestamp) group(s) in "
                        f"log_entries_default: the re-insert would hit a unique violation.")
    return problems


# ============================================================== the move
def move_sql(t: Target) -> str:
    """The one statement that moves a day. Bounds are bound parameters matching the partition edges
    exactly: half-open `[day_start, next_day_start)`, the same arithmetic the DDL uses."""
    return (f"WITH moved AS ("
            f"  DELETE FROM {pt.default_partition_name(t.table)}"
            f'   WHERE "{t.key}" >= :lo AND "{t.key}" < :hi'
            f"  RETURNING *"
            f") INSERT INTO {t.table} SELECT * FROM moved")


async def move_one_day(db, t: Target, day: date_type, *, execute: bool) -> int:
    """Create the partition for `day` and move that day's rows into it. Returns rows moved.

    In dry-run mode the statements are printed and nothing is sent. The count reported is then the
    pre-counted total for that day rather than a rowcount, which is why the caller passes it in.
    """
    ddl = pt.create_partition_sql(t.table, day)
    lo, hi = pt.day_start(day), pt.day_end(day)
    sql = move_sql(t)

    if not execute:
        print(f"    {ddl};")
        print(f"    {sql.replace(':lo', repr(lo.isoformat())).replace(':hi', repr(hi.isoformat()))};")
        return 0

    await db.execute(text(ddl))
    res = await db.execute(text(sql), {"lo": lo, "hi": hi})
    await db.commit()          # one transaction per (table, day)
    return res.rowcount or 0


# ============================================================== driver
async def run(args) -> int:
    async with async_session() as db:
        host, database = await connection_identity(db)
        mode = "EXECUTE" if args.execute else "DRY RUN"
        print(f"\n{'=' * 78}\n{mode}   target: {host}   database: {database}\n{'=' * 78}")

        if args.execute:
            if args.confirm_target != host:
                print(f"\nREFUSING: --confirm-target {args.confirm_target!r} does not match the "
                      f"connected host {host!r}. Point DATABASE_URL where you mean to.")
                return 2
            if not args.confirm_backup:
                print("\nREFUSING: --confirm-backup is required. There is no in-place rollback: once "
                      "a partition exists for a day its rows cannot be routed back to DEFAULT, so "
                      "undoing means dropping the partition and losing them.")
                return 2
            problems = await preflight(db)
            if problems:
                print("\nREFUSING, pre-flight failed:")
                for p in problems:
                    print(f"  - {p}")
                return 2
            print("\npre-flight: clear")

        targets = [t for t in TARGETS if args.table in (None, t.table)]
        if args.table and not targets:
            print(f"\nunknown --table {args.table!r}; known: "
                  f"{', '.join(t.table for t in TARGETS)}")
            return 2

        before = {t.table: await counts(db, t) for t in targets}
        plan: dict[str, list[tuple[date_type, int]]] = {}

        for t in targets:
            days = await orphan_days(db, t)
            if args.day:
                days = [(d, n) for d, n in days if d.isoformat() == args.day]
            plan[t.table] = days

        # ---- report what will happen, before doing any of it ----
        total_rows = 0
        for t in targets:
            days = plan[t.table]
            grand, in_default, null_key = before[t.table]
            print(f"\n{t.table}   (key: {t.key})")
            print(f"  grand total {grand:>12,}   in DEFAULT {in_default:>10,}"
                  f"   of those NULL-key {null_key:>6,}  <- stay put")
            if not days:
                print("  nothing to move")
                continue
            for d, n in days:
                print(f"    {d}  {n:>10,}  -> {pt.partition_name(t.table, d)}")
                total_rows += n
            print(f"  {len(days)} partition(s), {sum(n for _d, n in days):,} row(s) to move")

        print(f"\ntotal to move: {total_rows:,} row(s) across "
              f"{sum(len(v) for v in plan.values())} partition(s)")

        if not args.execute:
            print(f"\n{'-' * 78}\nDRY RUN: statements that WOULD run\n{'-' * 78}")
            for t in targets:
                for d, _n in plan[t.table]:
                    print(f"  -- {t.table} {d}")
                    await move_one_day(db, t, d, execute=False)
            print("\nNothing was written. Compare the per-day counts above against the plan, then "
                  "re-run with --execute --confirm-target <host> --confirm-backup")
            return 0

        # ---- execute ----
        moved_total = 0
        for t in targets:
            for d, expected in plan[t.table]:
                moved = await move_one_day(db, t, d, execute=True)
                flag = "" if moved == expected else f"  !! expected {expected:,}"
                print(f"  moved {t.table} {d}  {moved:>10,}{flag}")
                moved_total += moved

        # ---- verify ----
        print(f"\n{'-' * 78}\nVERIFY\n{'-' * 78}")
        ok = True
        for t in targets:
            grand_b, default_b, null_b = before[t.table]
            grand_a, default_a, null_a = await counts(db, t)
            lost = grand_b - grand_a
            print(f"  {t.table:<22} total {grand_b:,} -> {grand_a:,}"
                  f"   DEFAULT {default_b:,} -> {default_a:,}   NULL-key {null_a:,}")
            if lost:
                ok = False
                print(f"    !! GRAND TOTAL CHANGED BY {lost:+,} -- rows were lost or duplicated")
            if default_a != null_a:
                ok = False
                print(f"    !! DEFAULT still holds {default_a - null_a:,} row(s) with a usable key")
        print(f"\nmoved {moved_total:,} row(s). {'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
        if ok:
            print("\nRetention now reclaims these progressively through its existing gates, oldest "
                  f"first, as each day passes the {settings.log_partition_retention_days}-day cutoff. "
                  "Nothing is eligible immediately.")
        return 0 if ok else 1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Move rows out of the DEFAULT partitions into their dated partitions.")
    p.add_argument("--execute", action="store_true",
                   help="actually write. Omit for a dry run, which is the default.")
    p.add_argument("--confirm-target", metavar="HOST",
                   help="must equal the connected server host. Required with --execute.")
    p.add_argument("--confirm-backup", action="store_true",
                   help="acknowledge a fresh verified dump exists. Required with --execute, because "
                        "there is no in-place rollback.")
    p.add_argument("--table", metavar="NAME",
                   help="restrict to one table (default: all three)")
    p.add_argument("--day", metavar="YYYY-MM-DD",
                   help="restrict to one UTC day (default: every orphaned day)")
    return asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
