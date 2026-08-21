"""Purge the orphaned rows sitting in the DEFAULT partitions.

Run (DRY RUN, the default -- reports everything, writes nothing):

    PYTHONPATH="$PWD" DATABASE_URL=... python scripts/purge_default_partition_rows.py

Run for real:

    PYTHONPATH="$PWD" DATABASE_URL=... python scripts/purge_default_partition_rows.py \
        --execute --confirm-target 192.168.0.142 --confirm-backup

Why this exists
---------------
The partition runway is built FORWARD only -- `partitioning.coverage_days(today, ahead=N)` covers
`[today, today + N]`, and nothing provisions a past day. So historical log files ingested after
partitioning was introduced had no partition for their days, and PostgreSQL routed every one of those
rows into the DEFAULT partition. Measured on the live server 2026-08-21: 294,747 entries plus their
294,747 assignments plus 15,622 transactions, 448 MB, spanning 2026-06-29 to 2026-08-04 UTC.

Retention will NEVER reclaim them: `drop_partition_sql` only ever names a DATED partition, and DEFAULT
is skipped everywhere a day is derived. So they do not age out. They sit there permanently, scanned by
every range query because a DEFAULT partition cannot be pruned from a range predicate, which is the
single-heap cost partitioning exists to remove.

The data is 17 to 53 days old against a 60-day retention policy, so all of it would have been dropped
within about six weeks anyway. Purging it now does in one step what the policy does over that period.

Why TRUNCATE and not DELETE
---------------------------
Everything in these DEFAULT partitions is being removed, so this is a whole-relation operation:

  - `TRUNCATE` is O(1). It does not write per-row WAL and does not create 3M dead index entries across
    the 32 partitioned indexes these three tables carry.
  - It returns the space to the operating system immediately. A `DELETE` only marks it reusable, so it
    would need a follow-up `VACUUM FULL` and an exclusive lock to actually free 448 MB.
  - It is transactional in PostgreSQL, so it is still all-or-nothing.

That matters on this host specifically: the repo documents a degraded disk with bad sectors, and a
600k-row DELETE plus a VACUUM FULL is a great deal more I/O than three catalogue operations.

`log_entries_default` is the one exception. It holds a single row whose timestamp could not be parsed
-- a genuinely truncated log line, not misfiled July data -- so by default that row is PRESERVED and
this uses a bounded DELETE there instead. Pass `--drop-unparsable` to remove it too and take the
TRUNCATE path for all three. See the note that flag prints.

There is no in-place rollback once committed. `--execute` therefore requires one of two explicit
acknowledgements: `--confirm-backup` (a verified dump exists) or `--accept-irreversible` (there is no
backup and the loss is acceptable). Which one was given is printed into the run output, so the record
says what was actually true rather than whichever flag happened to unlock the gate.
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass

from sqlalchemy import text

from app.config.database import async_session
from app.persistence import partitioning as pt


@dataclass(frozen=True)
class Target:
    table: str
    key: str


#: Reported and purged in this order: the entries, then the links that point at them, then the
#: transactions those links point at. There are no foreign keys between them (assignments have none by
#: design), and everything happens in ONE transaction, so the order is for readability only.
TARGETS: tuple[Target, ...] = (
    Target("log_entries", "timestamp"),
    Target("log_entry_assignment", "entry_ts"),
    Target("log_transactions", "started_at"),
)


# ============================================================== reads
async def connection_identity(db) -> tuple[str, str]:
    """(host, database) actually connected to. Matched against `--confirm-target` before any write:
    the classic way to lose data here is to fire a correct script at the wrong database."""
    row = (await db.execute(text(
        "SELECT COALESCE(inet_server_addr()::text, 'local-socket'), current_database()"))).one()
    return row[0], row[1]


async def survey(db, t: Target) -> dict:
    """What is in `t`'s DEFAULT partition, and how much of the table it is."""
    default = pt.default_partition_name(t.table)
    row = (await db.execute(text(f"""
        SELECT (SELECT count(*) FROM {t.table})                                  AS grand,
               (SELECT count(*) FROM {default})                                  AS in_default,
               (SELECT count(*) FROM {default} WHERE "{t.key}" IS NULL)          AS null_key,
               (SELECT min(("{t.key}" AT TIME ZONE 'UTC')::date) FROM {default}
                 WHERE "{t.key}" IS NOT NULL)                                    AS first_day,
               (SELECT max(("{t.key}" AT TIME ZONE 'UTC')::date) FROM {default}
                 WHERE "{t.key}" IS NOT NULL)                                    AS last_day,
               pg_size_pretty(pg_total_relation_size('{default}'))                AS size
    """))).one()
    return {"grand": int(row.grand), "in_default": int(row.in_default),
            "null_key": int(row.null_key), "first_day": row.first_day,
            "last_day": row.last_day, "size": row.size}


async def oldest_dated_partition(db):
    """The first day `log_entries` has a real partition for, or None.

    Everything in DEFAULT is strictly older than this (verified in preflight), which is what makes it
    usable as the boundary instant below instead of an anti-join.
    """
    return (await db.execute(text("""
        SELECT min((regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \\(''([0-9-]{10})'))[1]::date)
        FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'log_entries'::regclass
          AND pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
    """))).scalar()


async def midnight_casualties(db, oldest) -> dict:
    """Transactions about to be purged that still own entries which are NOT being purged.

    A transaction starting just before midnight UTC owns entries into the next day. If its `started_at`
    is in DEFAULT but some of its entries are in a real dated partition, purging the transaction leaves
    those entries unassigned and their assignment rows pointing at a dead id.

    Bounded and small by construction: a transaction spans at most the seal window, so only ones
    starting in the final minutes of the last orphaned day can straddle. Reported rather than assumed,
    because "small" is not "none" and the repair is a scoped regroup over that boundary.

    Keyed on `entry_ts >= boundary` rather than on "is this entry in DEFAULT". `entry_ts` is a copy of
    the entry's own timestamp and it is the PARTITION KEY, so the predicate prunes at plan time. The
    obvious formulation -- `entry_id NOT IN (SELECT id FROM log_entries_default)` -- is an anti-join of
    2.7M rows against 294k with no bound, and it does not finish.
    """
    if oldest is None:
        return {"assignments": 0, "transactions": 0, "entries": 0}
    row = (await db.execute(text("""
        SELECT count(*) AS assignments,
               count(DISTINCT a.transaction_id) AS transactions,
               count(DISTINCT a.entry_id) AS entries
        FROM log_entry_assignment a
        JOIN log_transactions_default d ON d.id = a.transaction_id
        WHERE a.entry_ts >= :edge AND d.started_at IS NOT NULL
    """), {"edge": pt.day_start(oldest)})).one()
    return {"assignments": int(row.assignments), "transactions": int(row.transactions),
            "entries": int(row.entries)}


async def preflight(db, oldest) -> list[str]:
    """Conditions that must hold before writing. Empty list means clear."""
    problems: list[str] = []
    open_windows = int(await db.scalar(text(
        "SELECT count(*) FROM log_regroup_pending "
        "WHERE consumed_at IS NULL AND abandoned_at IS NULL")) or 0)
    if open_windows:
        problems.append(f"{open_windows} open stitch window(s): Stage 2 has unfinished work and may "
                        f"be writing to these tables. Let it drain first.")

    # Anything in DEFAULT dated on or after the oldest real partition would mean the runway story is
    # not what we think, and a blanket purge could take data that has a proper home.
    if oldest is None:
        problems.append("log_entries has no dated partitions at all: refusing to purge DEFAULT, "
                        "because then DEFAULT is the only place the data lives.")
    else:
        stragglers = int(await db.scalar(text(
            "SELECT count(*) FROM log_entries_default WHERE timestamp >= :edge"),
            {"edge": pt.day_start(oldest)}) or 0)
        if stragglers:
            problems.append(
                f"{stragglers} row(s) in log_entries_default are dated on or after {oldest}, which "
                f"HAS a partition. That is not the forward-runway story this script assumes; "
                f"investigate before purging.")
    return problems


# ============================================================== the purge
def purge_statements(*, drop_unparsable: bool) -> list[str]:
    """The statements, in order, as one atomic batch.

    `log_entries_default` keeps its NULL-key row unless asked otherwise, so it gets a bounded DELETE
    while the other two -- which hold nothing but orphans -- are TRUNCATEd.
    """
    entries_default = pt.default_partition_name("log_entries")
    stmts = []
    if drop_unparsable:
        stmts.append(f"TRUNCATE {entries_default}")
    else:
        stmts.append(f"DELETE FROM {entries_default} WHERE timestamp IS NOT NULL")
    stmts.append(f"TRUNCATE {pt.default_partition_name('log_entry_assignment')}")
    stmts.append(f"TRUNCATE {pt.default_partition_name('log_transactions')}")
    return stmts


# ============================================================== driver
async def run(args) -> int:
    async with async_session() as db:
        host, database = await connection_identity(db)
        mode = "EXECUTE" if args.execute else "DRY RUN"
        print(f"\n{'=' * 80}\n{mode}   target: {host}   database: {database}\n{'=' * 80}")

        # A hard ceiling per statement: the reads here are all counts and catalogue lookups, so
        # anything running for minutes is a query bug rather than slow data. Fail loudly, not hang.
        await db.execute(text("SET LOCAL statement_timeout = '120s'"))
        # And a SEPARATE, much shorter ceiling on waiting for a LOCK. TRUNCATE needs ACCESS EXCLUSIVE,
        # so any concurrent reader of the partition -- including the status card's own polled
        # count(*) -- makes it queue. Without this the wait burns the statement timeout and reports
        # "statement timeout", which points at the wrong problem entirely.
        await db.execute(text("SET LOCAL lock_timeout = '15s'"))

        oldest = await oldest_dated_partition(db)
        before = {t.table: await survey(db, t) for t in TARGETS}
        casualties = await midnight_casualties(db, oldest)

        # ---- report ----
        total = 0
        for t in TARGETS:
            s = before[t.table]
            purged = s["in_default"] if args.drop_unparsable else s["in_default"] - s["null_key"]
            total += purged
            span = f"{s['first_day']} .. {s['last_day']}" if s["first_day"] else "-"
            print(f"\n{t.table}   (key: {t.key}, {s['size']} in DEFAULT)")
            print(f"  grand total {s['grand']:>12,}")
            print(f"  in DEFAULT  {s['in_default']:>12,}   UTC days {span}")
            if s["null_key"]:
                fate = "ALSO PURGED (--drop-unparsable)" if args.drop_unparsable else "PRESERVED"
                print(f"  NULL key    {s['null_key']:>12,}   <- {fate}")
            print(f"  to purge    {purged:>12,}   leaving {s['grand'] - purged:,}")

        print(f"\ntotal to purge: {total:,} row(s), reclaiming roughly "
              f"{', '.join(before[t.table]['size'] for t in TARGETS)}")

        if not args.drop_unparsable and before["log_entries"]["null_key"]:
            print("\nNOTE: the unparsable row is preserved, so `default_partition_rows` stays "
                  "non-zero and the status card stays amber. Nothing will ever clear it, because that "
                  "line can never become parseable. Pass --drop-unparsable to remove it too and get "
                  "the card back to green.")

        if casualties["assignments"]:
            print(f"\nMIDNIGHT BOUNDARY: {casualties['transactions']:,} transaction(s) being purged "
                  f"still own {casualties['entries']:,} entr(y/ies) that are NOT being purged "
                  f"({casualties['assignments']:,} assignment row(s)).")
            print("  After the purge those entries are unassigned and their assignment rows point at "
                  "a dead transaction id. Repair with a scoped regroup over that boundary: "
                  "POST /api/v1/logs/regroup/finalize, or POST /api/v1/logs/regroup for the tenant.")
        else:
            print("\nMIDNIGHT BOUNDARY: clean -- every purged transaction's entries are also being "
                  "purged, so nothing is left half-referenced.")

        stmts = purge_statements(drop_unparsable=args.drop_unparsable)

        if not args.execute:
            print(f"\n{'-' * 80}\nDRY RUN: the statements that WOULD run, in ONE transaction\n"
                  f"{'-' * 80}")
            for s in stmts:
                print(f"  {s};")
            print("\nNothing was written. Check the counts above, take a fresh dump, then re-run with "
                  "--execute --confirm-target <host> --confirm-backup")
            return 0

        # ---- gates ----
        if args.confirm_target != host:
            print(f"\nREFUSING: --confirm-target {args.confirm_target!r} does not match the connected "
                  f"host {host!r}. Point DATABASE_URL where you mean to.")
            return 2
        if not (args.confirm_backup or args.accept_irreversible):
            print("\nREFUSING: there is no in-place rollback, so one of these is required:")
            print("  --confirm-backup       a fresh verified dump exists")
            print("  --accept-irreversible  no backup, and the loss is acceptable")
            return 2
        print("\nacknowledged: " + ("a verified backup exists" if args.confirm_backup
                                    else "NO BACKUP -- operator accepts irreversible loss"))
        problems = await preflight(db, oldest)
        if problems:
            print("\nREFUSING, pre-flight failed:")
            for p in problems:
                print(f"  - {p}")
            return 2
        print("\npre-flight: clear")

        # ---- one atomic transaction ----
        for s in stmts:
            await db.execute(text(s))
            print(f"  ran: {s}")
        await db.commit()

        # ---- verify ----
        print(f"\n{'-' * 80}\nVERIFY\n{'-' * 80}")
        ok = True
        for t in TARGETS:
            b, a = before[t.table], await survey(db, t)
            expected_grand = b["grand"] - (b["in_default"] if args.drop_unparsable
                                           else b["in_default"] - b["null_key"])
            expected_default = 0 if args.drop_unparsable else b["null_key"]
            print(f"  {t.table:<22} total {b['grand']:,} -> {a['grand']:,} (expected "
                  f"{expected_grand:,})   DEFAULT {b['in_default']:,} -> {a['in_default']:,} "
                  f"(expected {expected_default:,})   now {a['size']}")
            if a["grand"] != expected_grand:
                ok = False
                print(f"    !! grand total is {a['grand'] - expected_grand:+,} off expectation")
            if a["in_default"] != expected_default:
                ok = False
                print(f"    !! DEFAULT still holds {a['in_default']:,}, expected {expected_default:,}")

        print(f"\n{'ALL CHECKS PASSED' if ok else 'CHECKS FAILED'}")
        if ok:
            print("\nTRUNCATE returned its space to the operating system immediately; no VACUUM FULL "
                  "is needed for those two tables.")
            if not args.drop_unparsable:
                print("log_entries_default was DELETEd from, so its space is reusable but not yet "
                      "returned. `VACUUM FULL log_entries_default;` reclaims it (brief exclusive "
                      "lock, tiny table once emptied).")
            if casualties["assignments"]:
                print("Run the boundary repair now -- see MIDNIGHT BOUNDARY above.")
        return 0 if ok else 1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Purge the orphaned rows in the DEFAULT partitions. Dry run unless --execute.")
    p.add_argument("--execute", action="store_true",
                   help="actually write. Omit for a dry run, which is the default.")
    p.add_argument("--confirm-target", metavar="HOST",
                   help="must equal the connected server host. Required with --execute.")
    p.add_argument("--confirm-backup", action="store_true",
                   help="acknowledge a fresh verified dump exists.")
    p.add_argument("--accept-irreversible", action="store_true",
                   help="acknowledge there is NO backup and the loss is acceptable. Use instead of "
                        "--confirm-backup. One of the two is required with --execute.")
    p.add_argument("--drop-unparsable", action="store_true",
                   help="also remove the NULL-timestamp row(s), which are otherwise preserved. "
                        "Without this the status card stays amber forever.")
    return asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
