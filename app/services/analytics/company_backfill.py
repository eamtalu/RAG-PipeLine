"""Filling the company on facts and ledger versions written before chunk 226.

A fact takes the company of its source transaction. A fact whose transaction no longer exists
takes the customer's only company for its warehouse, and stays empty when the kept transactions
name two or more, because guessing would file a row under the wrong site. The work goes partition
by partition in small batches, each in its own transaction, so it can be stopped at any batch and
run again. Used by `scripts/backfill_fact_company.py`; nothing in the serving path imports this.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass
from typing import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import SessionFactory

#: The only tables this module may write: both carry the fact columns and the company.
TABLES = ("analytics_facts", "analytics_fact_ledger")
_NAME = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True, slots=True)
class Batch:
    """What one batch did: the last row it looked at, how many it read, how many it filled."""

    last_id: uuid.UUID | None
    read: int
    filled: int


@dataclass(slots=True)
class Totals:
    """The sum over a run, and whether every partition was walked to its end."""

    read: int = 0
    filled: int = 0
    batches: int = 0
    is_finished: bool = False


def _checked(name: str) -> str:
    """A relation name that is safe to place in SQL text, or an error.

    >>> _checked("analytics_facts_2026_11")
    'analytics_facts_2026_11'
    >>> _checked("facts; drop table x")
    Traceback (most recent call last):
    ...
    ValueError: not a plain relation name: 'facts; drop table x'
    """
    if not _NAME.match(name):
        raise ValueError(f"not a plain relation name: {name!r}")
    return name


async def partitions_of(db: AsyncSession, table: str) -> list[str]:
    """The partitions of a fact table, by name.

    ("analytics_facts") -> ["analytics_facts_2026_10", ..., "analytics_facts_default"]
    """
    rows = await db.execute(text(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = :t ORDER BY c.relname"),
        {"t": table})
    return [_checked(r[0]) for r in rows]


async def single_companies(db: AsyncSession, customer_code: str) -> dict[str, str]:
    """Each warehouse the customer's kept transactions file under exactly one company.

    (BRI under 915 only; BIC under 920 and 940) -> {"BRI": "915"}
    """
    rows = await db.execute(text(
        "SELECT warehouse, min(company) FROM log_transactions "
        "WHERE customer_code = :cc AND warehouse IS NOT NULL AND company IS NOT NULL "
        "GROUP BY warehouse HAVING count(DISTINCT company) = 1"), {"cc": customer_code})
    return {warehouse: company for warehouse, company in rows}


def _batch_sql(partition: str, singles: dict[str, str], *, has_cursor: bool, dry_run: bool) -> str:
    """The statement for one batch: pick rows, find their company, fill it, report.

    (dry_run) -> the same picks and the same count of fillable rows, with no UPDATE
    """
    cursor = "AND id > :after" if has_cursor else ""
    if singles:
        pairs = ", ".join(f"(CAST(:w{i} AS text), CAST(:c{i} AS text))"
                          for i in range(len(singles)))
        rule = (f"LEFT JOIN (VALUES {pairs}) one(warehouse, company) "
                "ON t.id IS NULL AND one.warehouse = todo.warehouse")
        company = "COALESCE(t.company, one.company)"
    else:
        rule, company = "", "t.company"
    # A statement that writes must sit at the top level of the WITH, so the dry run differs only
    # by leaving that one part out and counting the rows it would have written.
    if dry_run:
        write, filled = "", "SELECT count(*) FROM found WHERE company IS NOT NULL"
    else:
        write = (f", done AS (UPDATE {partition} f SET company = found.company FROM found "
                 "WHERE f.id = found.id AND found.company IS NOT NULL RETURNING 1)")
        filled = "SELECT count(*) FROM done"
    return f"""
        WITH todo AS MATERIALIZED (
            SELECT id, source_transaction_id, source_started_at, warehouse FROM {partition}
             WHERE customer_code = :cc AND company IS NULL AND warehouse IS NOT NULL {cursor}
             ORDER BY id LIMIT :limit),
        found AS MATERIALIZED (
            SELECT todo.id, {company} AS company FROM todo
              LEFT JOIN log_transactions t
                ON t.customer_code = :cc AND t.id = todo.source_transaction_id
               AND (t.started_at = todo.source_started_at
                    OR (t.started_at IS NULL AND todo.source_started_at IS NULL))
              {rule})
        {write}
        SELECT (SELECT id FROM todo ORDER BY id DESC LIMIT 1) AS last_id,
               (SELECT count(*) FROM todo) AS read,
               ({filled}) AS filled
    """


async def fill_batch(db: AsyncSession, partition: str, customer_code: str, *,
                     after: uuid.UUID | None, limit: int, singles: dict[str, str],
                     dry_run: bool = False) -> Batch:
    """Fill the company on the next `limit` rows of one partition that lack it.

    (5 rows lack it, limit 2) -> Batch(last_id=<2nd>, read=2, filled=2) ; (none left) -> read 0
    """
    params: dict = {"cc": customer_code, "limit": limit}
    if after is not None:
        params["after"] = after
    for i, (warehouse, company) in enumerate(sorted(singles.items())):
        params[f"w{i}"], params[f"c{i}"] = warehouse, company
    sql = _batch_sql(_checked(partition), singles, has_cursor=after is not None, dry_run=dry_run)
    row = (await db.execute(text(sql), params)).one()
    return Batch(last_id=row.last_id, read=int(row.read), filled=int(row.filled))


async def fill_table(session_factory: SessionFactory, table: str, customer_code: str, *,
                     batch: int = 5000, pause_seconds: float = 0.0, dry_run: bool = False,
                     max_batches: int | None = None,
                     on_batch: Callable[[str, Batch], None] | None = None) -> Totals:
    """Fill the company on every row of one fact table that lacks it, a batch per transaction.

    (5 old rows, batch 2) -> Totals(read=5, filled=5, batches=3, is_finished=True)
    ; (max_batches=1) -> Totals(filled=2, is_finished=False), and the next run does the rest
    """
    if table not in TABLES:
        raise ValueError(f"{table!r} is not a fact table; expected one of {TABLES}")
    totals = Totals()
    # Step 1: Read the rule and the partition list once, so that every batch of the run judges a
    # row whose transaction is gone by the same set of companies.
    async with session_factory() as db:
        singles = await single_companies(db, customer_code)
        partitions = await partitions_of(db, table)
    for partition in partitions:
        after: uuid.UUID | None = None
        while True:
            if max_batches is not None and totals.batches >= max_batches:
                return totals
            # Step 2: One short transaction per batch, because a long one would hold row locks
            # the fact writer waits on and would lose everything on a cancelled statement.
            async with session_factory() as db:
                done = await fill_batch(db, partition, customer_code, after=after, limit=batch,
                                        singles=singles, dry_run=dry_run)
                await db.commit()
            if done.read == 0:
                break
            totals.read += done.read
            totals.filled += done.filled
            totals.batches += 1
            if on_batch is not None:
                on_batch(partition, done)
            # Step 3: Move past the rows just read by id, otherwise a row that cannot be filled
            # would be picked again by every batch and the walk would never end.
            after = done.last_id
            if pause_seconds:
                await asyncio.sleep(pause_seconds)
    totals.is_finished = True
    return totals
