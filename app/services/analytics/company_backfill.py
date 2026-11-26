"""Filling the company on facts and ledger versions written before chunk 226.

Every fact keeps the attributes of its WMS call, and the company is one of them (`Company`, or
`CONO`): it is the same value ingestion files the transaction under. Measured on 26 Nov 2026,
the two agree on all 479,707 facts whose transaction still exists, so the row itself is the
source and a fact whose transaction was removed is filled like any other. The work goes
partition by partition in small batches, each in its own transaction, so it can be stopped at
any batch and run again. Used by `scripts/backfill_fact_company.py`; nothing in the serving
path imports this.
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


#: The company as the WMS call carried it, or NULL: an empty value and one too long for the
#: column (16 characters) are treated as absent, so a malformed call cannot fail a whole batch.
_OWN_COMPANY = ("CASE WHEN char_length(COALESCE(attributes->>'Company', attributes->>'CONO')) "
                "BETWEEN 1 AND 16 THEN COALESCE(attributes->>'Company', attributes->>'CONO') END")


def _batch_sql(partition: str, *, has_cursor: bool, dry_run: bool) -> str:
    """The statement for one batch: pick rows, read their own company, fill it, report.

    (dry_run) -> the same picks and the same count of fillable rows, with no UPDATE
    """
    cursor = "AND id > :after" if has_cursor else ""
    # A statement that writes must sit at the top level of the WITH, so the dry run differs only
    # by leaving that one part out and counting the rows it would have written.
    if dry_run:
        write, filled = "", "SELECT count(*) FROM todo WHERE company IS NOT NULL"
    else:
        write = (f", done AS (UPDATE {partition} f SET company = todo.company FROM todo "
                 "WHERE f.id = todo.id AND todo.company IS NOT NULL RETURNING 1)")
        filled = "SELECT count(*) FROM done"
    return f"""
        WITH todo AS MATERIALIZED (
            SELECT id, {_OWN_COMPANY} AS company FROM {partition}
             WHERE customer_code = :cc AND company IS NULL {cursor}
             ORDER BY id LIMIT :limit)
        {write}
        SELECT (SELECT id FROM todo ORDER BY id DESC LIMIT 1) AS last_id,
               (SELECT count(*) FROM todo) AS read,
               ({filled}) AS filled
    """


async def fill_batch(db: AsyncSession, partition: str, customer_code: str, *,
                     after: uuid.UUID | None, limit: int, dry_run: bool = False) -> Batch:
    """Fill the company on the next `limit` rows of one partition that lack it.

    (5 rows lack it, limit 2) -> Batch(last_id=<2nd>, read=2, filled=2) ; (none left) -> read 0
    """
    params: dict = {"cc": customer_code, "limit": limit}
    if after is not None:
        params["after"] = after
    sql = _batch_sql(_checked(partition), has_cursor=after is not None, dry_run=dry_run)
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
    # Step 1: List the partitions once and work on each by name, because a batch on the parent
    # table would have no single index to walk in id order.
    async with session_factory() as db:
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
                                        dry_run=dry_run)
                await db.commit()
            if done.read == 0:
                break
            totals.read += done.read
            totals.filled += done.filled
            totals.batches += 1
            if on_batch is not None:
                on_batch(partition, done)
            # Step 3: Move past the rows just read by id, otherwise a row whose call carried no
            # company would be picked again by every batch and the walk would never end.
            after = done.last_id
            if pause_seconds:
                await asyncio.sleep(pause_seconds)
    totals.is_finished = True
    return totals
