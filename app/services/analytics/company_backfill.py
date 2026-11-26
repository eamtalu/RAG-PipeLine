"""Filling the company on facts and ledger versions written before chunk 226.

Every fact keeps the attributes of its WMS call, and the company is one of them (`Company`, or
`CONO`): it is the same value ingestion files the transaction under. Measured on 26 Nov 2026,
the two agree on all 479,707 facts whose transaction still exists, so the row itself is the
source and a fact whose transaction was removed is filled like any other. The work goes
partition by partition, a range of table blocks per batch, each batch in its own transaction, so
it can be stopped at any batch and run again. It walks blocks and not ids because neither table
has an index on its id: a batch ordered by id scanned and sorted the whole partition, which took
22 seconds on the ledger against a 30 second statement limit. Used by
`scripts/backfill_fact_company.py`; nothing in the serving path imports this.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Callable, Iterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import SessionFactory

#: The only tables this module may write: both carry the fact columns and the company.
TABLES = ("analytics_facts", "analytics_fact_ledger")
_NAME = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True, slots=True)
class Batch:
    """What one batch did: how many rows without a company it read, and how many it filled."""

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


async def block_count(db: AsyncSession, partition: str) -> int:
    """How many blocks the partition's table holds now: (a 16 MB table, 8 kB blocks) -> 2048."""
    return int(await db.scalar(text(
        "SELECT pg_relation_size(CAST(:p AS regclass)) / current_setting('block_size')::int"),
        {"p": _checked(partition)}))


def block_ranges(total_blocks: int, blocks: int) -> Iterator[tuple[int, int]]:
    """The block ranges that cover a table, each `blocks` long but the last.

    >>> list(block_ranges(250, 100))
    [(0, 100), (100, 200), (200, 250)]
    >>> list(block_ranges(0, 100))
    []
    """
    for first in range(0, total_blocks, blocks):
        yield first, min(first + blocks, total_blocks)


#: The company as the WMS call carried it, or NULL: an empty value and one too long for the
#: column (16 characters) are treated as absent, so a malformed call cannot fail a whole batch.
_OWN_COMPANY = ("CASE WHEN char_length(COALESCE(attributes->>'Company', attributes->>'CONO')) "
                "BETWEEN 1 AND 16 THEN COALESCE(attributes->>'Company', attributes->>'CONO') END")


def _batch_sql(partition: str, *, dry_run: bool) -> str:
    """The statement for one batch: read a block range, take each row's own company, fill it.

    (dry_run) -> the same rows and the same count of fillable rows, with no UPDATE
    """
    # The block range is read in a fenced subquery (OFFSET 0) and the customer is tested outside
    # it, otherwise the planner may pick the customer index and read every row of the partition
    # to find the few in the range.
    # A statement that writes must sit at the top level of the WITH, so the dry run differs only
    # by leaving that one part out and counting the rows it would have written.
    if dry_run:
        write, filled = "", "SELECT count(*) FROM todo WHERE company IS NOT NULL"
    else:
        # `f.company IS NULL` again: a row the fact writer changed since `todo` was read has
        # moved, is not matched here, and is found by the next run.
        write = (f", done AS (UPDATE {partition} f SET company = todo.company FROM todo "
                 "WHERE f.ctid = todo.row AND f.company IS NULL AND todo.company IS NOT NULL "
                 "RETURNING 1)")
        filled = "SELECT count(*) FROM done"
    return f"""
        WITH todo AS MATERIALIZED (
            SELECT block.row, {_OWN_COMPANY} AS company
              FROM (SELECT ctid AS row, customer_code, company, attributes FROM {partition}
                     WHERE ctid >= CAST(:lo AS tid) AND ctid < CAST(:hi AS tid)
                    OFFSET 0) block
             WHERE block.customer_code = :cc AND block.company IS NULL)
        {write}
        SELECT (SELECT count(*) FROM todo) AS read, ({filled}) AS filled
    """


def _batch_params(customer_code: str, first_block: int, blocks: int) -> dict:
    """The bind values of one batch; a row position is a (block, offset) pair to the driver.

    >>> _batch_params("tmp-live", 1000, 500)
    {'cc': 'tmp-live', 'lo': (1000, 0), 'hi': (1500, 0)}
    """
    return {"cc": customer_code, "lo": (first_block, 0), "hi": (first_block + blocks, 0)}


async def fill_batch(db: AsyncSession, partition: str, customer_code: str, *, first_block: int,
                     blocks: int, dry_run: bool = False) -> Batch:
    """Fill the company on the rows of one block range of a partition that lack it.

    (blocks 0 to 999 hold 4,800 old rows) -> Batch(read=4800, filled=4800) ; (run again) -> read 0
    """
    row = (await db.execute(text(_batch_sql(_checked(partition), dry_run=dry_run)),
                            _batch_params(customer_code, first_block, blocks))).one()
    return Batch(read=int(row.read), filled=int(row.filled))


async def explain_batch(db: AsyncSession, partition: str, customer_code: str, *, first_block: int,
                        blocks: int) -> str:
    """The query plan of one batch's read, as text; nothing is executed.

    (any partition) -> "... Tid Range Scan on analytics_facts_2026_11 ..."
    """
    rows = await db.execute(text("EXPLAIN " + _batch_sql(_checked(partition), dry_run=True)),
                            _batch_params(customer_code, first_block, blocks))
    return "\n".join(r[0] for r in rows)


async def fill_table(session_factory: SessionFactory, table: str, customer_code: str, *,
                     blocks: int = 1000, pause_seconds: float = 0.0, dry_run: bool = False,
                     max_batches: int | None = None,
                     on_batch: Callable[[str, Batch], None] | None = None) -> Totals:
    """Fill the company on every row of one fact table that lacks it, a block range at a time.

    (5 old rows in 2 partitions) -> Totals(read=5, filled=5, batches=2, is_finished=True)
    ; (max_batches=1) -> Totals(is_finished=False), and the next run does the rest
    """
    if table not in TABLES:
        raise ValueError(f"{table!r} is not a fact table; expected one of {TABLES}")
    totals = Totals()
    # Step 1: List the partitions once and work on each by name, so that a block range means
    # one table's blocks.
    async with session_factory() as db:
        partitions = await partitions_of(db, table)
    for partition in partitions:
        # Step 2: Fix the end of the walk now, because every update writes a new copy of its row
        # at the end of the table and following those would never finish.
        async with session_factory() as db:
            total_blocks = await block_count(db, partition)
        for first_block, end_block in block_ranges(total_blocks, blocks):
            if max_batches is not None and totals.batches >= max_batches:
                return totals
            # Step 3: One short transaction per range, because a long one would hold row locks
            # the fact writer waits on and would lose everything on a cancelled statement.
            async with session_factory() as db:
                done = await fill_batch(db, partition, customer_code, first_block=first_block,
                                        blocks=end_block - first_block, dry_run=dry_run)
                await db.commit()
            if done.read == 0:
                continue
            totals.read += done.read
            totals.filled += done.filled
            totals.batches += 1
            if on_batch is not None:
                on_batch(partition, done)
            if pause_seconds:
                await asyncio.sleep(pause_seconds)
    totals.is_finished = True
    return totals
