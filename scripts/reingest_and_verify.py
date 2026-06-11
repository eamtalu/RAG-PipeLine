"""One-off: re-ingest logs/processed, run Stage 2 regroup, verify user-aware response matching.

Run:  PYTHONPATH="$PWD" python scripts/reingest_and_verify.py
"""

import asyncio
from pathlib import Path

from sqlalchemy import text

from app.config.database import async_session
from app.persistence.repositories.job_repository import JobRepository
from app.persistence.storage import get_storage
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion
from app.services.mnp_log_ingestion.pipeline.derive_transactions import regroup_all

SRC = Path("logs/processed")


async def reingest() -> int:
    storage = get_storage()
    files = sorted(p for p in SRC.iterdir() if p.is_file() and not p.name.startswith("."))
    total_new = 0
    for p in files:
        data = p.read_bytes()
        async with async_session() as db:
            ingestion = LogIngestion(storage, JobRepository(db))
            job = await ingestion.ingest(data, p.name, background=False)
        async with async_session() as db:
            cnt = await db.scalar(text("SELECT chunk_count FROM jobs WHERE id = :i"), {"i": job.id})
        total_new += cnt or 0
        print(f"  {p.name:45s} +{cnt or 0} new")
    print(f"Re-ingest complete: {total_new} new entries from {len(files)} files")
    return total_new


async def verify() -> None:
    async with async_session() as db:
        entries = await db.scalar(text("SELECT count(*) FROM log_entries"))
        with_user = await db.scalar(text("SELECT count(*) FROM log_entries WHERE user_ctx IS NOT NULL"))
        responses = await db.scalar(text("SELECT count(*) FROM log_entries WHERE entry_type = 'response'"))
        resp_user = await db.scalar(
            text("SELECT count(*) FROM log_entries WHERE entry_type = 'response' AND user_ctx IS NOT NULL")
        )
        print(f"\nlog_entries: {entries}  (user_ctx set on {with_user})")
        print(f"responses: {responses}  (with user_ctx: {resp_user})")

    print("\n--- Stage 2 regroup ---")
    async with async_session() as db:
        stats = await regroup_all(db)
    print(stats)

    async with async_session() as db:
        # 1. transactions whose entries span more than one distinct user_ctx -> cross-user contamination
        cross = await db.scalar(text(
            """
            SELECT count(*) FROM (
              SELECT transaction_id
              FROM log_entries
              WHERE transaction_id IS NOT NULL AND user_ctx IS NOT NULL
              GROUP BY transaction_id
              HAVING count(DISTINCT user_ctx) > 1
            ) x
            """
        ))
        # 2. a transaction's RESPONSE user must equal the transaction's request/work user
        resp_mismatch = await db.scalar(text(
            """
            WITH txn_user AS (
              SELECT transaction_id, min(user_ctx) AS u
              FROM log_entries
              WHERE transaction_id IS NOT NULL AND entry_type <> 'response' AND user_ctx IS NOT NULL
              GROUP BY transaction_id
            ),
            resp AS (
              SELECT transaction_id, user_ctx AS ru
              FROM log_entries
              WHERE entry_type = 'response' AND transaction_id IS NOT NULL AND user_ctx IS NOT NULL
            )
            SELECT count(*) FROM resp r JOIN txn_user t USING (transaction_id)
            WHERE r.ru <> t.u
            """
        ))
        # 3. structural sanity: transactions with >1 request / >1 response
        multi_req = await db.scalar(text(
            "SELECT count(*) FROM (SELECT transaction_id FROM log_entries WHERE entry_type='request' "
            "AND transaction_id IS NOT NULL GROUP BY transaction_id HAVING count(*)>1) x"
        ))
        multi_resp = await db.scalar(text(
            "SELECT count(*) FROM (SELECT transaction_id FROM log_entries WHERE entry_type='response' "
            "AND transaction_id IS NOT NULL GROUP BY transaction_id HAVING count(*)>1) x"
        ))
        txns = await db.scalar(text("SELECT count(*) FROM log_transactions"))
        by_status = (await db.execute(text(
            "SELECT status, count(*) FROM log_transactions GROUP BY status ORDER BY 2 DESC"
        ))).all()

    print("\n================ VERIFICATION ================")
    print(f"transactions:            {txns}")
    print(f"by status:               {dict(by_status)}")
    print(f"CROSS-USER transactions: {cross}   (must be 0)")
    print(f"response-user mismatch:  {resp_mismatch}   (must be 0)")
    print(f"multi-request txns:      {multi_req}   (must be 0)")
    print(f"multi-response txns:     {multi_resp}   (must be 0)")
    ok = cross == 0 and resp_mismatch == 0 and multi_req == 0 and multi_resp == 0
    print("RESULT:", "PASS ✅" if ok else "FAIL ❌")


async def main() -> None:
    await reingest()
    await verify()


if __name__ == "__main__":
    asyncio.run(main())
