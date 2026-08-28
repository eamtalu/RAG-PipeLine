import asyncio, re
async def main():
    import asyncpg
    from dotenv import dotenv_values
    env = dotenv_values("/opt/RAG-Pipeline/RAG-PipeLine/.env")
    url = re.sub(r"\+asyncpg", "", env.get("DATABASE_URL") or env.get("database_url"))
    conn = await asyncpg.connect(url)
    q = conn.fetchval
    print("regroup_backlog:", await q("SELECT count(*) FROM log_regroup_pending WHERE consumed_at IS NULL AND abandoned_at IS NULL"))
    print("analytics_backlog:", await q("SELECT count(*) FROM analytics_pending_windows WHERE consumed_at IS NULL AND abandoned_at IS NULL"))
    print("dead_letters:", await q("SELECT (SELECT count(*) FROM log_regroup_pending WHERE abandoned_at IS NOT NULL) + (SELECT count(*) FROM analytics_pending_windows WHERE abandoned_at IS NOT NULL)"))
    print("orphans:", await q("SELECT count(*) FROM log_entries e WHERE e.timestamp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM log_entry_assignment a WHERE a.entry_id = e.id)"))
    print("newest_entry:", await q("SELECT max(timestamp) FROM log_entries"))
    print("checkpoint:", await q("SELECT stitched_through FROM log_stitch_checkpoint WHERE customer_code='tmp-live'"))
    print("facts_last_hour:", await q("SELECT count(*) FROM analytics_facts WHERE created_at > now() - interval '1 hour'"))
    rows = await conn.fetch("""SELECT s.transaction_id, t.status, t.ended_at
        FROM log_open_stream s LEFT JOIN log_transactions t ON t.id = s.transaction_id
        WHERE s.customer_code='tmp-live'""")
    closed = [r for r in rows if r["status"] is not None and str(r["status"]) != "incomplete"]
    print("parked_streams:", len(rows), "| parked-but-closed (stale):", len(closed))
    await conn.close()
asyncio.run(main())
