import asyncio, re
from datetime import datetime, timezone
async def main():
    import asyncpg
    from dotenv import dotenv_values
    env = dotenv_values("/opt/RAG-Pipeline/RAG-PipeLine/.env")
    url = re.sub(r"\+asyncpg", "", env.get("DATABASE_URL") or env.get("database_url"))
    conn = await asyncpg.connect(url)
    lo1 = datetime(2026, 8, 28, 11, 49, 30, 323000, tzinfo=timezone.utc)
    print("=== window 1 lo:", lo1)
    for tid in ("f8734a04-3dd0-55ba-a524-d26912b232c0",   # actual owner of first mismatched entry
                "f0feaa75-1d0e-5252-85ba-1e6b20814c85",
                "d6e633b8-1bce-5a3a-85c9-1e83cc6c3789",
                "1eecc356-b42e-5ad0-9082-e22ca174e577"):  # planned-only (last in chain)
        rows = await conn.fetch("""SELECT e.entry_type, e.timestamp, e.thread, e.user_ctx,
                   split_part(e.source_file,'/',1) AS srv
            FROM log_entry_assignment a JOIN log_entries e ON e.id=a.entry_id
            WHERE a.transaction_id=$1::uuid ORDER BY e.timestamp, e.line_number LIMIT 4""", tid)
        if not rows:
            print(tid[:8], "-> no assignments (does txn exist?)",
                  await conn.fetchval("SELECT count(*) FROM log_transactions WHERE id=$1::uuid", tid))
            continue
        n = await conn.fetchval("SELECT count(*) FROM log_entry_assignment WHERE transaction_id=$1::uuid", tid)
        first = rows[0]
        print(tid[:8], f"({n} entries) first: {first['entry_type']} {first['timestamp']} thread {first['thread']} {first['user_ctx']} {first['srv']}",
              "| first BELOW window lo" if first["timestamp"] < lo1 else "| first inside window")
    # mismatched entries themselves
    for eid in ("6227013c-938c-4e87-a3d5-cff55db0f410",
                "aa740fc8-210e-4963-b05a-e79c33924b4e",
                "603933a7-f588-46b3-8001-58bc7888ccf0"):
        e = await conn.fetchrow("SELECT entry_type, timestamp, thread, user_ctx FROM log_entries WHERE id=$1::uuid", eid)
        print("entry", eid[:8], e["entry_type"], e["timestamp"], "thread", e["thread"], e["user_ctx"])
    await conn.close()
asyncio.run(main())
