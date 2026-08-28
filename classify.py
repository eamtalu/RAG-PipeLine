import asyncio, re
async def main():
    import asyncpg
    from dotenv import dotenv_values
    env = dotenv_values("/opt/RAG-Pipeline/RAG-PipeLine/.env")
    url = re.sub(r"\+asyncpg", "", env.get("DATABASE_URL") or env.get("database_url"))
    conn = await asyncpg.connect(url)
    for eid, planned, actual in [
        ("93f5b7b7-fb9c-4c6d-92c6-443189f70b47", "345591e7-fd46-5613-9e17-170e4c5cab42", "46127eb1-34f4-57f5-9188-798b7d444b2c"),
        ("8e5e077f-44e9-4259-9f57-f2aa073cf14b", "7bf780a9-74b6-5a41-8d05-fee7ef35e161", "a0393048-ad07-52ef-8388-61b334c70baf"),
    ]:
        e = await conn.fetchrow("SELECT entry_type, timestamp, thread, user_ctx, source_file FROM log_entries WHERE id=$1::uuid", eid)
        print("ENTRY", eid[:8], e["entry_type"], e["timestamp"], "thread", e["thread"], e["user_ctx"], e["source_file"].split("/")[0])
        for label, tid in (("planned", planned), ("actual", actual)):
            t = await conn.fetchrow("SELECT started_at, ended_at, status, entry_count, sealed FROM log_transactions WHERE id=$1::uuid", tid)
            if t is None:
                print("  ", label, tid[:8], "-> DOES NOT EXIST in log_transactions")
            else:
                print("  ", label, tid[:8], "->", t["started_at"], "..", t["ended_at"], t["status"], "entries", t["entry_count"], "sealed", t["sealed"])
        if True:
            kinds = await conn.fetch("""SELECT e.entry_type, min(e.timestamp) lo, max(e.timestamp) hi, count(*) n
                FROM log_entry_assignment a JOIN log_entries e ON e.id=a.entry_id
                WHERE a.transaction_id=$1::uuid GROUP BY e.entry_type ORDER BY lo""", actual)
            for k in kinds:
                print("     actual txn member types:", k["entry_type"], k["n"], k["lo"], "->", k["hi"])
        print()
    await conn.close()
asyncio.run(main())
