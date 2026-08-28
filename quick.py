import asyncio, re
async def main():
    import asyncpg
    from dotenv import dotenv_values
    env = dotenv_values("/opt/RAG-Pipeline/RAG-PipeLine/.env")
    url = re.sub(r"\+asyncpg", "", env.get("DATABASE_URL") or env.get("database_url"))
    conn = await asyncpg.connect(url)
    print("newest_entry:", await conn.fetchval("SELECT max(timestamp) FROM log_entries"))
    print("entries_last_15m:", await conn.fetchval("SELECT count(*) FROM log_entries WHERE created_at > now() - interval '15 minutes'"))
    await conn.close()
asyncio.run(main())
