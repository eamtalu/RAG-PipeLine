"""Phase 2 — Claude tool-use debugging agent over the M3 WMS relational log store.

The agent answers natural-language debugging questions ("why did user X's pick fail on
2026-05-19?", "how many transactions errored today?", "what happened in transaction <id>?")
by letting Claude choose and call read-only SQL-backed tools against log_transactions /
log_entries, then citing the transaction ids it used.
"""
