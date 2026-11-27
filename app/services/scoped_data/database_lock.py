"""The second lock: what the database itself lets the agent's reader user see.

The scoped session (`session.py`) limits a read in the application. This module describes the
same boundary inside PostgreSQL, for one user, `rag_agent`: it may only select, only from the
tables in the registry, and only the rows of the customer its transaction names. The site limit
inside a customer stays with the application rule (owner's decision, 27 Nov 2026): the database
lock is the backup against the worst leak, one customer reading another.

The statements are generated from the registry, so the two cannot drift; a migration runs them
and a test compares the database with the registry. The application's own user owns the tables
and PostgreSQL applies no row rule to an owner, so ingestion, the web API and every write are
untouched. Used by the migrations, by `session.py` and by the tests.
"""

from __future__ import annotations

from app.services.scoped_data import registry

#: The database user the agent's data tools read as.
ROLE = "rag_agent"
#: The row policy every registry table carries.
POLICY = "agent_reader"
#: The transaction setting that names the customer being read for.
READER_CUSTOMER = "app.reader_customer"

_NAMED = f"current_setting('{READER_CUSTOMER}', true)"


def _visible_rows(rule: registry.Rule) -> str:
    """The SQL condition for the rows of one table the reader may see.

    (a table with a customer column) -> customer_code = the named customer
    ; (a child table) -> its owner is the named customer's ; (a shared list) -> true
    """
    if isinstance(rule, registry.Shared):
        return "true"
    if isinstance(rule, registry.OwnedBy):
        owner = rule.owner.__tablename__
        return f"{rule.owner_key} IN (SELECT id FROM {owner} WHERE customer_code = {_NAMED})"
    return f"customer_code = {_NAMED}"


def tables() -> list[str]:
    """Every table the reader may select from, in a stable order."""
    return sorted(registry.BY_TABLE)


def policy_statements() -> list[str]:
    """Switch row security on for every registry table and give it the reader policy.

    Safe to run again: the policy is dropped first. With no customer named the setting is NULL,
    no row matches, and the reader gets nothing.
    """
    statements = []
    for table in tables():
        statements += [
            f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
            f"DROP POLICY IF EXISTS {POLICY} ON {table}",
            f"CREATE POLICY {POLICY} ON {table} FOR SELECT "
            f"USING ({_visible_rows(registry.BY_TABLE[table])})",
        ]
    return statements


def grant_statements() -> list[str]:
    """Let the reader select from the registry tables and from nothing else.

    Rights are taken away first, so a table that left the registry stops being readable. Only
    the parent of a partitioned table is granted: a partition read directly would skip the
    parent's row policy.
    """
    return [f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {ROLE}",
            *(f"GRANT SELECT ON {table} TO {ROLE}" for table in tables())]


def removal_statements() -> list[str]:
    """Take the policies and the row security away again, for a downgrade."""
    statements = []
    for table in tables():
        statements += [f"DROP POLICY IF EXISTS {POLICY} ON {table}",
                       f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY"]
    return statements
