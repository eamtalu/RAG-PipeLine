"""Chunk 26 (step 5 of docs/plan/2026-08-05_20-32_daily-partitioning.md): close the timezone dedup
hole.

`log_entries.timestamp` is derived at parse time by attaching the CUSTOMER's configured timezone to
the naive local wall-clock the parser yields (`parse_insert.py`). Change that configuration and
re-ingest the same file, and the same raw line becomes a different UTC instant.

Before partitioning that was merely wrong-ish. Now the dedup key is
`(customer_code, entry_hash, timestamp)` — `timestamp` had to join it because a unique constraint on a
partitioned table must contain every partition column — so a shifted instant no longer collides.
Verified against the real database rather than reasoned about:

    identical hash, SAME tz     -> inserted 0   (dedup works)
    identical hash, CHANGED tz  -> inserted 1   (the hole)

and the tenant ends up with two rows for one log line, in two different partitions.

The fix is to stop the change rather than to detect the duplicates afterwards, because a tz change is
already destructive independently of partitioning: instants written BEFORE the change keep their old
derivation and instants written after get the new one, so the tenant's timeline becomes silently
inconsistent with no record of where the seam is.

Two things this deliberately does NOT block, because blocking them would be noise:

- setting a timezone on a tenant that has no entries yet — the normal case, right after creation;
- a change that does not move the EFFECTIVE zone, e.g. filling in `null -> "Europe/London"` when the
  global default already was `Europe/London`. Nothing about the stored data changes.

And it is a locked door with a key, not a wall: an operator who has accepted the consequence can pass
`allow_mixed_timezones=true`. That path is logged CRITICAL, because afterwards nothing in the data
itself records that the seam exists.
"""

import uuid

import pytest
from sqlalchemy import delete, select, text

from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.services import timezone_change_guard as guard
from app.settings import settings

CC = "test_chunk26"   # lowercase: normalize_customer_code() canonicalises, so an uppercase
                      # fixture row would never match the code the endpoint looks up


async def _cleanup(db):
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def _entry(db):
    j = Job(customer_code=CC, filename="c26.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c26.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    db.add(LogEntry(customer_code=CC, job_id=j.id, timestamp=None, source_file="c26.log",
                    line_number=1, level="INFO", raw_body="x", entry_hash=uuid.uuid4().hex))
    await db.flush()


# ==================================================== does the change move the meaning? (pure)
def test_a_different_zone_changes_the_meaning():
    assert guard.changes_meaning("Europe/London", "Europe/Berlin") is True


def test_the_same_zone_is_not_a_change():
    assert guard.changes_meaning("Europe/London", "Europe/London") is False


def test_filling_in_an_unset_zone_that_matches_the_default_is_not_a_change():
    """`null` already MEANT the global default, so writing that same name down changes nothing about
    how any instant was or will be derived. Blocking it would make the normal 'formalise the config'
    action fail for no reason."""
    assert guard.changes_meaning(None, settings.display_timezone) is False


def test_filling_in_an_unset_zone_with_a_different_one_does_change_the_meaning():
    """The dangerous version of the same action: entries already exist, derived with the DEFAULT zone,
    and every future entry would use a different one."""
    other = "Asia/Tokyo" if settings.display_timezone != "Asia/Tokyo" else "Europe/London"
    assert guard.changes_meaning(None, other) is True


def test_clearing_a_zone_back_to_unset_is_judged_against_the_default():
    other = "Asia/Tokyo" if settings.display_timezone != "Asia/Tokyo" else "Europe/London"
    assert guard.changes_meaning(other, None) is True
    assert guard.changes_meaning(settings.display_timezone, None) is False


# ==================================================== does the tenant have data at risk?
async def test_a_tenant_with_no_entries_is_free_to_change(db):
    """The common case — setting the zone right after creating the log space."""
    await _cleanup(db)
    assert await guard.blocking_reason(
        db, customer_code=CC, stored_tz="Europe/London", new_tz="Europe/Berlin") is None


async def test_a_tenant_with_entries_is_blocked(db):
    await _cleanup(db)
    await _entry(db)
    reason = await guard.blocking_reason(
        db, customer_code=CC, stored_tz="Europe/London", new_tz="Europe/Berlin")
    assert reason is not None


async def test_a_tenant_with_entries_may_still_make_a_no_op_change(db):
    """Having data must not freeze the field against a change that means nothing."""
    await _cleanup(db)
    await _entry(db)
    assert await guard.blocking_reason(
        db, customer_code=CC, stored_tz="Europe/London", new_tz="Europe/London") is None


async def test_the_reason_tells_the_operator_how_to_proceed(db):
    """A block that does not say what to do instead is just an obstacle. It has to name both the safe
    route (purge, then set, then re-ingest) and the override."""
    await _cleanup(db)
    await _entry(db)
    reason = await guard.blocking_reason(
        db, customer_code=CC, stored_tz="Europe/London", new_tz="Europe/Berlin")
    assert "Europe/London" in reason and "Europe/Berlin" in reason
    assert "allow_mixed_timezones" in reason
    assert "/logs/data" in reason, "the safe remedy must be named, not just the override"


async def test_the_entry_check_does_not_count_the_whole_table(db):
    """It runs on an admin path over a partitioned table, so it must short-circuit on the first row
    rather than count every entry the tenant has."""
    plan = "\n".join(r[0] for r in (await db.execute(
        text("EXPLAIN " + str(guard.has_entries_stmt(CC).compile(
            db.bind, compile_kwargs={"literal_binds": True}))))).all())
    assert "Limit" in plan, plan
    assert "Aggregate" not in plan, f"must not be a COUNT:\n{plan}"


# ==================================================== the endpoint
@pytest.fixture
async def customer(db):
    await db.execute(delete(Customer).where(Customer.customer_code == CC))
    c = Customer(customer_code=CC, name="chunk26", timezone="Europe/London", active=True)
    db.add(c)
    await db.flush()
    yield c
    await _cleanup(db)
    await db.execute(delete(Customer).where(Customer.customer_code == CC))
    await db.flush()


async def test_patch_rejects_the_change_with_409(db, customer):
    """409 rather than 400: the request is well-formed, it conflicts with the tenant's current state."""
    await _entry(db)
    from app.api.v1.customers import update_customer, UpdateCustomerRequest
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        await update_customer(CC, UpdateCustomerRequest(timezone="Europe/Berlin"),
                              allow_mixed_timezones=False, db=db,
                              repo=_repo(db), presence_repo=_presence(db))
    assert e.value.status_code == 409
    assert "allow_mixed_timezones" in e.value.detail


async def test_patch_allows_the_change_with_the_override(db, customer, caplog):
    """The escape hatch has to actually work, and has to leave a CRITICAL trail — after this, nothing
    in the data itself records where the derivation changed."""
    await _entry(db)
    from app.api.v1.customers import update_customer, UpdateCustomerRequest
    with caplog.at_level("CRITICAL"):
        await update_customer(CC, UpdateCustomerRequest(timezone="Europe/Berlin"),
                              allow_mixed_timezones=True, db=db,
                              repo=_repo(db), presence_repo=_presence(db))
    stored = await db.scalar(select(Customer.timezone).where(Customer.customer_code == CC))
    assert stored == "Europe/Berlin"
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


async def test_patch_still_allows_a_timezone_on_an_empty_tenant(db, customer):
    """The guard must not break the normal setup flow."""
    from app.api.v1.customers import update_customer, UpdateCustomerRequest
    await update_customer(CC, UpdateCustomerRequest(timezone="Europe/Berlin"),
                          allow_mixed_timezones=False, db=db,
                          repo=_repo(db), presence_repo=_presence(db))
    stored = await db.scalar(select(Customer.timezone).where(Customer.customer_code == CC))
    assert stored == "Europe/Berlin"


async def test_patch_leaves_other_fields_alone(db, customer):
    """Updating `active` on a tenant WITH entries must not trip a timezone guard it never asked for."""
    await _entry(db)
    from app.api.v1.customers import update_customer, UpdateCustomerRequest
    await update_customer(CC, UpdateCustomerRequest(active=False),
                          allow_mixed_timezones=False, db=db,
                          repo=_repo(db), presence_repo=_presence(db))
    assert await db.scalar(select(Customer.active).where(Customer.customer_code == CC)) is False


def _repo(db):
    from app.persistence.repositories.customer_repository import CustomerRepository
    return CustomerRepository(db)


def _presence(db):
    from app.persistence.repositories.logspace_presence_repository import LogspacePresenceRepository
    return LogspacePresenceRepository(db)


def test_no_path_writes_a_timezone_around_the_guard():
    """The guard is only worth anything if every mutation route goes through it. `repo.set_timezone`
    must have exactly ONE caller — a future endpoint calling it directly would silently reopen the
    hole with nothing failing."""
    import ast
    import pathlib as _p
    repo = _p.Path(__file__).resolve().parent.parent
    callers = []
    for path in (repo / "app").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "set_timezone"):
                callers.append(f"{path.relative_to(repo)}:{node.lineno}")
    assert len(callers) == 1, f"set_timezone must only be called by the guarded path, got {callers}"
    assert callers[0].startswith("app/api/v1/customers.py"), callers


# ==================================================== the hole itself stays closed
async def test_the_dedup_key_still_contains_the_timezone_derived_column():
    """The guard exists because `timestamp` is in the dedup key. If that ever stops being true the
    guard is solving a problem that no longer exists — and, more importantly, something has gone wrong
    with the partitioned unique constraint."""
    from app.services.mnp_log_ingestion.pipeline.parse_insert import _insert_dedup
    import inspect
    src = inspect.getsource(_insert_dedup)
    assert '"timestamp"' in src or "'timestamp'" in src
