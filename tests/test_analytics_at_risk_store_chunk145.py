"""Chunk 145: writing the board rows, the acknowledgements and the outcomes, against the real tables.

The worker hands `delivery_store.apply` the states the board read produced and it keeps one row per
delivery per departure: the first flag is recorded with its tier, time and the van clock it was judged by;
every tier change is appended to the history; a moved departure updates the row in place. A person's
check is kept when the tier later rises, the row is counted re-opened, and the ledger says so. Rows
close after departure plus a grace with one of three outcomes, and a row left open for a day closes
as unknown. Settings and the tenant state row are one read each.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import (AnalyticsAtRiskCheck, AnalyticsAtRiskDelivery,
                                                      AnalyticsAtRiskSettings)
from app.services.analytics_at_risk import RULE_VERSION, check_store, delivery_store, model, settings_store, state_store
from tests import at_risk_fixtures as fx

CC = "test_chunk145ar"
LONDON = fx.LONDON
UTC = timezone.utc
DEP = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)  # 11:30 BST on 2 Oct
USUAL = DEP - timedelta(minutes=270)  # the BRI03 van is usually ready at 07:00 BST
CLOCK_FOR = fx.clock_before_departure(270)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _state(**over) -> model.DeliveryState:
    base = dict(delivery_number="29616", route="BRI03", customer_name="BOK SHOP HORSHAM", customer_number="10567",
                departure_at=DEP, lines_expected=5, lines_confirmed=0, lines_picked=0, lines_short=0,
                packages_created=0, packages_loaded=0, last_pick_at=None, last_load_at=None)
    base.update(over)
    if "lines_confirmed" not in over:  # the helper confirms what it picks unless a test says otherwise
        base["lines_confirmed"] = base["lines_picked"]
    return model.DeliveryState(**base)


async def _apply(states, now, clock_for=CLOCK_FOR) -> delivery_store.ApplyStats:
    async with async_session() as db:
        stats = await delivery_store.apply(db, CC, states, now=now, tz=LONDON, clock_for=clock_for, rule_version=RULE_VERSION)
        await db.commit()
    return stats


async def _row(delivery="29616") -> AnalyticsAtRiskDelivery | None:
    async with async_session() as db:
        return await db.scalar(select(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == CC, AnalyticsAtRiskDelivery.delivery_number == delivery
        ).order_by(AnalyticsAtRiskDelivery.departure_date.desc()))


async def _ledger(delivery="29616") -> list[AnalyticsAtRiskCheck]:
    async with async_session() as db:
        return list((await db.execute(select(AnalyticsAtRiskCheck).where(
            AnalyticsAtRiskCheck.customer_code == CC, AnalyticsAtRiskCheck.delivery_number == delivery
        ).order_by(AnalyticsAtRiskCheck.at))).scalars().all())


# ============================================================== flags

async def test_first_flag_records_tier_time_and_the_van_clock():
    calm = DEP - timedelta(hours=8)
    stats = await _apply([_state()], calm)
    row = await _row()
    assert (stats.evaluated, stats.flagged) == (1, 0)
    assert (row.tier, row.max_tier, row.first_flagged_at, row.tier_history) == ("none", "none", None, [])
    assert row.departure_date == date(2026, 10, 2) and row.departure_at == DEP
    assert (row.route, row.customer_name, row.customer_number) == ("BRI03", "BOK SHOP HORSHAM", "10567")

    in_window = USUAL - timedelta(minutes=20)  # picking open, its one package on the van, 20 min before the van is usually ready
    stats = await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], in_window)
    row = await _row()
    assert stats.flagged == 1
    assert (row.tier, row.max_tier, row.first_flagged_tier, row.first_flagged_at) == ("watch", "watch", "watch", in_window)
    assert row.tier_history == [{"tier": "watch", "at": in_window.isoformat(), "minutes_to_usual_ready": "20",
                                 "usual_ready_at": USUAL.isoformat(), "source": "learned"}]
    assert (row.usual_ready_at, row.usual_ready_source) == (USUAL, "learned")
    assert (row.lines_expected, row.lines_picked, row.status, row.rule_version) == (5, 2, "open", RULE_VERSION)
    assert row.last_evaluated_at == in_window


async def test_a_tier_change_appends_history_and_raises_max_tier_but_never_lowers_it():
    await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=20))
    stats = await _apply([_state(lines_picked=5, packages_created=2, packages_loaded=1)], USUAL - timedelta(minutes=10))
    row = await _row()
    assert (stats.flagged, stats.escalated) == (0, 1)
    assert (row.tier, row.max_tier) == ("at_risk", "at_risk")
    assert [h["tier"] for h in row.tier_history] == ["watch", "at_risk"]
    assert row.tier_history[-1]["minutes_to_usual_ready"] == "10"
    # everything loaded: the tier drops back to none but max_tier remembers
    await _apply([_state(lines_picked=5, packages_created=2, packages_loaded=2)], USUAL - timedelta(minutes=5))
    row = await _row()
    assert (row.tier, row.max_tier) == ("none", "at_risk")
    assert [h["tier"] for h in row.tier_history] == ["watch", "at_risk", "none"]
    # the same tier again appends nothing
    await _apply([_state(lines_picked=5, packages_created=2, packages_loaded=2)], USUAL)
    assert len((await _row()).tier_history) == 3


async def test_a_moved_departure_updates_the_open_row_in_place():
    await _apply([_state()], DEP - timedelta(hours=8))
    moved = DEP + timedelta(days=1)
    stats = await _apply([_state(departure_at=moved)], DEP - timedelta(hours=7))
    async with async_session() as db:
        n = await db.scalar(select(func.count()).select_from(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == CC))
    row = await _row()
    assert (n, stats.moved) == (1, 1)
    assert (row.departure_date, row.departure_at) == (date(2026, 10, 3), moved)


async def test_a_delivery_already_closed_for_that_departure_is_not_reopened_by_the_board():
    await _apply([_state(packages_created=1, packages_loaded=1, lines_picked=5)], DEP - timedelta(hours=4))
    async with async_session() as db:
        closed = await delivery_store.close_due(db, CC, {}, now=DEP + timedelta(hours=4), grace=timedelta(hours=3), tz=LONDON)
        await db.commit()
    assert closed == 1
    # the routing row still names it the next morning; nothing new is written
    stats = await _apply([_state(packages_created=1, packages_loaded=1, lines_picked=5)], DEP + timedelta(hours=5))
    async with async_session() as db:
        n = await db.scalar(select(func.count()).select_from(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == CC))
    assert (n, stats.evaluated) == (1, 0)
    assert (await _row()).status == "closed"


# ============================================================== checks

async def test_a_check_records_who_when_the_tier_and_a_note_and_writes_a_ledger_row():
    await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=20))  # watch
    at = USUAL - timedelta(minutes=15)
    async with async_session() as db:
        row = await check_store.check(db, CC, "29616", actor="amin", note="loader called", now=at)
        await db.commit()
    assert (row.checked_at, row.checked_by, row.check_note, row.checked_tier) == (at, "amin", "loader called", "watch")
    ledger = await _ledger()
    assert [(c.action, c.tier, c.actor, c.note, c.at) for c in ledger] == [("checked", "watch", "amin", "loader called", at)]
    assert ledger[0].delivery_id == row.id and ledger[0].departure_date == date(2026, 10, 2)


async def test_an_unknown_delivery_cannot_be_checked():
    with pytest.raises(check_store.NoOpenDelivery):
        async with async_session() as db:
            await check_store.check(db, CC, "nope", actor="amin", note=None, now=DEP)


async def test_escalation_past_the_checked_tier_reopens_the_row_and_keeps_the_check():
    await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=20))  # watch
    at = USUAL - timedelta(minutes=15)
    async with async_session() as db:
        await check_store.check(db, CC, "29616", actor="amin", note=None, now=at)
        await db.commit()
    # still watch: nothing re-opens
    await _apply([_state(lines_picked=3, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=12))
    assert (await _row()).reopened_count == 0
    # rises to at_risk: re-opened once, the check stays
    stats = await _apply([_state(lines_picked=5, packages_created=2, packages_loaded=1)], USUAL - timedelta(minutes=10))
    row = await _row()
    assert stats.reopened == 1
    assert (row.reopened_count, row.checked_by, row.checked_tier, row.tier) == (1, "amin", "watch", "at_risk")
    # stays at_risk next minute: not counted again
    await _apply([_state(lines_picked=5, packages_created=2, packages_loaded=1)], USUAL - timedelta(minutes=9))
    assert (await _row()).reopened_count == 1
    # the dock has been quiet past the usual time: left behind, a second re-open
    await _apply([_state(lines_picked=5, packages_created=2, packages_loaded=1)], USUAL + timedelta(minutes=21))
    row = await _row()
    assert (row.reopened_count, row.tier) == (2, "left_behind")
    assert [(c.action, c.tier, c.actor) for c in await _ledger()] == [
        ("checked", "watch", "amin"), ("reopened", "at_risk", "worker"), ("reopened", "left_behind", "worker")]


async def test_uncheck_clears_the_check_and_logs_it():
    await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=20))
    async with async_session() as db:
        await check_store.check(db, CC, "29616", actor="amin", note="x", now=USUAL - timedelta(minutes=15))
        row = await check_store.uncheck(db, CC, "29616", actor="mark", now=USUAL - timedelta(minutes=14))
        await db.commit()
    assert (row.checked_at, row.checked_by, row.check_note, row.checked_tier) == (None, None, None, None)
    assert [(c.action, c.actor) for c in await _ledger()] == [("checked", "amin"), ("unchecked", "mark")]


async def test_a_check_names_the_departure_when_two_rows_share_a_delivery_number():
    await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=20))
    async with async_session() as db:
        await delivery_store.close_due(db, CC, {}, now=DEP + timedelta(hours=4), grace=timedelta(hours=3), tz=LONDON)
        await db.commit()
    tomorrow = DEP + timedelta(days=1)
    await _apply([_state(departure_at=tomorrow, lines_picked=1, packages_created=1, packages_loaded=1)], tomorrow - timedelta(minutes=290))
    async with async_session() as db:
        row = await check_store.check(db, CC, "29616", actor="amin", note=None, now=tomorrow - timedelta(minutes=280))
        await db.commit()
    assert row.departure_date == date(2026, 10, 3)  # the open one
    async with async_session() as db:
        row = await check_store.check(db, CC, "29616", actor="amin", note=None, now=tomorrow,
                                      departure_date=date(2026, 10, 2))
        await db.commit()
    assert row.departure_date == date(2026, 10, 2) and row.status == "closed"


# ============================================================== closing

async def test_closing_writes_the_three_outcomes_and_the_lead_of_the_last_load():
    before = DEP - timedelta(hours=4)
    await _apply([
        _state(delivery_number="A", lines_picked=5, packages_created=2, packages_loaded=2, last_load_at=DEP - timedelta(hours=4),
               route_loaded_at=DEP - timedelta(minutes=10)),
        _state(delivery_number="B", lines_picked=5, packages_created=2, packages_loaded=2, last_load_at=DEP + timedelta(minutes=5)),
        _state(delivery_number="C", lines_picked=5, packages_created=2, packages_loaded=1, last_load_at=DEP - timedelta(hours=4)),
        _state(delivery_number="D", departure_at=DEP + timedelta(days=1)),  # tomorrow: untouched
    ], before)
    now = DEP + timedelta(hours=3)  # exactly the grace
    async with async_session() as db:
        # the latest state is passed in, so a load that landed after the last evaluation still counts
        states = {"B": _state(delivery_number="B", lines_picked=5, packages_created=2, packages_loaded=2,
                              last_load_at=DEP + timedelta(minutes=5))}
        closed = await delivery_store.close_due(db, CC, states, now=now, grace=timedelta(hours=3), tz=LONDON)
        await db.commit()
    assert closed == 3
    a, b, c, d = [await _row(x) for x in "ABCD"]
    assert (a.status, a.outcome, a.outcome_lead_min, a.closed_at, a.tier) == ("closed", "loaded_in_time", Decimal("240"), now, "none")
    assert a.route_loaded_at == DEP - timedelta(minutes=10)  # the van-ready moment of its route, kept beside the clock
    assert (a.usual_ready_at, a.usual_ready_source) == (USUAL, "learned")
    assert b.route_loaded_at is None
    assert (b.outcome, b.outcome_lead_min, b.tier, b.max_tier) == ("loaded_late", Decimal("-5"), "none", "none")
    assert (c.outcome, c.outcome_lead_min, c.tier, c.max_tier) == ("never_loaded", Decimal("240"), "left_behind", "left_behind")
    assert d.status == "open"


async def test_a_route_without_a_loading_step_closes_on_its_last_pick():
    before = DEP - timedelta(hours=4)
    await _apply([
        _state(delivery_number="G1", route="BRILAT", loading_expected=False, lines_picked=5, last_pick_at=DEP - timedelta(hours=2)),
        _state(delivery_number="G2", route="BRILAT", loading_expected=False, lines_picked=3, last_pick_at=DEP - timedelta(hours=2)),
    ], before)
    g1 = await _row("G1")
    assert (g1.loading_expected, g1.tier) == (False, "none")  # no package, no load, and that is not held against it
    async with async_session() as db:
        closed = await delivery_store.close_due(db, CC, {}, now=DEP + timedelta(hours=3), grace=timedelta(hours=3), tz=LONDON)
        await db.commit()
    g1, g2 = await _row("G1"), await _row("G2")
    assert (closed, g1.outcome, g1.outcome_lead_min, g1.tier) == (2, "picked_in_time", Decimal("120"), "none")
    assert (g2.outcome, g2.tier, g2.max_tier) == ("picked_late", "left_behind", "left_behind")


async def _history_set():
    day = date(2026, 9, 30)
    dep = fx.local_day(day, 11, 30)
    usual = dep - timedelta(minutes=270)  # the vans are usually ready at 07:00
    await fx.plant([
        fx.closed_delivery(CC, "fine1", route="BRI03", departure_at=dep, last_load_at=dep - timedelta(hours=5), outcome="loaded_in_time",
                           usual_ready_at=usual, transaction_names=("Brighton Stock Pick",), customer_name="BOK SHOP"),
        # on the van an hour after the van's usual time: it held the van, whatever the tiers said
        fx.closed_delivery(CC, "held1", route="BRI03", departure_at=dep, last_load_at=usual + timedelta(hours=1), outcome="loaded_in_time",
                           usual_ready_at=usual, max_tier="at_risk", first_flagged_at=usual - timedelta(minutes=20),
                           transaction_names=("Brighton Stock Pick", "JIT and Shorts Pick (Brighton)")),
        fx.closed_delivery(CC, "held2", route="BRI06", departure_at=dep, last_load_at=dep + timedelta(minutes=5), outcome="loaded_late",
                           usual_ready_at=usual, transaction_names=("JIT and Shorts Pick (Brighton)",)),
        fx.closed_delivery(CC, "missed1", route="BRI06", departure_at=dep, last_load_at=dep - timedelta(hours=3), outcome="never_loaded",
                           usual_ready_at=usual, max_tier="left_behind", transaction_names=("Milk Pick (Brighton)",), customer_name="HILTON"),
        fx.closed_delivery(CC, "missed2", route="BRILAT", departure_at=dep, last_load_at=None, outcome="picked_late", usual_ready_at=usual,
                           lines_expected=5, lines_picked=3, max_tier="left_behind", transaction_names=("JIT and Shorts Pick (Brighton)",)),
        fx.closed_delivery(CC, "held3", route="BRILAT", departure_at=dep, last_load_at=None, outcome="picked_late", usual_ready_at=usual,
                           lines_expected=5, lines_picked=5, max_tier="left_behind", transaction_names=("JIT and Shorts Pick (Brighton)",)),
        fx.closed_delivery(CC, "lost", route="BRI01", departure_at=dep, last_load_at=None, outcome="unknown", usual_ready_at=usual),
    ])
    return day


async def test_history_filters_on_category_picking_screen_and_delivery_number():
    day = await _history_set()
    async with async_session() as db:
        kw = dict(start=day, end=day, limit=50)
        everything, _ = await delivery_store.history_rows(db, CC, **kw)
        missed, _ = await delivery_store.history_rows(db, CC, categories=["missed"], **kw)
        default, _ = await delivery_store.history_rows(db, CC, categories=["missed", "held"], **kw)
        jit, _ = await delivery_store.history_rows(db, CC, transaction="JIT and Shorts Pick (Brighton)", **kw)
        jit_missed, _ = await delivery_store.history_rows(db, CC, transaction="JIT and Shorts Pick (Brighton)", categories=["missed"], **kw)
        by_number, _ = await delivery_store.history_rows(db, CC, delivery="miss", **kw)
    assert len(everything) == 7
    assert sorted(r.delivery_number for r in missed) == ["missed1", "missed2"]
    assert sorted(r.delivery_number for r in default) == ["held1", "held2", "held3", "missed1", "missed2"]
    # a delivery that spans two kinds of picking appears under both
    assert sorted(r.delivery_number for r in jit) == ["held1", "held2", "held3", "missed2"]
    assert [r.delivery_number for r in jit_missed] == ["missed2"]
    assert sorted(r.delivery_number for r in by_number) == ["missed1", "missed2"]


async def test_history_counts_feed_the_pie_and_the_picking_pills():
    day = await _history_set()
    async with async_session() as db:
        summary = await delivery_store.history_counts(db, CC, start=day, end=day)
        jit = await delivery_store.history_counts(db, CC, start=day, end=day, transaction="JIT and Shorts Pick (Brighton)")
        none = await delivery_store.history_counts(db, CC, start=day + timedelta(days=1), end=day + timedelta(days=1))
    assert summary["counts"] == {"missed": 2, "held": 3, "fine": 1, "unknown": 1}
    assert summary["transactions"] == ["Brighton Stock Pick", "JIT and Shorts Pick (Brighton)", "Milk Pick (Brighton)"]
    assert jit["counts"] == {"missed": 1, "held": 3, "fine": 0, "unknown": 0}
    assert none == {"counts": {"missed": 0, "held": 0, "fine": 0, "unknown": 0}, "transactions": []}


async def test_the_sweep_closes_a_day_old_open_row_as_unknown():
    await _apply([_state()], DEP - timedelta(hours=8))
    async with async_session() as db:
        assert await delivery_store.sweep(db, CC, now=DEP + timedelta(hours=23)) == 0
        swept = await delivery_store.sweep(db, CC, now=DEP + timedelta(hours=25))
        await db.commit()
    row = await _row()
    assert (swept, row.status, row.outcome) == (1, "closed", "unknown")


# ============================================================== settings and state

async def test_settings_default_without_a_row_and_persist_when_put():
    async with async_session() as db:
        s = await settings_store.effective(db, CC)
    assert (s.defaulted, s.warn_before_min, s.gone_after_min, s.min_days, s.window_days, s.close_grace_min,
            s.coverage, s.enabled) == (True, 30, 20, 5, 28, 180, Decimal("0.900"), True)
    async with async_session() as db:
        s = await settings_store.put(db, CC, warn_before_min=45, coverage=Decimal("0.95"), updated_by="amin")
        await db.commit()
    async with async_session() as db:
        again = await settings_store.effective(db, CC)
        n = await db.scalar(select(func.count()).select_from(AnalyticsAtRiskSettings).where(AnalyticsAtRiskSettings.customer_code == CC))
    assert (s.defaulted, again.defaulted, again.warn_before_min, again.gone_after_min, again.coverage, again.updated_by, n) == (
        False, False, 45, 20, Decimal("0.950"), "amin", 1)


async def test_tenant_state_is_one_row_upserted_in_place():
    t = DEP - timedelta(hours=8)
    async with async_session() as db:
        assert await state_store.get(db, CC) is None
        await state_store.touch(db, CC, last_evaluated_at=t, open_rows=3, last_error=None)
        await state_store.touch(db, CC, last_evaluated_at=t + timedelta(minutes=1), open_rows=4,
                                last_profiled_date=date(2026, 10, 1))
        await db.commit()
    async with async_session() as db:
        row = await state_store.get(db, CC)
    assert (row.last_evaluated_at, row.open_rows, row.last_profiled_date, row.last_error) == (
        t + timedelta(minutes=1), 4, date(2026, 10, 1), None)


async def test_the_purge_deletes_every_at_risk_table_for_a_tenant():
    from app.services.logspace_cleanup import purge_logspace
    await _apply([_state(lines_picked=2, packages_created=1, packages_loaded=1)], USUAL - timedelta(minutes=20))
    async with async_session() as db:
        await check_store.check(db, CC, "29616", actor="amin", note=None, now=DEP)
        await settings_store.put(db, CC, warn_before_min=45)
        await state_store.touch(db, CC, open_rows=1)
        await db.commit()
    async with async_session() as db:
        await purge_logspace(db, CC)
        await db.commit()
    async with async_session() as db:
        for m_ in fx.MODELS[:5]:
            assert await db.scalar(select(func.count()).select_from(m_).where(m_.customer_code == CC)) == 0, m_.__tablename__
