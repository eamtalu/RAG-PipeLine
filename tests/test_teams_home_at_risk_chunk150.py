"""Chunk 150: the deliveries-at-risk block of the Teams Home snapshot, and the check that rides a job.

The tab on the edge draws, never computes, so everything it shows about the board is decided here:
a summary line, the flagged deliveries worst first with every figure as text, a person's check with
their name, the re-open wording when a tier rises past it, and the accuracy line once enough has
closed. A `command` on a QuestionJob is recorded through the same store the web API uses, and the
agent never runs for it.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.services.analytics_at_risk import RULE_VERSION, check_store, delivery_store, model, state_store
from app.services.teams import commands, consumer as c, home_at_risk, home_snapshot
from app.services.teams.contracts import AnswerPayload, Command, QuestionJob
from tests import at_risk_fixtures as fx
from tests.test_teams_consumer_chunk123 import FakePoster, FakeSqs

CC = "test_chunk150ar"
LONDON = fx.LONDON
UTC = timezone.utc
DEP = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)  # 11:30 BST on 2 Oct
NOW = DEP - timedelta(minutes=90)  # 10:00 London
USUAL = DEP - timedelta(minutes=60)  # the van is usually ready at 10:30; NOW sits on the window edge
CLOCK_FOR = fx.clock_before_departure(60)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _state(number, **over) -> model.DeliveryState:
    base = dict(delivery_number=number, route="BRI03", customer_name="BOK SHOP HORSHAM", customer_number="10567",
                departure_at=DEP, lines_expected=5, lines_confirmed=5, lines_picked=5, lines_short=0,
                packages_created=2, packages_loaded=2, last_pick_at=DEP - timedelta(hours=4), last_load_at=DEP - timedelta(hours=3))
    base.update(over)
    if "lines_confirmed" not in over:  # the helper confirms what it picks unless a test says otherwise
        base["lines_confirmed"] = base["lines_picked"]
    return model.DeliveryState(**base)


async def _apply(states, now=NOW):
    async with async_session() as db:
        await delivery_store.apply(db, CC, states, now=now, tz=LONDON, clock_for=CLOCK_FOR, rule_version=RULE_VERSION)
        await state_store.touch(db, CC, last_evaluated_at=now, open_rows=len(states))
        await db.commit()


# ==================================================== 1. the block

async def test_without_rows_the_block_says_so():
    async with async_session() as db:
        block = await home_at_risk.compute(db, CC, NOW, LONDON)
    assert block == {"available": False, "note": home_at_risk.NO_BOARD_NOTE}


async def test_the_block_lists_flagged_deliveries_worst_first_as_text():
    await _apply([
        _state("fine"),
        _state("watch", lines_picked=2, packages_created=1, packages_loaded=1),
        _state("risk", packages_loaded=1, customer_name="Tesco Hove"),
        _state("behind", departure_at=DEP - timedelta(days=1), packages_loaded=1),
    ])
    async with async_session() as db:
        block = await home_at_risk.compute(db, CC, NOW, LONDON)
    assert block["available"] is True and block["stale"] is False and block["note"] == ""
    assert block["as_of_text"] == "as of 10:00"
    assert block["caption"] == ("departures today and tomorrow · each route's van has a learned ready time · "
                                "warned 30 min before it · left behind 20 min of quiet after it")
    assert block["summary"] == {"open": 4, "left_behind": 1, "at_risk": 1, "watch": 1, "checked": 0,
                                "text": "1 left behind · 1 at risk · 1 to watch · 1 fine"}
    assert block["empty_text"] is None and block["more_text"] == ""
    # only the flagged ones: the fine delivery is counted in the summary, never listed
    assert [d["delivery_number"] for d in block["deliveries"]] == ["behind", "risk", "watch"]
    risk = block["deliveries"][1]
    assert (risk["tier"], risk["tier_text"], risk["route"], risk["customer_name"]) == ("at_risk", "At risk", "BRI03", "Tesco Hove")
    assert (risk["departure_text"], risk["minutes_to_departure"], risk["minutes_text"]) == ("van usually ready 10:30", 30, "van usually ready in 30 min")
    assert risk["progress_text"] == "5 of 5 lines · 1 of 2 packages loaded"
    assert risk["last_text"] == "last pick 07:30 · last load 08:30"
    # the web board's cells, as text
    assert (risk["lines_text"], risk["packages_text"]) == ("5 / 5", "1 / 2")
    assert (risk["clock_text"], risk["clock_source"]) == ("van usually ready 10:30", "learned")
    assert risk["threshold_text"] == "packages still off the van · van usually ready by 10:30 · learned from this route's days"
    assert risk["checked"] is None and risk["reopened_text"] == ""
    watch = block["deliveries"][2]
    assert watch["threshold_text"] == "picking not finished · van usually ready by 10:30 · learned from this route's days"
    assert watch["progress_text"] == "2 of 5 lines · 1 of 1 packages loaded"
    assert watch["lines_text"] == "2 / 5" and watch["packages_text"] == "1 / 1"
    behind = block["deliveries"][0]
    assert behind["minutes_text"].startswith("usual time passed ") and behind["tier_text"] == "Left behind"
    assert behind["threshold_text"].startswith("the van is taken as gone and this delivery is not on it")
    assert block["accuracy_text"].startswith("accuracy · not enough closed departures yet · 0 of 20 needed")


async def test_a_check_shows_the_persons_name_and_reopens_in_words_when_the_tier_rises():
    await _apply([_state("watch", lines_picked=2, packages_created=1, packages_loaded=1)])
    async with async_session() as db:
        await check_store.check(db, CC, "watch", actor="Amin Talukder", note="loader called", now=NOW + timedelta(minutes=1))
        await db.commit()
        block = await home_at_risk.compute(db, CC, NOW + timedelta(minutes=2), LONDON)
    row = block["deliveries"][0]
    assert row["checked"] == {"by": "Amin Talukder", "at_text": "10:01", "note": "loader called"}
    assert row["reopened_text"] == "" and block["summary"]["checked"] == 1
    await _apply([_state("watch", lines_picked=5, packages_created=2, packages_loaded=1)], now=DEP - timedelta(minutes=50))  # at risk
    async with async_session() as db:
        block = await home_at_risk.compute(db, CC, DEP - timedelta(minutes=49), LONDON)
    row = block["deliveries"][0]
    assert row["tier"] == "at_risk" and row["checked"]["by"] == "Amin Talukder" and row["reopened_count"] == 1
    assert row["reopened_text"] == "checked at Watch · now At risk"


async def test_a_stale_board_says_so_and_a_long_list_is_cut_with_a_count(monkeypatch):
    monkeypatch.setattr(home_at_risk, "MAX_ROWS", 12)
    # fifteen vans usually ready 20 minutes after this pass, each with a package still off: all at risk
    await _apply([_state(f"r{i:02d}", departure_at=DEP - timedelta(minutes=20), packages_loaded=1) for i in range(15)], now=NOW - timedelta(minutes=10))
    async with async_session() as db:
        block = await home_at_risk.compute(db, CC, NOW, LONDON)
    assert block["stale"] is True and block["note"] == home_at_risk.STALE_NOTE
    assert len(block["deliveries"]) == 12 and block["more_text"] == "and 3 more"


async def test_a_board_with_nothing_flagged_lists_nothing_and_says_so():
    await _apply([_state("a"), _state("b"), _state("c", lines_expected=None, lines_short=2, packages_loaded=3)])
    async with async_session() as db:
        block = await home_at_risk.compute(db, CC, NOW, LONDON)
    assert block["deliveries"] == [] and block["summary"]["text"] == "3 fine"
    assert block["empty_text"] == "nothing to watch right now · 3 deliveries open, all on course"
    # a lookup gap and a short line read the way the web board prints them
    flagged = home_at_risk.build_rows([r for r in await delivery_store.board_rows(db, CC, dates=[DEP.date()])], NOW, LONDON)
    c = next(r for r in flagged if r["delivery_number"] == "c")
    assert (c["lines_text"], c["packages_text"]) == ("5 / ? · 2 short", "3 / 2")


async def test_an_empty_board_after_the_vans_have_gone_names_the_day_and_the_next_departure():
    await _apply([_state("a"), _state("b"), _state("c", packages_loaded=1), _state("tomorrow", departure_at=DEP + timedelta(days=1))],
                 now=DEP - timedelta(hours=4))
    async with async_session() as db:
        await delivery_store.close_due(db, CC, {}, now=DEP + timedelta(hours=3), grace=timedelta(hours=3), tz=LONDON)
        # tomorrow's delivery is loaded already, so nothing is flagged and nothing is listed, but it is open: the words say so
        block = await home_at_risk.compute(db, CC, DEP + timedelta(hours=3), LONDON)
        assert block["deliveries"] == [] and block["empty_text"] == "nothing to watch right now · 1 delivery open, all on course"
        await db.execute(AnalyticsAtRiskDelivery.__table__.delete().where(AnalyticsAtRiskDelivery.delivery_number == "tomorrow"))
        await db.commit()
        block = await home_at_risk.compute(db, CC, DEP + timedelta(hours=3), LONDON)
    assert block["deliveries"] == [] and block["summary"]["text"] == "0 fine"
    assert block["empty_text"] == "3 departures today · 1 left behind · 2 went out"


async def test_the_accuracy_line_appears_once_enough_departures_have_closed():
    day = date(2026, 9, 30)
    rows = []
    for i in range(24):
        flagged, late = i < 6, i < 4 or i == 23
        rows.append(fx.closed_delivery(CC, f"a{i}", route="BRI03", departure_at=fx.local_day(day, 11, 30),
                                       last_load_at=fx.local_day(day, 12, 0) if late else fx.local_day(day, 7, 0),
                                       outcome="loaded_late" if late else "loaded_in_time",
                                       max_tier="at_risk" if flagged else "none", first_flagged_at=fx.local_day(day, 9, 0) if flagged else None))
    await fx.plant(rows)
    async with async_session() as db:
        await state_store.touch(db, CC, last_evaluated_at=NOW)
        await db.commit()
        block = await home_at_risk.compute(db, CC, NOW, LONDON)
    assert block["accuracy_text"] == "last 14 days · flagged 6 · actually left behind 5 · precision 67% · recall 80%"


async def test_the_snapshot_carries_the_block_and_survives_its_failure(monkeypatch):
    # the snapshot reads the board on the real clock, so the delivery departs today, whatever day the test runs
    today_dep = datetime.combine(datetime.now(LONDON).date(), datetime.min.time(), tzinfo=LONDON) + timedelta(hours=11, minutes=30)
    await _apply([_state("risk", departure_at=today_dep, packages_loaded=1)], now=today_dep - timedelta(minutes=90))
    async with async_session() as db:
        snap = await home_snapshot.compute(db, CC)
    assert snap is not None and snap["at_risk"]["available"] is True
    assert [d["delivery_number"] for d in snap["at_risk"]["deliveries"]] == ["risk"]

    async def boom(*a, **k):
        raise RuntimeError("no board")
    monkeypatch.setattr(home_at_risk, "compute", boom)
    async with async_session() as db:
        snap = await home_snapshot.compute(db, CC)
    assert snap["at_risk"] == {"available": False, "note": "at-risk board unavailable right now"}
    assert snap["kpis"]  # the rest of the snapshot is intact


# ==================================================== 2. the command on a job

def _job(**command) -> str:
    base = dict(kind="at_risk_check", delivery_number="risk", by="Amin Talukder")
    base.update(command)
    return QuestionJob(job_id="job-9", tenant_id="t", customer_code=CC, conversation_id="conv-9", question="mark checked",
                       sender_name="Amin Talukder", source="tab", command=Command(**base)).model_dump_json()


class _Harness:
    def __init__(self, bodies, *, run_command):
        self.sqs = FakeSqs(bodies)
        self.poster = FakePoster()
        self.agent_calls = []
        self.recorded = []

        async def run_agent(code, question, history):
            self.agent_calls.append(question)
            return {"answer": "should not run"}

        async def load_history(conv, code):
            return []

        async def record(*a):
            self.recorded.append(a)

        async def ready(code):
            return True

        self.consumer = c.TeamsQuestionConsumer(sqs=self.sqs, queue_url="q", poster=self.poster, run_agent=run_agent,
                                                load_history=load_history, record_exchange=record, customer_ready=ready,
                                                concurrency=1, visibility_seconds=30, wait_seconds=0, run_command=run_command)

    async def drain(self):
        while self.sqs.pending:
            await self.consumer.poll_once()
        import asyncio
        while self.consumer.in_flight:
            await asyncio.sleep(0.01)


async def test_a_command_job_records_the_check_and_never_runs_the_agent():
    await _apply([_state("risk", packages_loaded=1)])
    h = _Harness([_job(note="loader called")], run_command=commands.run_command)
    await h.drain()
    [p] = h.poster.posted
    assert (p.status, p.answer) == ("ok", "checked · delivery risk · Amin Talukder")
    assert h.agent_calls == [] and h.recorded == [] and h.sqs.deleted == ["r0"]
    async with async_session() as db:
        row = await db.scalar(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))
    assert (row.checked_by, row.check_note, row.checked_tier) == ("Amin Talukder", "loader called", "at_risk")

    h = _Harness([_job(kind="at_risk_uncheck")], run_command=commands.run_command)
    await h.drain()
    [p] = h.poster.posted
    assert p.answer == "check removed · delivery risk · Amin Talukder"
    async with async_session() as db:
        row = await db.scalar(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))
    assert row.checked_by is None


async def test_a_refused_command_is_a_short_error_sentence_not_a_crash():
    h = _Harness([_job(delivery_number="nope")], run_command=commands.run_command)
    await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer.startswith("Could not record that: no open delivery nope")
    assert h.sqs.deleted == ["r0"]


async def test_a_consumer_without_a_command_runner_declines_politely():
    h = _Harness([_job()], run_command=None)
    await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer == c.COMMAND_UNSUPPORTED and h.agent_calls == []
