"""Chunk 131: a Teams alert opens its transaction in the log explorer, and never carries secrets.

The card's button used to open `/transactions/<id>`. A reader coming from Teams has no logspace in
that tab, so the picker showed, and choosing a logspace then sent them to the home feed: the
transaction was lost. The button now opens the explorer filtered by the transaction's request id on
its day (`/?date=<day>&reqid=<id>`), exactly as if the reader had typed the request id into the
filter, and the explorer keeps that address through the picker. A transaction without a request id
(alerts from before the 29 Sep log format, a manual alert on an id-less line) keeps the detail link.

The error text of a failed M3 call is the full request URL, and on tmp-live two alerts posted the
encrypted M3 credentials (`M3Credentials=%7B…Password…UserName…%7D`) to the channel. Every text that
reaches a card goes through `redact_secrets`, and alerts are stored already redacted.
"""

import uuid
from datetime import date as date_type, datetime, timezone

import pytest

from app.api.v1.logs import view_transactions
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.notifications.channels.teams import TeamsChannel
from app.services.notifications.events import NotificationEvent
from app.services.notifications.links import link_keys, transaction_url
from app.services.notifications.redact import REDACTED, redact_secrets
from app.services.notifications.rules.evaluators import transaction_payload
from app.settings import settings

BASE = "http://192.168.0.142:3000"
REQID = "13-2026-09-30_14:45:10.141-7116"
TXN_ID = "3223610d-7016-58c9-b9f0-16eaf55f4447"

# The shape seen on live, with made-up values: the credentials blob is URL-encoded JSON inside one
# query parameter, followed by ordinary parameters that must survive.
LEAKED = (
    "Error requesting http://172.17.0.230/api/picking/GetNextDeliveryByRoute?Company=915"
    "&Warehouse=BRI&User=PEVANS&ReqId=4-2026-09-30_06%3A08%3A52.065-1724"
    "&M3Credentials=%7B%22Domain%22%3A%22TMPDOM%22%2C%22Password%22%3A%22A1B2C3D4E5F6%22"
    "%2C%22UserName%22%3A%22FFEE0011AABB%22%7D&ApiPort=443&Division=TMP"
)


def _event(payload: dict, **kw) -> NotificationEvent:
    return NotificationEvent(event_type=kw.get("event_type", "transaction_error"),
                             customer_code="tmp-live", title=kw.get("title", "[tmp-live] error: X"),
                             summary=kw.get("summary"), dedup_key=f"t:{uuid.uuid4()}", payload=payload)


def _action_url(card: dict) -> str | None:
    actions = card["attachments"][0]["content"].get("actions") or []
    return actions[0]["url"] if actions else None


def _txn(**kw) -> LogTransaction:
    base = dict(id=uuid.UUID(TXN_ID), customer_code="tmp-live", date=date_type(2026, 9, 30),
                started_at=datetime(2026, 9, 30, 5, 1, 57, tzinfo=timezone.utc),
                status=LogTransactionStatus.error, method="GetNextDeliveryByRoute",
                user_name="PEVANS", warehouse="BRI", reqid=REQID, duration_ms=30, error_text=LEAKED)
    base.update(kw)
    return LogTransaction(**base)


# ---- the link ------------------------------------------------------------------------------------
def test_a_transaction_with_a_request_id_opens_the_explorer_filtered_by_it():
    url = transaction_url(BASE, {"transaction_id": TXN_ID, "reqid": REQID, "date": "2026-09-30"})
    assert url == f"{BASE}/?date=2026-09-30&reqid=13-2026-09-30_14%3A45%3A10.141-7116"


def test_without_a_request_id_the_link_falls_back_to_the_transaction_page():
    assert transaction_url(BASE, {"transaction_id": TXN_ID}) == f"{BASE}/transactions/{TXN_ID}"
    # a request id without its day cannot drive the day-scoped feed either
    assert transaction_url(BASE, {"transaction_id": TXN_ID, "reqid": REQID}) == \
        f"{BASE}/transactions/{TXN_ID}"


def test_no_base_or_nothing_to_open_means_no_link():
    assert transaction_url("", {"transaction_id": TXN_ID, "reqid": REQID, "date": "2026-09-30"}) is None
    assert transaction_url(BASE, {}) is None
    assert transaction_url(BASE, {"reqid": REQID}) is None


def test_a_trailing_slash_on_the_base_is_not_doubled():
    assert transaction_url(BASE + "/", {"transaction_id": TXN_ID}) == f"{BASE}/transactions/{TXN_ID}"


def test_link_keys_carry_the_request_id_and_the_tenant_day():
    assert link_keys(_txn()) == {"reqid": REQID, "date": "2026-09-30"}
    assert link_keys(_txn(reqid=None)) == {"date": "2026-09-30"}
    assert link_keys(_txn(reqid="  ", date=None)) == {}


def test_the_card_button_opens_the_explorer(monkeypatch):
    monkeypatch.setattr(settings, "app_public_base_url", BASE)
    card = TeamsChannel().build_card(_event(transaction_payload(_txn())))
    assert _action_url(card) == f"{BASE}/?date=2026-09-30&reqid=13-2026-09-30_14%3A45%3A10.141-7116"
    action = card["attachments"][0]["content"]["actions"][0]
    assert action["type"] == "Action.OpenUrl" and action["title"] == "Open in eSmart Eye"


def test_an_old_stored_alert_without_link_keys_still_gets_the_detail_link(monkeypatch):
    monkeypatch.setattr(settings, "app_public_base_url", BASE)
    card = TeamsChannel().build_card(_event({"transaction_id": TXN_ID, "facts": {"Status": "error"}}))
    assert _action_url(card) == f"{BASE}/transactions/{TXN_ID}"


def test_an_explicit_url_in_the_payload_still_wins(monkeypatch):
    monkeypatch.setattr(settings, "app_public_base_url", BASE)
    card = TeamsChannel().build_card(_event({"url": "https://x.example/a?b=1", "reqid": REQID,
                                             "date": "2026-09-30", "transaction_id": TXN_ID}))
    assert _action_url(card) == "https://x.example/a?b=1"


def test_no_base_url_means_no_button(monkeypatch):
    monkeypatch.setattr(settings, "app_public_base_url", "")
    card = TeamsChannel().build_card(_event(transaction_payload(_txn())))
    assert "actions" not in card["attachments"][0]["content"]


# ---- the facts -----------------------------------------------------------------------------------
def test_request_id_is_the_first_fact_and_named_in_full():
    facts = transaction_payload(_txn())["facts"]
    assert list(facts)[0] == "Request ID"
    assert facts["Request ID"] == REQID
    assert "Req ID" not in facts
    assert list(facts)[1:4] == ["Customer", "Status", "Method"]


def test_a_transaction_without_a_request_id_has_no_empty_row():
    assert "Request ID" not in transaction_payload(_txn(reqid=None))["facts"]


# ---- redaction -----------------------------------------------------------------------------------
def test_the_leaked_credentials_shape_is_redacted_and_the_rest_kept():
    out = redact_secrets(LEAKED)
    assert "A1B2C3D4E5F6" not in out and "FFEE0011AABB" not in out and "TMPDOM" not in out
    assert f"M3Credentials={REDACTED}&ApiPort=443&Division=TMP" in out
    assert out.startswith("Error requesting http://172.17.0.230/api/picking/GetNextDeliveryByRoute"
                          "?Company=915&Warehouse=BRI&User=PEVANS&ReqId=4-2026-09-30_06%3A08%3A52.065-1724&")


@pytest.mark.parametrize("raw, secret", [
    ("GET /x?password=hunter2&user=A", "hunter2"),
    ("GET /x?Password=hunter2", "hunter2"),
    ("GET /x?access_token=abc.def&ok=1", "abc.def"),
    ("GET /x?AccessToken=abc&ok=1", "abc"),
    ("GET /x?client_secret=s3&ok=1", "s3"),
    ("GET /x?api_key=k1&ok=1", "k1"),
    ("GET /x?apikey=k1", "k1"),
    ('body {"UserName": "BOB", "Password": "hunter2"}', "hunter2"),
    ('body {"M3Credentials": {"Domain": "D1", "Password": "P1", "UserName": "U1"}, "Company": 915}', "U1"),
    ("body %22Password%22%3A%22hunter2%22%2C%22x%22", "hunter2"),
    ("Authorization: Bearer eyJhbGciOi.payload.sig", "eyJhbGciOi.payload.sig"),
])
def test_known_secret_shapes_are_redacted(raw, secret):
    out = redact_secrets(raw)
    assert secret not in out
    assert REDACTED in out


def test_the_redacted_json_object_keeps_the_surrounding_fields():
    out = redact_secrets('{"M3Credentials": {"Domain": "D1", "Password": "P1"}, "Company": 915}')
    assert out == f'{{"M3Credentials": "{REDACTED}", "Company": 915}}'


@pytest.mark.parametrize("text", [
    "Error requesting http://172.17.0.230/api/picking/ConfirmPickLine?Company=915&User=PEVANS&ReqId=13-x",
    "Timeout after 30000 ms calling MMS060MI/LstBalID",
    "Item 12345 not found in location A1-01-02",
    '{"User": "PEVANS", "Warehouse": "BRI"}',
    "",
])
def test_text_without_secrets_comes_back_identical(text):
    assert redact_secrets(text) == text


def test_none_passes_through():
    assert redact_secrets(None) is None


def test_the_card_never_shows_the_secret_even_from_an_old_stored_row(monkeypatch):
    monkeypatch.setattr(settings, "app_public_base_url", BASE)
    # a row stored before this change: raw summary and raw Error fact
    event = _event({"transaction_id": TXN_ID, "facts": {"Req ID": REQID, "Error": LEAKED}},
                   summary=LEAKED, title=f"[tmp-live] error: {LEAKED[:40]}")
    text = str(TeamsChannel().build_card(event))
    assert "A1B2C3D4E5F6" not in text and "FFEE0011AABB" not in text
    assert REQID in text                   # the old fact is still shown, unchanged


def test_new_alerts_are_stored_already_redacted():
    from app.services.notifications.rules.evaluators import _txn_event
    from app.persistence.models.notification import NotificationRule

    rule = NotificationRule(id=uuid.uuid4(), customer_code="tmp-live", name="Error Alert",
                            rule_type="status_match", match={"statuses": ["error"]}, severity="error")
    event = _txn_event(rule, _txn(), "transaction_error")
    assert "A1B2C3D4E5F6" not in (event.summary or "")
    assert "A1B2C3D4E5F6" not in event.payload["facts"]["Error"]
    assert event.payload["reqid"] == REQID and event.payload["date"] == "2026-09-30"


# ---- the feed's request id filter ----------------------------------------------------------------
D = date_type(2026, 6, 26)


async def _seed(db, cc: str) -> list[LogTransaction]:
    job = Job(customer_code=cc, filename="f.log", storage_key="k")
    db.add(job)
    await db.flush()
    rows = []
    for i, (rid, st) in enumerate([("A-1", LogTransactionStatus.success), ("B-2", LogTransactionStatus.error),
                                   (None, LogTransactionStatus.success)]):
        tx = LogTransaction(customer_code=cc, job_id=job.id, date=D, reqid=rid, status=st,
                            started_at=datetime(2026, 6, 26, 9 + i, 0, 0, tzinfo=timezone.utc))
        db.add(tx)
        rows.append(tx)
    await db.flush()
    return rows


async def _view(db, cc, **kw):
    args = dict(customer=cc, db=db, pending={}, date=D, limit=100, offset=0, user=None, hour=None,
                status=None, order_number=None, item_number=None, verbose=False, reqid=None)
    args.update(kw)
    r = await view_transactions(**args)
    return r.headers["X-Total-Count"], r.body.decode()


async def test_the_feed_filters_by_request_id(db):
    cc = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    a, b, c = await _seed(db, cc)
    total, body = await _view(db, cc, reqid="B-2")
    assert total == "1" and str(b.id) in body and str(a.id) not in body and str(c.id) not in body
    assert "request B-2" in body.splitlines()[0]


async def test_the_request_id_is_trimmed_and_exact(db):
    cc = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    _, b, _ = await _seed(db, cc)
    total, body = await _view(db, cc, reqid="  B-2  ")
    assert total == "1" and str(b.id) in body
    assert (await _view(db, cc, reqid="B-"))[0] == "0"


async def test_the_request_id_stacks_on_the_day_and_the_other_filters(db):
    cc = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    await _seed(db, cc)
    assert (await _view(db, cc, reqid="B-2", status=LogTransactionStatus.success))[0] == "0"
    assert (await _view(db, cc, reqid="B-2", date=date_type(2026, 6, 27)))[0] == "0"


async def test_the_request_id_never_crosses_tenants(db):
    cc1 = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    cc2 = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    await _seed(db, cc1)
    assert (await _view(db, cc2, reqid="B-2"))[0] == "0"


async def test_an_empty_request_id_is_no_filter(db):
    cc = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    await _seed(db, cc)
    assert (await _view(db, cc, reqid="   "))[0] == "3"


# ---- the manual "Notify Team" path ---------------------------------------------------------------
async def test_manual_link_keys_come_from_the_tenants_own_transaction(db):
    from app.persistence.repositories.notification_repository import NotificationRepository

    cc = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    a, _, c = await _seed(db, cc)
    repo = NotificationRepository(db)
    assert await repo.transaction_link_keys(cc, str(a.id)) == {"reqid": "A-1", "date": "2026-06-26"}
    assert await repo.transaction_link_keys(cc, str(c.id)) == {"date": "2026-06-26"}
    assert await repo.transaction_link_keys("OTHER_TENANT", str(a.id)) == {}
    assert await repo.transaction_link_keys(cc, "not-a-uuid") == {}
    assert await repo.transaction_link_keys(cc, str(uuid.uuid4())) == {}


async def test_a_manual_alert_on_a_transaction_carries_its_link_keys(monkeypatch):
    """The web page's "Notify Team" goes through the same card link as rule alerts."""
    from types import SimpleNamespace

    from app.api.v1 import notifications as api
    from app.services.notifications import dispatcher

    sent: list[NotificationEvent] = []

    async def fake_enqueue(event):
        sent.append(event)
        return event

    async def fake_deliver_now(_):
        return None

    monkeypatch.setattr(dispatcher, "enqueue", fake_enqueue)
    monkeypatch.setattr(dispatcher, "deliver_now", fake_deliver_now)

    class Repo:
        async def list_channels(self, **_):
            return [SimpleNamespace(id=uuid.uuid4())]

        async def transaction_link_keys(self, code, txn_id):
            assert (code, txn_id) == ("tmp-live", TXN_ID)
            return {"reqid": REQID, "date": "2026-09-30"}

        async def get_event_by_dedup_key(self, _):
            return None

    class Customers:
        async def exists(self, _):
            return True

    body = api.ManualPublishRequest(title="Look at this", transaction_id=TXN_ID)
    await api.publish_manual_endpoint("tmp-live", body, repo=Repo(), customers=Customers())
    (event,) = sent
    assert event.payload["reqid"] == REQID and event.payload["date"] == "2026-09-30"
    assert event.payload["facts"]["Transaction"] == TXN_ID
    monkeypatch.setattr(settings, "app_public_base_url", BASE)
    assert _action_url(TeamsChannel().build_card(event)) == \
        f"{BASE}/?date=2026-09-30&reqid=13-2026-09-30_14%3A45%3A10.141-7116"


# ---- cleaning alerts stored before this change ---------------------------------------------------
def test_redact_value_walks_nested_payloads_and_keeps_non_strings():
    from app.services.notifications.redact import redact_value

    payload = {"transaction_id": TXN_ID, "facts": {"Error": LEAKED, "Duration (ms)": 30,
                                                   "Req ID": REQID}, "list": [LEAKED, None, True]}
    out = redact_value(payload)
    assert "A1B2C3D4E5F6" not in str(out)
    assert out["facts"]["Duration (ms)"] == 30 and out["facts"]["Req ID"] == REQID
    assert out["list"][1:] == [None, True] and out["transaction_id"] == TXN_ID
    assert payload["facts"]["Error"] == LEAKED          # the input is not mutated


async def test_the_cleanup_changes_only_rows_with_secrets(db):
    from app.persistence.models.notification import NotificationEvent as Row
    from app.services.notifications.redact_store import redact_stored_events

    cc = f"TESTCH131_{uuid.uuid4().hex[:6]}"
    dirty = Row(customer_code=cc, event_type="transaction_error", severity="error", title="t",
                summary=LEAKED, dedup_key=f"a:{uuid.uuid4()}",
                payload={"transaction_id": TXN_ID, "facts": {"Error": LEAKED}})
    clean = Row(customer_code=cc, event_type="transaction_error", severity="error", title="t",
                summary="Timeout", dedup_key=f"b:{uuid.uuid4()}", payload={"facts": {"Error": "Timeout"}})
    db.add_all([dirty, clean])
    await db.flush()

    dry = await redact_stored_events(db, cc, apply=False)
    assert dry == {"scanned": 2, "changed": 1, "ids": [str(dirty.id)], "applied": False}
    await db.refresh(dirty)
    assert dirty.summary == LEAKED                       # a dry run writes nothing

    done = await redact_stored_events(db, cc, apply=True)
    assert done["changed"] == 1 and done["applied"] is True
    await db.refresh(dirty)
    await db.refresh(clean)
    assert "A1B2C3D4E5F6" not in dirty.summary and "A1B2C3D4E5F6" not in str(dirty.payload)
    assert dirty.payload["transaction_id"] == TXN_ID
    assert clean.summary == "Timeout" and clean.payload == {"facts": {"Error": "Timeout"}}
    assert (await redact_stored_events(db, cc, apply=False))["changed"] == 0   # safe to run again


@pytest.mark.parametrize("raw", [
    LEAKED, "GET /x?password=hunter2&user=A", 'b {"Password": "p", "M3Credentials": {"a": "b"}}',
    "b %22Password%22%3A%22p%22", "Authorization: Bearer abc", "GET /x?password=&u=1",
])
def test_redacting_twice_changes_nothing(raw):
    once = redact_secrets(raw)
    assert redact_secrets(once) == once
