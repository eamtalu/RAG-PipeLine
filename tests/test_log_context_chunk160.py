"""Step 9 of docs/esmart-agent-execution-plan.md: every log line says which request and tenant.

A request id and a tenant travel in context variables (`app/log_context.py`); the log record
factory stamps both on every record, so they are there whatever logger wrote the line and
whichever handler formats it. The API middleware binds them from the request (echoing
X-Request-ID back), the Teams consumer from the job, the workers from the tenant they are on.
JSON output is the default so the lines can be filtered with jq.
"""

import json
import logging

from httpx import ASGITransport, AsyncClient

from app import log_context
from app.background import JsonFormatter, setup_logging
from app.main import app
from app.services.teams import consumer as c
from app.settings import settings
from tests.test_teams_consumer_chunk123 import Harness, job

logger = logging.getLogger("tests.log_context")


# ==================================================== 1. the context and the record factory


def test_records_carry_the_bound_request_id_and_tenant(caplog):
    setup_logging()
    with caplog.at_level(logging.INFO, logger="tests.log_context"):
        with log_context.bind(request_id="req-1", tenant="acme"):
            logger.info("inside")
        logger.info("outside")
    inside, outside = caplog.records[-2:]
    assert (inside.request_id, inside.tenant) == ("req-1", "acme")
    assert (outside.request_id, outside.tenant) == (None, None)


def test_bind_nests_and_restores():
    with log_context.bind(request_id="a", tenant="t1"):
        with log_context.bind(tenant="t2"):
            assert log_context.current() == {"request_id": "a", "tenant": "t2"}
        assert log_context.current() == {"request_id": "a", "tenant": "t1"}
    assert log_context.current() == {"request_id": None, "tenant": None}


# ==================================================== 2. the formats


def test_the_json_line_has_the_fields_and_the_exception():
    record = logging.LogRecord("x", logging.ERROR, __file__, 1, "boom %s", ("now",), None)
    record.request_id, record.tenant = "req-2", "acme"
    try:
        raise ValueError("bad")
    except ValueError:
        import sys

        record.exc_info = sys.exc_info()
    line = json.loads(JsonFormatter().format(record))
    assert line["msg"] == "boom now" and line["level"] == "ERROR" and line["logger"] == "x"
    assert line["request_id"] == "req-2" and line["tenant"] == "acme"
    assert "ValueError: bad" in line["exc"] and "ts" in line


def test_the_text_line_carries_the_fields_too(monkeypatch):
    monkeypatch.setattr(settings, "log_output", "text")
    from app.background import build_formatter

    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", (), None)
    record.request_id, record.tenant = None, "acme"
    assert "request_id=- tenant=acme hello" in build_formatter().format(record)


def test_setup_logging_installs_the_json_formatter_by_default(monkeypatch):
    from logging.handlers import RotatingFileHandler
    from app.background import build_formatter

    monkeypatch.setattr(settings, "log_output", "json")
    assert isinstance(build_formatter(), JsonFormatter)
    setup_logging()
    # pytest's own capture handlers also subclass StreamHandler; only the app's are formatted here
    own = [
        h
        for h in logging.getLogger().handlers
        if type(h) is logging.StreamHandler or isinstance(h, RotatingFileHandler)
    ]
    assert all(isinstance(h.formatter, JsonFormatter) for h in own)


# ==================================================== 3. the API binds from the request


async def test_the_api_echoes_or_mints_the_request_id_and_binds_the_tenant(caplog, monkeypatch):
    monkeypatch.setattr(settings, "api_auth_required", False)
    setup_logging()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        with caplog.at_level(logging.INFO):
            given = await client.get(
                "/api/v1/customers/timezones",
                headers={"X-Customer-Code": "acme", "X-Request-ID": "abc-123"},
            )
            minted = await client.get("/health")
    assert given.headers["x-request-id"] == "abc-123"
    assert len(minted.headers["x-request-id"]) >= 16
    lines = [r for r in caplog.records if getattr(r, "request_id", None) == "abc-123"]
    assert lines and all(r.tenant == "acme" for r in lines)
    assert any(
        r.name == "app.middleware.request_context"
        and "api.request" in r.message
        and "status=200" in r.message
        for r in lines
    )


# ==================================================== 4. the consumer binds from the job


async def test_the_consumer_binds_the_job_id_and_the_customer(caplog):
    setup_logging()

    async def chatty(code, question, history):
        logging.getLogger("tests.agent").info("thinking")
        return {"answer": "ok", "tool_calls": []}

    h = Harness([job(job_id="job-9", customer_code="acme")], agent=chatty)
    with caplog.at_level(logging.INFO):
        await h.drain()
    thinking = [r for r in caplog.records if r.message == "thinking"]
    assert thinking and (thinking[0].request_id, thinking[0].tenant) == ("job-9", "acme")
    answered = [r for r in caplog.records if r.name == c.__name__ and "answered" in r.message]
    assert answered and answered[0].request_id == "job-9" and answered[0].tenant == "acme"
