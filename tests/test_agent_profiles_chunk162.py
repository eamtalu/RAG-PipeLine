"""Step 12 of docs/esmart-agent-execution-plan.md: a profile is a file, and there is one runtime.

The two agent classes and the legacy Claude agent carried their prompts, nudges, domain, tool set
and presentation as class attributes and hooks. Now `app/agent/profiles/<name>.yaml` holds that
as data, loaded and validated once with a version, and `GroundedAgent` takes a profile and a tool
set. Answers are unchanged: every prompt is held to a byte-for-byte snapshot of the previous
constants in tests/profile_snapshots/.
"""

import importlib.util
import json
import logging
import subprocess
from pathlib import Path

import pytest
import yaml
from langchain_core.messages import AIMessage

from app.agent.gateway import run_request
from app.agent.profiles import REQUIRED_KEYS, load_profile, profile_names, read_profile
from app.agent.profiles.version import code_version, file_version
from app.agent.runtime.grounded import GroundedAgent, frame_question
from app.settings import settings
from tests.agent_helpers import TODAY, TZ, analytics_runtime, ask, context_for, request_for
from tests.test_analytics_agent_chunk124 import CC, _scripted, clean  # noqa: F401

SNAPSHOTS = Path(__file__).parent / "profile_snapshots"
PROFILES = Path("app/agent/profiles")
REPO = Path(__file__).resolve().parents[1]


def _snapshot(name: str) -> dict:
    return json.loads((SNAPSHOTS / f"{name}.json").read_text())


# ==================================================== 1. the files load


def test_both_profiles_load_with_exactly_the_required_keys():
    assert profile_names() == ("analytics", "logspace")
    for name in profile_names():
        raw = yaml.safe_load((PROFILES / f"{name}.yaml").read_text())
        assert set(raw) == set(REQUIRED_KEYS)
        profile = load_profile(name)
        assert profile.name == name and profile.presentation == name
        expected = {"analytics": ("logs", "analytics"), "logspace": ("logspace",)}
        assert profile.namespaces == expected[name]


# ==================================================== 2. the prompts are byte for byte unchanged


@pytest.mark.parametrize("name", ["analytics", "logspace"])
def test_the_prompts_equal_the_snapshot_of_the_previous_constants(name):
    snap, profile = _snapshot(name), load_profile(name)
    assert profile.system_prompt == snap["system_prompt"]
    assert "\n\n" + profile.retry_nudge == snap["retry_nudge"]
    assert "\n\n" + profile.ungrounded_nudge == snap["ungrounded_nudge"]
    assert profile.domain == snap["domain"] and profile.decline == snap["decline"]


def test_the_schema_tools_are_what_they_were():
    assert load_profile("analytics").schema_tools == frozenset(
        {"describe_releases", "list_metrics", "explain_freshness", "describe_at_risk"}
    )
    assert load_profile("logspace").schema_tools == frozenset({"overview"})


# ==================================================== 3. the loader refuses a bad file


def _write(tmp_path: Path, name: str, **changes) -> Path:
    raw = yaml.safe_load((PROFILES / "analytics.yaml").read_text())
    raw["name"] = name
    for key, value in changes.items():
        if value is None:
            raw.pop(key, None)
        else:
            raw[key] = value
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize(
    "changes, expected",
    [
        ({"extra_key": "x"}, "extra_key"),
        ({"decline": None}, "decline"),
        ({"question_frame": "no placeholder"}, "question"),
        ({"ungrounded_nudge": "no placeholder"}, "figures"),
        ({"presentation": "nope"}, "presentation"),
    ],
)
def test_a_bad_profile_is_refused_naming_the_problem(tmp_path, changes, expected):
    with pytest.raises(ValueError, match=expected):
        read_profile(_write(tmp_path, "custom", **changes))


def test_a_name_that_differs_from_the_file_stem_is_refused(tmp_path):
    path = _write(tmp_path, "custom")
    path.rename(tmp_path / "other.yaml")
    with pytest.raises(ValueError, match="name"):
        read_profile(tmp_path / "other.yaml")


# ==================================================== 4. versions


def test_the_profile_version_is_the_hash_of_the_file(tmp_path):
    path = PROFILES / "analytics.yaml"
    assert load_profile("analytics").version == file_version(path.read_bytes())
    copy = tmp_path / "analytics.yaml"
    copy.write_bytes(path.read_bytes().replace(b"warehouse", b"Warehouse", 1))
    assert read_profile(copy).version != load_profile("analytics").version


def test_the_code_version_is_the_short_git_hash_or_unknown(monkeypatch):
    expected = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    code_version.cache_clear()
    assert code_version() == expected
    code_version.cache_clear()

    def boom(*a, **kw):
        raise OSError("no git")

    monkeypatch.setattr(subprocess, "run", boom)
    assert code_version() == "unknown"
    code_version.cache_clear()


# ==================================================== 5. versions on the result and the log line


async def test_a_result_carries_both_versions_and_the_log_line_names_them(caplog):
    runtime = analytics_runtime(CC, model=_scripted(AIMessage(content="Fine.")))
    with caplog.at_level(logging.INFO, logger="app.agent.gateway.run"):
        result = await run_request(request_for(CC, "hi"), runtime=runtime)
    assert result.profile_version == load_profile("analytics").version
    assert result.code_version == code_version() and result.code_version != ""
    assert list(result.to_legacy_dict()) == [
        "answer",
        "stop_reason",
        "tool_calls",
        "iterations",
        "retries",
        "model",
        "evidence",
        "evidence_data",
        "withheld_figures",
    ]
    line = next(r.message for r in caplog.records if r.message.startswith("agent.run"))
    assert (
        f"profile_version={result.profile_version}" in line
        and f"code_version={result.code_version}" in line
    )


# (section 6, the tool sets, moved to tests/test_agent_tool_registry_chunk163.py in step 13)


# ==================================================== 7. the question frame


def test_the_question_frames_are_what_the_subclasses_produced():
    assert frame_question(
        load_profile("analytics"), "how many?", today=TODAY, scope_description=None
    ) == ("(Today is 2026-09-30.)\n\nhow many?")
    assert frame_question(
        load_profile("logspace"),
        "has D1 been picked?",
        today=TODAY,
        scope_description="2026-09-30 · user PEVANS",
    ) == (
        "(Today is 2026-09-30. The records fetched: 2026-09-30 · user PEVANS.)\n\n"
        "has D1 been picked?"
    )


# ==================================================== 8. the three classes are gone


def test_the_three_agent_modules_and_the_dead_settings_are_gone():
    for module in (
        "app.services.analytics_agent.agent",
        "app.services.logspace_agent.agent",
        "app.services.log_agent.agent",
    ):
        assert importlib.util.find_spec(module) is None, module
    for field in (
        "teams_agent",
        "log_agent_model",
        "log_agent_max_tokens",
        "log_agent_max_iterations",
        "log_agent_max_retries",
    ):
        assert not hasattr(settings, field), field
    assert not hasattr(GroundedAgent, "ask")


async def test_ask_through_the_helper_is_the_legacy_shape():
    out = await ask(analytics_runtime(CC, model=_scripted(AIMessage(content="Fine."))), "hi")
    assert out["answer"] == "Fine." and out["stop_reason"] == "end_turn" and TZ
