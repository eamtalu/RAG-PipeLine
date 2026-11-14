"""Step 19 (D19) chunk 190: one presentation policy every profile includes as a fragment, versioned
with the profile, pinned by the snapshot; the table stripper keeps the bullets that carry the
reasoning."""

from pathlib import Path

import pytest
import yaml

from app.agent.presentation.evidence import strip_tables
from app.agent.profiles import (
    FRAGMENTS_DIR,
    PROFILES_DIR,
    compose_prompt,
    load_profile,
    profile_names,
    read_profile,
    source_bytes,
)

FRAGMENT = (FRAGMENTS_DIR / "presentation.md").read_text()


# ==================================================== 1. the fragment is part of every profile


def test_both_profiles_include_the_presentation_fragment_once_at_the_end():
    for name in profile_names():
        prompt = load_profile(name).system_prompt
        assert prompt.rstrip("\n").endswith(FRAGMENT.rstrip("\n")), name
        assert prompt.count("How to present an answer") == 1, name
        assert load_profile(name).includes == ("presentation",)


def test_the_fragment_is_short_and_names_the_tools_as_offered_only():
    lines = [line for line in FRAGMENT.splitlines() if line.strip()]
    assert len(lines) <= 26  # the budget: about 500 tokens on a 16k context
    assert "apply only when they are offered" in FRAGMENT
    for tool in ("chart_from_data", "show_table", "show_kpis", "state_findings", "suggest_followups"):
        assert tool in FRAGMENT


def test_the_analytics_prompt_no_longer_carries_the_old_presenting_block():
    prompt = load_profile("analytics").system_prompt
    assert "Presenting the answer (only when" not in prompt
    assert "do not repeat the rows as a" not in prompt
    assert "then a compact table if there are several rows" not in prompt
    assert "Cite the release key" in prompt  # the domain rule stays


def test_compose_prompt_puts_one_blank_line_before_each_fragment():
    assert compose_prompt("A\n", ["B\n"]) == "A\n\nB\n"
    assert compose_prompt("A", ["B\n", "C"]) == "A\n\nB\n\nC"
    assert compose_prompt("A\n", []) == "A\n"


# ==================================================== 2. the version covers the fragment


def test_the_version_changes_when_only_the_fragment_changes(tmp_path, monkeypatch):
    path = PROFILES_DIR / "analytics.yaml"
    before = read_profile(path).version
    assert before == load_profile("analytics").version
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    (fragments / "presentation.md").write_text(FRAGMENT + "- One more rule.\n")
    monkeypatch.setattr("app.agent.profiles.FRAGMENTS_DIR", fragments)
    after = read_profile(path)
    assert after.version != before
    assert after.system_prompt.rstrip("\n").endswith("- One more rule.")
    assert source_bytes(path).endswith(b"- One more rule.\n")


def test_an_unknown_fragment_is_refused_naming_it(tmp_path):
    raw = yaml.safe_load((PROFILES_DIR / "analytics.yaml").read_text())
    raw["name"] = "custom"
    raw["includes"] = ["presentation", "nope"]
    path = tmp_path / "custom.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="unknown fragment 'nope'"):
        read_profile(path)
    raw["includes"] = ["presentation", "presentation"]
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="includes"):
        read_profile(path)


def test_a_profile_without_includes_still_loads_with_its_prompt_as_written(tmp_path):
    raw = yaml.safe_load((PROFILES_DIR / "analytics.yaml").read_text())
    raw["name"] = "bare"
    raw.pop("includes")
    raw["system_prompt"] = "Just this.\n"
    path = tmp_path / "bare.yaml"
    path.write_text(yaml.safe_dump(raw))
    profile = read_profile(path)
    assert profile.system_prompt == "Just this.\n" and profile.includes == ()


# ==================================================== 3. the table stripper keeps reasoning bullets


def test_strip_tables_keeps_text_bullets_and_drops_row_like_lines():
    text = (
        "Two items were short.\n"
        "- GINGER - 125 units short (2 releases)\n"
        "1. PARSLEY 11 units on 2 releases\n"
        "- The night shift carried the slip\n"
        "- 15 left behind on Thursday\n"
        "| item | short |\n|---|---|\n| GINGER | 125 |\n"
        "So the gap sits with two items."
    )
    out = strip_tables(text)
    assert "125 units short" not in out and "PARSLEY 11" not in out and "| item |" not in out
    assert "- The night shift carried the slip" in out
    assert "- 15 left behind on Thursday" in out
    assert out.startswith("Two items were short.") and out.endswith("two items.")


def test_the_snapshot_script_reports_drift_and_writes_the_files(tmp_path):
    from scripts.profile_snapshots import snapshot_of, write_snapshots

    snap = snapshot_of("analytics")
    assert snap["system_prompt"] == load_profile("analytics").system_prompt
    assert snap["retry_nudge"].startswith("\n\n")
    written = write_snapshots(tmp_path)
    assert sorted(p.name for p in written) == ["analytics.json", "logspace.json"]
    assert (tmp_path / "analytics.json").read_text().endswith("}\n")
    assert Path(__file__).parent.joinpath("profile_snapshots", "analytics.json").read_text() == (
        tmp_path / "analytics.json"
    ).read_text()
