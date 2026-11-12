"""The copies of the shared contract equal the snapshot the edge repo wrote, so both repos agree."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import contract_schema as cs  # noqa: E402

SNAPSHOT = ROOT / "docs" / "contract_schema.json"


def test_the_contract_copies_match_the_edge_snapshot():
    assert SNAPSHOT.exists(), "run the edge's scripts/sync_contract.sh"
    committed = json.loads(SNAPSHOT.read_text())
    assert cs.snapshot() == committed, "the contract copies drift from the snapshot: run the edge's sync_contract.sh"


def test_the_snapshot_names_the_revision_and_the_rows():
    snap = cs.snapshot()
    assert snap["revision"] == cs.contracts.CONTRACT_REVISION
    assert set(snap["models"]) == {"QuestionJob", "AnswerPayload", "MirrorEnvelope"}
    assert "site_home" in snap["key_layout"]
