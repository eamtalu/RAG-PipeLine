"""The shared edge/backend contract as a JSON schema snapshot, so the two repos can prove they agree.

    PYTHONPATH=. .venv/bin/python scripts/contract_schema.py            # print the snapshot

The edge repo is canonical: its `scripts/contract_schema.py --write` writes the snapshot and its
`scripts/sync_contract.sh` copies the contract files and `docs/contract_schema.json` here. This
script only prints; `tests/test_contract_schema.py` fails when this repo's copies and the snapshot
drift apart, which means the sync was not run.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.teams import contracts, mirror_rows  # noqa: E402

SNAPSHOT = ROOT / "docs" / "contract_schema.json"

#: The models whose JSON schema is the contract; the same set the edge snapshots.
SHARED_MODELS = {
    "QuestionJob": contracts.QuestionJob,
    "AnswerPayload": contracts.AnswerPayload,
    "MirrorEnvelope": mirror_rows.MirrorEnvelope,
}


def snapshot() -> dict:
    """The snapshot: the revision, every shared model's JSON schema, and the mirror key layout.

    () -> {"revision": "3.5", "models": {...}, "key_layout": {...}}
    """
    return {
        "revision": contracts.CONTRACT_REVISION,
        "mirror_schema_version": mirror_rows.MIRROR_SCHEMA_VERSION,
        "models": {name: model.model_json_schema() for name, model in SHARED_MODELS.items()},
        "key_layout": {name: spec.model_dump() for name, spec in mirror_rows.KEY_LAYOUT.items()},
    }


if __name__ == "__main__":
    print(json.dumps(snapshot(), indent=2, sort_keys=True))
