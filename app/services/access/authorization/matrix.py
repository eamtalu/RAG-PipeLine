"""The roles, the capabilities and the default matrix, read from matrix.yaml next to this file
(stage D, step 7). The codes are the vocabulary every other access module speaks; the names are
what the Admin tab shows. Used by the migration's seed, the per-customer seed and the records.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

STATES = ("allowed", "with_approval", "no")
ALLOWED, WITH_APPROVAL, NO = STATES

MATRIX_FILE = Path(__file__).with_name("matrix.yaml")


@dataclass(frozen=True)
class Named:
    code: str
    name: str
    sort_order: int


@dataclass(frozen=True)
class Matrix:
    """What the file holds: the roles and capabilities in order, and the default state per cell."""

    roles: tuple[Named, ...]
    capabilities: tuple[Named, ...]
    defaults: dict[tuple[str, str], str]  # (role_code, capability_code) -> state

    @property
    def role_codes(self) -> tuple[str, ...]:
        return tuple(r.code for r in self.roles)

    @property
    def capability_codes(self) -> tuple[str, ...]:
        return tuple(c.code for c in self.capabilities)

    def default(self, role_code: str, capability_code: str) -> str:
        """The file's state for one cell; "no" for a pair the file does not name.

        >>> load().default("supervisor", "view_all_sites"), load().default("picker", "approve")
        ('with_approval', 'no')
        """
        return self.defaults.get((role_code, capability_code), NO)


@lru_cache(maxsize=1)
def load(path: Path = MATRIX_FILE) -> Matrix:
    """The matrix file parsed once and checked: six roles, eight capabilities, every cell a state.

    >>> m = load()
    >>> m.role_codes[0], m.role_codes[-1], len(m.capability_codes)
    ('picker', 'admin', 8)
    """
    raw = yaml.safe_load(path.read_text())
    roles = tuple(Named(r["code"], r["name"], i) for i, r in enumerate(raw["roles"]))
    capabilities = tuple(Named(c["code"], c["name"], i) for i, c in enumerate(raw["capabilities"]))
    defaults: dict[tuple[str, str], str] = {}
    for capability in capabilities:
        states = raw["matrix"].get(capability.code)
        if not isinstance(states, list) or len(states) != len(roles):
            raise ValueError(f"matrix row {capability.code!r} must list {len(roles)} states")
        for role, state in zip(roles, states):
            if state not in STATES:
                raise ValueError(f"matrix cell {capability.code}/{role.code} has state {state!r}")
            defaults[(role.code, capability.code)] = state
    return Matrix(roles=roles, capabilities=capabilities, defaults=defaults)
