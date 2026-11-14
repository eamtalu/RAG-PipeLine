"""A profile is a file: what an agent is, as data, loaded and validated once with a version.

`app/agent/profiles/<name>.yaml` holds the system prompt, the two nudges, the domain and decline
sentences, the schema tool names, the namespaces, the guards in run order, what the profile
needs of a model, which presentation the runtime uses, and the question frame; `includes` names
the shared fragments under `fragments/` composed into the prompt (step 19). The loader is strict
(exactly the keys below plus the optional ones) so a later step adds its keys on purpose.
`tests/profile_snapshots/` holds the prompt text byte for byte; a prompt edit must change the
snapshot in the same commit, which is the reviewed change the architecture asks for.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import yaml

from app.agent.presentation import presentation_for
from app.agent.profiles.version import file_version
from app.agent.router.policy import ModelPolicy

PROFILES_DIR = Path(__file__).parent
#: The prose every profile may share (step 19): `fragments/<name>.md`, appended to the prompt.
FRAGMENTS_DIR = PROFILES_DIR / "fragments"
OPTIONAL_KEYS = ("includes",)
_FRAGMENT_NAME = re.compile(r"^[a-z_]+$")
REQUIRED_KEYS = (
    "name",
    "domain",
    "decline",
    "schema_tools",
    "namespaces",
    "guards",
    "model",
    "presentation",
    "question_frame",
    "retry_nudge",
    "ungrounded_nudge",
    "system_prompt",
)


@dataclass(frozen=True, slots=True)
class Profile:
    """One agent's data: prompts, sentences, schema tools, its tool set and presentation names."""

    name: str
    domain: str
    decline: str
    system_prompt: str
    retry_nudge: str
    ungrounded_nudge: str
    schema_tools: frozenset[str]
    namespaces: tuple[str, ...]
    guards: tuple[str, ...]  # the pipeline, in run order (app/agent/runtime/pipeline.py)
    model: ModelPolicy  # what the profile needs of a model; the router picks (app/agent/router)
    presentation: str
    question_frame: str
    version: str
    includes: tuple[str, ...] = ()  # the fragments composed into system_prompt, in order


def compose_prompt(prompt: str, fragments: Sequence[str]) -> str:
    """The system prompt with each fragment after one blank line; it ends with a newline when the
    prompt did, so a profile's own text keeps its shape.

    >>> compose_prompt("A\\n", ["B\\n"])
    'A\\n\\nB\\n'
    >>> compose_prompt("A", ["B\\n"])
    'A\\n\\nB'
    """
    out = prompt.rstrip("\n")
    for fragment in fragments:
        out += "\n\n" + fragment.rstrip("\n")
    return out + ("\n" if prompt.endswith("\n") else "")


def _includes_of(raw: dict, where: str) -> tuple[str, ...]:
    """The fragment names a profile includes, checked to exist, so a typo is refused by name.

    ({"includes": ["presentation"]}) -> ("presentation",) ; ({}) -> () ; (["nope"]) -> ValueError
    """
    names = raw.get("includes") or []
    if (
        not isinstance(names, list)
        or not all(isinstance(n, str) and _FRAGMENT_NAME.match(n) for n in names)
        or len(set(names)) != len(names)
    ):
        raise ValueError(f"{where}: includes must be a list of unique fragment names")
    for name in names:
        if not (FRAGMENTS_DIR / f"{name}.md").is_file():
            raise ValueError(f"{where}: unknown fragment {name!r}; the fragments are {fragment_names()}")
    return tuple(names)


def fragment_names() -> tuple[str, ...]:
    """The fragments that exist, by file name.

    (fragments/presentation.md) -> ("presentation",)
    """
    return tuple(sorted(p.stem for p in FRAGMENTS_DIR.glob("*.md")))


def source_bytes(path: Path) -> bytes:
    """The bytes the profile's version names: its file, then each included fragment in order, so a
    changed fragment changes every profile that includes it.

    (analytics.yaml including presentation) -> the yaml bytes + presentation.md bytes
    """
    data = path.read_bytes()
    raw = yaml.safe_load(data)
    names = raw.get("includes") or [] if isinstance(raw, dict) else []
    return data + b"".join((FRAGMENTS_DIR / f"{n}.md").read_bytes() for n in names if isinstance(n, str))


def read_profile(path: Path) -> Profile:
    """The profile in one YAML file, refused with a named reason when anything is off.

    (profiles/analytics.yaml) -> Profile(name="analytics", namespaces=("logs", "analytics"), ...) ;
    (a file with an extra key) -> ValueError naming the file and the key
    """
    data = path.read_bytes()
    raw = yaml.safe_load(data)
    where = path.name
    # Step 1: Hold the file to exactly the known keys, so that a typo cannot silently drop a
    # prompt and a later step adds its keys deliberately.
    if not isinstance(raw, dict):
        raise ValueError(f"{where}: a profile is a mapping of {len(REQUIRED_KEYS)} keys")
    missing = [k for k in REQUIRED_KEYS if k not in raw]
    unknown = [k for k in raw if k not in REQUIRED_KEYS and k not in OPTIONAL_KEYS]
    if missing or unknown:
        raise ValueError(f"{where}: missing keys {missing}, unknown keys {unknown}")
    includes = _includes_of(raw, where)
    # Step 2: Check the values the runtime will format or look up, because a frame without a
    # question slot or a presentation that does not exist would fail on the first question.
    text_keys = (
        "name",
        "domain",
        "decline",
        "system_prompt",
        "retry_nudge",
        "ungrounded_nudge",
        "presentation",
        "question_frame",
    )
    for key in text_keys:
        if not isinstance(raw[key], str) or not raw[key].strip():
            raise ValueError(f"{where}: {key} must be a non-empty string")
    if raw["name"] != path.stem:
        raise ValueError(f"{where}: name {raw['name']!r} must equal the file name {path.stem!r}")
    if "{question}" not in raw["question_frame"]:
        raise ValueError(f"{where}: question_frame must contain {{question}}")
    if "{figures}" not in raw["ungrounded_nudge"]:
        raise ValueError(f"{where}: ungrounded_nudge must contain {{figures}}")
    tools = raw["schema_tools"]
    if (
        not isinstance(tools, list)
        or not all(isinstance(t, str) for t in tools)
        or len(set(tools)) != len(tools)
    ):
        raise ValueError(f"{where}: schema_tools must be a list of unique tool names")
    namespaces = raw["namespaces"]
    if (
        not isinstance(namespaces, list)
        or not namespaces
        or not all(isinstance(n, str) and n.strip() for n in namespaces)
        or len(set(namespaces)) != len(namespaces)
    ):
        raise ValueError(f"{where}: namespaces must be a non-empty list of unique names")
    guards = raw["guards"]
    if (
        not isinstance(guards, list)
        or not guards
        or not all(isinstance(g, str) and g.strip() for g in guards)
        or len(set(guards)) != len(guards)
    ):
        raise ValueError(f"{where}: guards must be a non-empty list of unique names")
    model = ModelPolicy.from_dict(raw["model"], where)
    presentation_for(raw["presentation"])  # raises for an unknown presentation
    # Step 3: Compose the shared fragments into the prompt and version the lot, so the snapshot
    # pins what the model reads and a fragment edit changes every profile's version.
    fragments = [(FRAGMENTS_DIR / f"{n}.md").read_text() for n in includes]
    return Profile(
        name=raw["name"],
        domain=raw["domain"],
        decline=raw["decline"],
        system_prompt=compose_prompt(raw["system_prompt"], fragments),
        retry_nudge=raw["retry_nudge"],
        ungrounded_nudge=raw["ungrounded_nudge"],
        schema_tools=frozenset(tools),
        namespaces=tuple(raw["namespaces"]),
        guards=tuple(guards),
        model=model,
        presentation=raw["presentation"],
        question_frame=raw["question_frame"],
        version=file_version(data + b"".join(f.encode() for f in fragments)),
        includes=includes,
    )


@cache
def load_profile(name: str) -> Profile:
    """The named profile, read once per process.

    ("analytics") -> Profile(name="analytics", ...) ; ("nope") -> ValueError listing the names
    """
    path = PROFILES_DIR / f"{name}.yaml"
    if not path.is_file():
        raise ValueError(f"no profile {name!r}; the profiles are {profile_names()}")
    return read_profile(path)


def profile_names() -> tuple[str, ...]:
    """The profiles that exist, by file name.

    >>> profile_names()
    ('analytics', 'logspace')
    """
    return tuple(sorted(p.stem for p in PROFILES_DIR.glob("*.yaml")))
