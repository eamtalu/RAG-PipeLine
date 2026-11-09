"""The boundary of the agent package, read with `ast` so a wrong import fails here first.

Rules (docs/esmart-agent-architecture.md, section 12):
- `app.agent` never imports the web, worker or persistence layers.
- The analytics side (`app.services`, `app.persistence`) never imports `app.agent`; only the
  channels (`app/teams_consumer.py`, `app/api/*`) may.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENT = ROOT / "app" / "agent"

# What the agent package must not reach into.
AGENT_MAY_NOT_IMPORT = ("app.persistence", "app.api", "app.background", "app.worker", "app.main")

# Who may import the agent package: the channels, until they move inside it.
CHANNEL_FILES = {"app/teams_consumer.py"}
CHANNEL_DIRS = ("app/api/",)

# The legacy agent modules still outside the package (their tools read the database directly,
# so they wait for the query model). They import the loop and the guards from app.agent for now.
# This list must only ever shrink; a new entry is a design error.
TRANSITIONAL_IMPORTER_DIRS = (
    "app/services/analytics_agent/",
    "app/services/logspace_agent/",
    "app/services/log_agent/",
)


def _imports(path: Path) -> set[str]:
    """Every `app...` module a file imports, by `from` or `import`.

    (app/main.py) -> {"app.api.v1.router", "app.settings", ...} ; (a file with no app imports) -> set()
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("app"):
            found.add(node.module)
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names if a.name.startswith("app"))
    return found


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def test_the_agent_package_exists():
    assert (AGENT / "__init__.py").is_file(), "app/agent/__init__.py is the package's root"


def test_the_agent_package_never_imports_web_worker_or_persistence():
    offenders = {
        (_rel(p), m)
        for p in AGENT.rglob("*.py")
        for m in _imports(p)
        if m.startswith(AGENT_MAY_NOT_IMPORT)
    }
    assert offenders == set()


def test_only_channels_import_the_agent_package():
    importers = {
        _rel(p)
        for p in (ROOT / "app").rglob("*.py")
        if not _rel(p).startswith("app/agent/")
        and any(m == "app.agent" or m.startswith("app.agent.") for m in _imports(p))
    }
    not_channels = {
        f
        for f in importers
        if f not in CHANNEL_FILES
        and not f.startswith(CHANNEL_DIRS)
        and not f.startswith(TRANSITIONAL_IMPORTER_DIRS)
    }
    assert not_channels == set()
