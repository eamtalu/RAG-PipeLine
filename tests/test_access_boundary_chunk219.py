"""Chunk 219: the boundary of access control, read with `ast` so a wrong import fails here first.

Access control is who a person is to us, which tenant and customer they belong to, what they may do,
and how people are added, approved and removed (app/services/access/README.md). It is one package
with one door:
- Outside the package, code imports `app.services.access` itself, never a submodule, so the
  package can be reorganised (or moved to its own project) without touching its callers.
- The package never imports a feature (Ask, groups, tasks, watchlist, advisories, analytics, the
  agents, notifications) or the web layer; features depend on access control, never the reverse.
- The access-owned tables are read and written only through the package (and the persistence
  layer that defines them, and the tenant-binding route that the superadmin calls).
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ACCESS = "app/services/access/"
FACADE = "app.services.access"

FEATURES = tuple(
    f"app.services.{name}"
    for name in ("ask", "groups", "tasks", "watchlist", "advisories", "analytics", "analytics_agent",
                 "analytics_at_risk", "analytics_forecast", "analytics_ml", "log_agent",
                 "logspace_agent", "notifications")
) + ("app.agent", "app.api")

ACCESS_MODELS = ("app.persistence.models.access", "app.persistence.models.teams_binding")
# Who besides the package may touch the access tables: the layer that defines them, and the
# superadmin's tenant-binding route, which the package's tenancy module serves.
MODEL_IMPORTERS_ALLOWED = ("app/persistence/", "app/api/v1/teams.py")


def _imports(path: Path) -> set[str]:
    """Every `app...` module a file imports, by `from` or `import`.

    (a file with `from app.services.access import records`) -> {"app.services.access"} ;
    (a file with `from app.services.access.records import X`) -> {"app.services.access.records"}
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("app"):
            found.add(node.module)
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names if a.name.startswith("app"))
    return found


def _sources() -> list[Path]:
    return [*(ROOT / "app").rglob("*.py"), *(ROOT / "scripts").rglob("*.py")]


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _submodules() -> set[str]:
    """The names of the package's own modules and subpackages, which callers may not import.

    (access/ with records.py and people/) -> {"records", "people", ...}
    """
    base = ROOT / ACCESS
    return {p.stem for p in base.glob("*.py") if p.stem != "__init__"} | {
        p.name for p in base.iterdir() if p.is_dir() and (p / "__init__.py").is_file()
    }


def _facade_names(path: Path) -> set[str]:
    """The names a file takes from the front door, as in `from app.services.access import X`."""
    tree = ast.parse(path.read_text(), filename=str(path))
    return {
        a.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == FACADE
        for a in node.names
    }


def test_outside_code_uses_only_the_front_door():
    inner = _submodules()
    offenders = sorted(
        {(_rel(p), m) for p in _sources() if not _rel(p).startswith(ACCESS)
         for m in _imports(p) if m.startswith(FACADE + ".")}
        | {(_rel(p), f"{FACADE}.{n}") for p in _sources() if not _rel(p).startswith(ACCESS)
           for n in _facade_names(p) if n in inner}
    )
    assert offenders == []


def test_access_control_never_imports_a_feature_or_the_web_layer():
    offenders = sorted(
        (_rel(p), m)
        for p in (ROOT / ACCESS).rglob("*.py")
        for m in _imports(p)
        if any(m == f or m.startswith(f + ".") or m.startswith(f + "_") for f in FEATURES)
    )
    assert offenders == []


def test_the_access_tables_are_reached_only_through_the_package():
    offenders = sorted(
        (_rel(p), m)
        for p in _sources()
        if not _rel(p).startswith((ACCESS, *MODEL_IMPORTERS_ALLOWED))
        for m in _imports(p)
        if m in ACCESS_MODELS
    )
    assert offenders == []


def test_the_package_explains_itself():
    readme = ROOT / ACCESS / "README.md"
    assert readme.is_file(), "app/services/access/README.md says what access control is and who calls it"
