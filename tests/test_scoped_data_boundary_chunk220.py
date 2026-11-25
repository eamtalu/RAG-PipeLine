"""Chunk 220: the boundary of the scoped data package, read with `ast` so a wrong import fails here.

`app/services/scoped_data/` is the one seam between the agent's tools and the data tables, so it
must be movable with either side when the projects are separated:
- it imports access control only through its front door, and otherwise only the persistence layer
  and SQLAlchemy; never a feature, the agent or the web layer;
- outside the package, code imports `app.services.scoped_data` itself, never a submodule;
- every table it classifies really has the customer column and the site column it names.
"""

import ast
from pathlib import Path

from app.services.scoped_data import registry

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "app/services/scoped_data/"
FACADE = "app.services.scoped_data"
ALLOWED = ("app.services.scoped_data", "app.services.access", "app.persistence.models")


def _imports(path: Path) -> set[str]:
    """Every `app...` module a file imports, by `from` or `import`.

    (a file with `from app.services import access`) -> {"app.services.access"}
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("app"):
            found.update({node.module} if node.module != "app.services"
                         else {f"app.services.{a.name}" for a in node.names})
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names if a.name.startswith("app"))
    return found


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def test_the_package_depends_only_on_access_control_and_the_data_models():
    offenders = sorted(
        (_rel(p), m)
        for p in (ROOT / PACKAGE).rglob("*.py")
        for m in _imports(p)
        if not any(m == a or m.startswith(a + ".") for a in ALLOWED)
    )
    assert offenders == []


def test_access_control_is_reached_only_through_its_front_door():
    offenders = sorted(
        (_rel(p), m)
        for p in (ROOT / PACKAGE).rglob("*.py")
        for m in _imports(p)
        if m.startswith("app.services.access.")
    )
    assert offenders == []


def test_outside_code_uses_only_the_front_door():
    sources = [*(ROOT / "app").rglob("*.py"), *(ROOT / "scripts").rglob("*.py")]
    offenders = sorted(
        (_rel(p), m)
        for p in sources
        if not _rel(p).startswith(PACKAGE)
        for m in _imports(p)
        if m.startswith(FACADE + ".")
    )
    assert offenders == []


def test_every_classified_table_has_the_columns_it_names():
    for entry in registry.SITE_LIMITED:
        columns = entry.model.__table__.columns
        assert "customer_code" in columns, entry.model.__tablename__
        assert entry.site_column in columns, entry.model.__tablename__
