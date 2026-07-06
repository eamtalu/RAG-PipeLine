# Project Memory

Instructions for Claude when working in this repository.

## Keep the ER diagram in sync with the schema

The file `docs/database-er-diagram.md` is a human-facing ER diagram of the entire database.
It is generated from the ORM models and must be treated as a maintained artifact, not a one-off.

**Whenever you add, remove, or modify anything about the database schema, you must update `docs/database-er-diagram.md` in the same change.**

The database schema is defined in these places:

- SQLAlchemy 2.0 ORM models under `app/persistence/models/` (registered in `app/persistence/models/__init__.py`).
- The base declarative class in `app/config/database.py`.
- The raw-SQL `embeddings` table (pgvector) created in `app/persistence/vectorstore/pgvector.py`.
- Alembic migrations under `alembic/versions/`.

A schema change that requires updating the diagram includes any of the following:

- Adding, renaming, or dropping a table or model.
- Adding, renaming, retyping, or dropping a column.
- Adding, changing, or removing a primary key, foreign key, unique constraint, or index that the diagram documents.
- Changing an `ON DELETE` behavior (CASCADE / SET NULL).
- Changing an enum's allowed values where the diagram lists them.
- Turning a soft reference into an enforced foreign key, or vice versa.
- Adding or removing a subsystem.

### How to update the diagram

Read `docs/database-er-diagram.md` first, then apply the change in the right place so the document stays internally consistent.

- Reflect the change in the relevant per-subsystem `erDiagram` block (columns, PK/FK markers, cardinality).
- Update the master overview diagram and the tenant-partitioning diagram if a table or an enforced foreign key was added or removed.
- Update the "Full relationship reference" tables at the bottom (enforced foreign keys and soft references).
- Preserve the document's core convention: solid lines (`--`) are database-enforced foreign keys, and dashed lines (`..`) are logical "soft" references such as the `customer_code` tenant key.
- If you add a new subsystem, add both a short prose description and its own `erDiagram` block, matching the existing structure.

### Verify after editing the diagram

- Confirm every `erDiagram` block still parses: braces balanced inside each entity block, and every relationship uses a valid crow's-foot cardinality token (for example `||--o{` for enforced, `||..o{` for soft).
- Cross-check the edited tables and relationships against the actual model files so nothing is invented or missed.
- The diagrams use Mermaid, which renders on GitHub and in VS Code Markdown preview; a PNG/SVG can be exported by pasting a block into the Mermaid live editor.

## Documentation conventions

- Do not use the em dash. Use a plain dash instead.
- In Markdown prose, put each full sentence on its own physical line while keeping normal Markdown structure.
