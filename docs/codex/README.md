# Scalable data, ML, and agent platform

This directory contains the proposed architecture package for review.
It does not represent an implemented database schema or deployed capability.

## Review order

1. Read [2026-07-28_12-10_current-log-regroup-data-deletion-behavior.md](2026-07-28_12-10_current-log-regroup-data-deletion-behavior.md) for the timestamped current-behavior baseline.
2. Read [2026-07-28_12-22_mnp-log-postgresql-low-level-design.md](2026-07-28_12-22_mnp-log-postgresql-low-level-design.md) for the proposed MNP-only PostgreSQL schema, ER diagrams, indexes, stitching transaction, and retention design.
3. Read [2026-07-28_15-49_mnp-log-grouped-er-diagram.md](2026-07-28_15-49_mnp-log-grouped-er-diagram.md) for the beginner-friendly grouped explanation of every proposed MNP table.
4. Open [2026-07-28_15-49_mnp-log-grouped-er-diagram.html](2026-07-28_15-49_mnp-log-grouped-er-diagram.html) for the interactive grouped ER view.
5. Open [2026-07-28_12-22_mnp-log-postgresql-low-level-design.html](2026-07-28_12-22_mnp-log-postgresql-low-level-design.html) for the simplified interactive MNP data-flow explanation.
6. Read [platform-architecture.md](platform-architecture.md) for the complete target design.
7. Read [implementation-roadmap.md](implementation-roadmap.md) for the delivery sequence and acceptance gates.
8. Review the decisions under [adrs/](adrs/).
9. Open [platform-architecture.html](platform-architecture.html) for the visual executive version.

## Current status

**Status:** Proposed for review

**Implementation authorized:** No

No application code, migration, operational configuration, or maintained database ER diagram is changed by this architecture package.

## Reusable playbooks

- [2026-08-01_18-38_reusable-data-platform-design-prompting-playbook.md](2026-08-01_18-38_reusable-data-platform-design-prompting-playbook.md) provides the reusable, documentation-first prompt sequence used to investigate current behavior, make data-design decisions, and produce low-level PostgreSQL diagrams and comparisons for another project.
