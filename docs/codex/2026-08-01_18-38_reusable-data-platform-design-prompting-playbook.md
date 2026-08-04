# Reusable data-platform design prompting playbook

**Created:** 2026-08-01 18:38 BST  
**Purpose:** Reproduce the documentation-first process used to design the MNP log PostgreSQL architecture in another project.  
**Audience:** Project owners, architects, data engineers, and developers working with an AI coding assistant.

## What this playbook produces

This playbook guides an assistant from repository discovery to a reviewable low-level data design without authorizing implementation.
It is intended to produce:

- A verified description of the current implementation.
- A timestamped current-behavior and data-loss assessment.
- A low-level PostgreSQL design with schemas, keys, constraints, indexes, retention, and operational behavior.
- Data-flow and grouped ER diagrams that a novice developer can follow.
- A comparison of the existing and proposed designs.
- Markdown source and, when requested, a polished HTML visualization.
- Explicit decisions, rejected alternatives, assumptions, risks, and unresolved questions.

## How to use this playbook

Run the phases in order.
Review each major output before requesting the next phase.
Replace placeholders such as `<PROJECT_NAME>` and `<DOMAIN>` with values from the new project.
Do not paste every prompt at once unless the repository and requirements are already well understood.

Useful placeholders include:

| Placeholder | Meaning | Example |
|---|---|---|
| `<PROJECT_NAME>` | Project or platform name | Operations Intelligence Platform |
| `<DOMAIN>` | The bounded area being designed | MNP log ingestion |
| `<CURRENT_ER_PATH>` | Existing ER document | `docs/database-er-diagram.md` |
| `<EXISTING_ARCHITECTURE_PATH>` | Existing architecture document | `docs/data-architecture.html` |
| `<OUTPUT_DIRECTORY>` | Location for generated documents | `docs/codex/` |
| `<ROWS_PER_DAY>` | Expected daily ingest volume | 1 million or more |
| `<HOT_RETENTION>` | Queryable database retention | 60 days |
| `<COLD_FORMAT>` | Long-term verified format | Parquet |

## Phase 0: Set documentation-only boundaries

Use this prompt before the assistant inspects or changes anything:

```text
Work on architecture documents only.
Do not modify application code, ORM models, migrations, tests, deployment configuration, or the maintained database ER diagram.
Inspecting code and configuration is allowed because the proposed design must be based on repository facts.
Clearly label current facts, proposed decisions, assumptions, and unresolved questions.
Stop after producing the requested documents so that I can review and approve the architecture before implementation.
Preserve unrelated working-tree changes.
```

This boundary is important because an architecture request is not authorization to implement the architecture.

## Phase 1: Establish the broader objective

Start with the business and platform outcome rather than immediately discussing tables.

```text
Review this repository and the existing architecture document at <EXISTING_ARCHITECTURE_PATH>.
I want a scalable data processing and machine learning platform that supports exploratory analysis, log exploration, model training, model evaluation and scanning, predictions, forecasting, alerting, and agentic or copilot applications.

Before designing it, inspect the repository and ask me the clarification questions that materially affect the architecture.
Do not implement anything.
```

Ask or answer these discovery questions:

1. Which data sources and formats are in scope?
2. Is the data labeled, partially labeled, or unlabeled?
3. What are the retention and deletion requirements?
4. Is deployment single-host, multi-host, cloud, or hybrid?
5. Is processing batch, near-real-time, streaming, or a combination?
6. What predictions or indicators must the platform produce?
7. What actions may follow a prediction or alert?
8. Which actions are recommendation-only, human-approved, or automatically executable?
9. What is the current and expected peak data volume?
10. Which areas must be designed now, and which must remain future work?

## Phase 2: Confirm measurable requirements

Turn general goals into explicit constraints before selecting technologies or schemas.

```text
Convert my answers into a requirements and constraints table.
Separate confirmed facts from assumptions.
Include ingest volume, peak rate, query latency, availability, data retention, recovery objectives, model freshness, security, tenancy, deployment topology, and automation permissions.
Ask me about any missing requirement that would materially change the design.
Do not choose an architecture until these requirements are confirmed.
```

A useful confirmed requirement set might look like this:

- Volume is at least `<ROWS_PER_DAY>` rows per day and may increase.
- Real-time processing is preferred, with batch processing also supported.
- Deployment begins on one self-hosted machine but must allow multiple hosts later.
- Data has labels or a defined route to create and govern labels.
- Predictions support demand forecasting and early warning of operational problems.
- Downstream actions may open incidents, notify teams, retrain models, or initiate remediation.
- The default autonomy policy recommends actions, requires human approval for high-risk actions, and permits only pre-approved low-risk automation.
- Hot data remains in PostgreSQL for `<HOT_RETENTION>`.
- Older data moves to verified `<COLD_FORMAT>` storage.

## Phase 3: Inspect the current implementation

The assistant must verify the current system from source before proposing a replacement.

```text
Inspect the current implementation for <DOMAIN>.
Read the existing ER diagram, ORM models, migrations, ingestion services, transaction-stitching logic, retention jobs, APIs, and relevant tests.

Document:

1. Every current table involved and its role.
2. Primary keys, foreign keys, unique constraints, indexes, and soft references.
3. The complete write path from source data to persisted rows.
4. How transaction stitching or regrouping currently works.
5. Every UPDATE, DELETE, truncate, replacement, or recreate operation.
6. Retry, deduplication, idempotency, failure, and recovery behavior.
7. The queries and access patterns the design must support.
8. Any mismatch between documentation and implementation.

Cite repository file paths and line numbers for important claims.
Do not infer behavior that can be checked in the repository.
```

## Phase 4: Investigate deletion and possible data loss

This question should be resolved before making immutability decisions.

```text
When the current workflow deletes and recreates log rows during transaction stitching, is any information lost?
Trace the behavior end to end from the original source through parsing, deletion, recreation, and recovery.

Explain separately:

- Whether the original source remains available.
- Whether every deleted database field can be reproduced exactly.
- Whether database identity, audit history, timestamps, annotations, or downstream references change.
- What happens if the process fails between deletion and recreation.
- Whether concurrent readers can observe an incomplete state.
- Whether retention or source rotation can make recovery impossible.

Classify the conclusion as no loss, recoverable loss, metadata or lineage loss, or irreversible loss.
Support the conclusion with repository evidence and state any uncertainty.
Do not change code.
```

The key distinction is between recoverability and immutability.
Data can sometimes be reconstructed from source files while the database still loses row identity, audit lineage, derived state, or a consistent intermediate view.

## Phase 5: Preserve a timestamped current-behavior baseline

After the behavior has been verified, record it so future changes can be compared with a known point in time.

```text
Create a timestamped Markdown document under <OUTPUT_DIRECTORY> describing the verified current behavior of <DOMAIN>.

Include:

- Repository revision and observation timestamp.
- Scope and evidence inspected.
- Current data flow.
- Current table responsibilities.
- Exact deletion and recreation behavior.
- Data-loss and recoverability assessment.
- Failure windows and concurrency risks.
- Conditions that could invalidate the conclusion later.
- A short checklist for comparing future behavior with this baseline.

This is a current-state record, not a proposed design.
Do not modify code.
```

## Phase 6: Make the critical design decisions

Ask these questions before designing the low-level schema.

### 6.1 Raw-data immutability

```text
Should successfully ingested raw events be strictly immutable, append-only with exceptional corrections, or mutable?
Explain the consequences for auditability, deduplication, storage, corrections, and transaction regrouping.
```

The choice used in the MNP design was strictly immutable raw events.
Transaction discovery therefore changes relationships and derived projections, not the original event records.

### 6.2 Transaction-assignment history

```text
Explain the alternatives for recording which transaction owns each event.
Compare:

1. Keeping only the current assignment.
2. Keeping a full temporal history of every assignment.
3. Keeping the current assignment plus a lightweight append-only change log.

For each option, explain query simplicity, auditability, storage, correction behavior, and operational complexity.
Recommend one based on the stated requirements, but wait for my selection.
```

The choice used in the MNP design was current assignment plus an append-only change log.
It preserves a simple current view while retaining evidence of regrouping changes.

### 6.3 Hot and cold retention

```text
Compare PostgreSQL-only retention, object-storage archive, and a hot PostgreSQL plus verified Parquet design.
Explain query performance, recovery, schema evolution, verification, deletion safety, and operating implications.
```

The choice used in the MNP design was 60 days of hot PostgreSQL data, followed by verified Parquet storage.
Hot deletion is permitted only after archive verification and recorded evidence.

### 6.4 Domain ownership

```text
Decide whether <DOMAIN> should use shared generic ingestion tables or domain-owned tables.
Compare coupling, naming clarity, independent evolution, operational ownership, and cross-domain analytics.
Record the chosen option and rejected alternatives.
```

The choice used in the MNP design was domain-owned MNP ingestion tables.

### 6.5 Scope boundary

```text
Focus the low-level design only on <DOMAIN>.
Show external systems and future consumers only as interfaces.
Do not design unrelated platform domains, machine learning internals, or agent internals in this document.
```

## Phase 7: Evaluate alternatives explicitly

```text
For every material design choice, document:

- The problem being decided.
- The viable options.
- The selected option.
- Why it was selected.
- Why each alternative was not selected.
- Consequences and tradeoffs.
- Conditions that would cause the decision to be revisited.

Prefer quality, simplicity, robustness, scalability, and long-term maintainability over short-term development cost.
```

At minimum, evaluate:

- Mutable rows versus immutable raw events.
- In-place transaction fields versus an assignment table.
- Current-only assignment versus full history versus current plus change log.
- PostgreSQL-only retention versus verified archival.
- Physical deletion versus tombstones or logical retirement.
- Natural keys versus surrogate keys.
- Database-enforced foreign keys versus soft references.
- Time-based partitioning choices.
- Synchronous work versus tracked background runs.
- Exactly-once claims versus idempotent at-least-once processing.

## Phase 8: Request the low-level PostgreSQL design

```text
Create a low-level PostgreSQL design for <DOMAIN> based on the verified current implementation and confirmed decisions.
Write it under <OUTPUT_DIRECTORY> as a timestamped Markdown document.
This is a proposal only and must not change the application schema or code.

Include:

1. Purpose, scope, goals, and non-goals.
2. Confirmed requirements and assumptions.
3. Current implementation summary.
4. Design principles and invariants.
5. Schema or namespace organization.
6. A table inventory grouped by responsibility.
7. Detailed columns, data types, nullability, defaults, and semantics.
8. Primary keys, foreign keys, unique constraints, checks, and delete behavior.
9. Indexes mapped to exact query patterns.
10. Tenant isolation and partition keys.
11. Partitioning and partition lifecycle.
12. Immutable raw-event storage.
13. Source-file and ingestion-run lineage.
14. Parser version and replay behavior.
15. Deduplication and idempotency keys.
16. Transaction candidates, current assignments, and assignment history.
17. Stitching and restitching state transitions.
18. The atomic database transaction boundary.
19. Concurrency control and locking.
20. Failure recovery and retry behavior.
21. Late, duplicate, malformed, and out-of-order events.
22. Hot retention and verified Parquet archival.
23. Archive manifests, checksums, row counts, and deletion gates.
24. Operational queries and bounded pagination.
25. Expected scale and capacity assumptions.
26. Security, tenancy, and audit requirements.
27. Metrics, logs, alerts, and service-level indicators.
28. Migration and rollback strategy at a design level.
29. Risks, unresolved questions, and decision gates.
30. Alternatives considered and why the selected options won.
31. A clear statement that implementation requires separate approval.

Cross-check every current-state statement against repository evidence.
Clearly mark proposed tables so readers do not confuse them with existing tables.
```

## Phase 9: Add a beginner-friendly data-flow diagram

```text
Add a data-flow diagram to the low-level design and explain it for a novice software developer.

Show these stages where applicable:

1. Source discovery.
2. Ingestion run creation.
3. File or object registration.
4. Parsing and validation.
5. Immutable raw-event insertion.
6. Duplicate handling and quarantine.
7. Transaction candidate discovery.
8. Current assignment update.
9. Assignment change-log append.
10. Read models and APIs.
11. Archive creation and verification.
12. Hot-data deletion after verification.

For every arrow, say what moves, who writes it, and what happens on failure.
Use plain language and define technical terms when first used.
Also generate a self-contained, polished HTML version that presents the same information.
```

## Phase 10: Group the ER diagram by responsibility

```text
Create a new grouped ER diagram because a flat table diagram is difficult to understand.

Group the proposed tables into clearly labelled areas such as:

- Source and collection.
- Immutable facts.
- Transaction stitching.
- Errors and quarantine.
- Archive and retention.
- Operations and audit.

For each group, explain:

1. Its responsibility in one sentence.
2. Why each table exists.
3. Which tables are written first.
4. Which relationships are database-enforced foreign keys.
5. Which relationships are logical or soft references.
6. What a novice developer should query for common tasks.

Use visual boundaries, a legend, and a simple reading order.
Generate both Markdown and a polished HTML visualization.
Keep table and relationship names consistent with the low-level design.
```

## Phase 11: Compare existing and proposed designs

```text
Add a current-versus-proposed comparison to the low-level design.

For every relevant current table, show:

- What it does today.
- Which workflows read or write it.
- What is missing or risky.
- Whether the proposed design retains, replaces, splits, or retires it.
- Which proposed table or tables take over its responsibilities.
- What problem the change solves.
- What migration or compatibility concern remains.

Also compare behavior across these concerns:

- Raw-event mutability.
- Transaction stitching.
- Row identity.
- Audit and lineage.
- Deduplication and replay.
- Failure atomicity.
- Concurrency visibility.
- Tenant isolation.
- Indexing and query patterns.
- Hot retention and cold archive.
- Operational observability.
- Read APIs and downstream consumers.

Separate verified current facts from proposed behavior.
Do not claim that proposed tables already exist.
```

## Phase 12: Review before implementation

Use this prompt after the document package is complete:

```text
Perform a design review only.
Do not implement anything.

Check the documents for:

- Contradictions between diagrams, table definitions, and prose.
- Missing foreign keys, constraints, indexes, or delete rules.
- Unbounded queries or expensive hot-path counts.
- Blocking request paths for long-running work.
- Unsafe archive deletion conditions.
- Broken retry, replay, or idempotency behavior.
- Ambiguous ownership between current and proposed tables.
- Failure windows that can lose data or expose partial state.
- Claims not supported by repository evidence.
- Terminology that would confuse a novice reader.

Return findings by severity, cite the relevant document sections, and list all questions that require owner decisions.
```

## Phase 13: Validate the documentation package

```text
Validate the generated documentation without changing application code.

Confirm that:

- Every Markdown link resolves.
- Every Mermaid block has balanced entity braces and valid relationship syntax.
- Table names and relationships match across all diagrams and prose.
- Current and proposed states are visually and verbally distinct.
- Every important design choice has alternatives and rationale.
- The HTML is self-contained and readable at desktop and mobile widths.
- The HTML remains understandable if optional diagram scripts do not load.
- No em dash character is used.
- Every full Markdown prose sentence is on its own physical line.
- The repository diff contains documentation changes only.

Report the validation commands and results.
```

## Compact master prompt

Use this only when requirements are already known and the assistant can inspect the full repository.

```text
Create a documentation-only low-level data architecture package for <DOMAIN> in <PROJECT_NAME>.

First inspect <CURRENT_ER_PATH>, <EXISTING_ARCHITECTURE_PATH>, ORM models, migrations, ingestion and stitching services, APIs, retention jobs, and tests.
Fact-check current behavior from source and cite important paths and lines.
Do not modify application code, migrations, configuration, tests, or the maintained database ER artifact.

Before designing, ask me to confirm any unresolved decision that could materially change the result, including immutability, assignment history, retention, tenancy, partitioning, volume, latency, and scope.

Confirmed starting decisions are:

- Daily volume: <ROWS_PER_DAY>.
- Deployment: <DEPLOYMENT_TOPOLOGY>.
- Processing: <PROCESSING_MODE>.
- Raw-event policy: <IMMUTABILITY_POLICY>.
- Assignment-history policy: <ASSIGNMENT_HISTORY_POLICY>.
- Hot retention: <HOT_RETENTION>.
- Cold storage: verified <COLD_FORMAT>.
- Scope: <DOMAIN> only.

Produce under <OUTPUT_DIRECTORY>:

1. A timestamped current-behavior and data-loss baseline.
2. A timestamped low-level PostgreSQL design.
3. Detailed table definitions, constraints, indexes, partitions, lineage, idempotency, stitching, retention, archive verification, concurrency, and recovery behavior.
4. A beginner-friendly data-flow diagram.
5. A grouped ER diagram with table roles and a reading guide.
6. A current-versus-proposed table and behavior comparison.
7. Alternatives considered, the selected decisions, rationale, consequences, and revisit conditions.
8. Polished self-contained HTML versions of the visual documents.
9. A validation report confirming internal consistency and documentation-only scope.

Clearly distinguish verified current facts, proposed decisions, assumptions, and open questions.
Prefer quality, simplicity, robustness, scalability, and long-term maintainability over short-term development cost.
Stop after documentation and wait for explicit approval before implementation.
```

## Expected output package

A complete package usually contains:

```text
docs/codex/
  README.md
  <timestamp>_current-<domain>-behavior.md
  <timestamp>_<domain>-postgresql-low-level-design.md
  <timestamp>_<domain>-postgresql-low-level-design.html
  <timestamp>_<domain>-grouped-er-diagram.md
  <timestamp>_<domain>-grouped-er-diagram.html
  adrs/
    <decision-records>.md
```

The exact filenames may follow the target repository's conventions.
The index should state that the architecture is proposed and that implementation is not yet authorized.

## Quality checklist

Before accepting the package, verify the following:

- [ ] The assistant inspected the repository instead of relying only on the initial architecture document.
- [ ] Current behavior has evidence and a timestamp.
- [ ] Data loss is analyzed separately from source recoverability.
- [ ] Raw facts, derived state, and operational metadata have distinct ownership.
- [ ] Every table has a clear role and lifecycle.
- [ ] Every relationship states whether it is enforced or logical.
- [ ] Every index maps to an actual access pattern.
- [ ] Tenant-scoped indexes lead with the tenant key where appropriate.
- [ ] Large reads are bounded and paginated.
- [ ] Long-running operations use tracked background jobs rather than blocking requests.
- [ ] The stitching workflow defines atomicity, retries, and concurrency behavior.
- [ ] The archive workflow verifies content before deleting hot data.
- [ ] The retention policy explains legal holds, failed archives, and replay.
- [ ] The diagrams, prose, and table definitions agree.
- [ ] The existing-versus-proposed comparison maps every changed responsibility.
- [ ] Alternatives and rejected options are recorded.
- [ ] Novice explanations identify what to read and query first.
- [ ] Open questions and approval gates are visible.
- [ ] No application or schema files changed.

## Common prompting mistakes

Avoid asking only for a "scalable architecture" without measurable volume, latency, retention, and deployment constraints.
Avoid treating an existing design document as proof of current behavior when source code can be inspected.
Avoid asking for an ER diagram without first defining table responsibilities and lifecycle.
Avoid saying data is not lost merely because the source file still exists.
Avoid mixing current and proposed tables without strong labels.
Avoid choosing full temporal history by default when a simpler current-state table plus change log satisfies the audit requirement.
Avoid deleting hot data merely because an archive file was written.
Require verification evidence such as checksums, row counts, manifests, and a recorded deletion gate.
Avoid asking for implementation in the same approval step as architecture review.

## Final handoff question

End the documentation phase with this question:

```text
The documentation package is complete and no implementation changes have been made.
Which decisions or diagrams would you like to revise before we create a separately approved implementation plan?
```
