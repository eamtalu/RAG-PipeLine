# Phased implementation roadmap

**Status:** Proposed for review

No phase begins until the preceding review gate is accepted.

## Phase 0 - Measurement, labels, and safety foundations

### Deliverables

- Measure row sizes, index sizes, ingestion bursts, query latency, and disk throughput.
- Profile label counts for success, soft, error, and incomplete outcomes.
- Define failure, duration, demand, anomaly, and root-cause product metrics.
- Define the root-cause taxonomy and operator feedback workflow.
- Add authentication, tenant membership, RBAC, and service identities to the implementation plan.
- Establish data and action threat models.

### Exit gate

- Capacity worksheet is approved.
- Label audit is approved.
- Initial SLOs and alert budget are approved.
- Security design is approved.

## Phase 1 - Scalable data foundation

### Deliverables

- Design and E2E-test append-only raw entries and transaction assignments.
- Partition hot log tables by time.
- Add automated partition creation and monitoring.
- Build verified Parquet export and archive manifests.
- Add object-store abstraction and retention jobs.
- Remove global unbounded regroup and count scans.

### Exit gate

- Replay, rotation, late-arrival, month-boundary, deletion, and recovery E2E tests pass.
- `EXPLAIN` proves partition pruning and matching tenant-first indexes.
- Sustained and burst tests exceed 1 million rows per day equivalent.
- API health remains responsive during backfill and archive operations.

## Phase 2 - Durable ML control plane

### Deliverables

- Durable leased job queue.
- Training-run tracking.
- Feature and dataset versioning.
- Model registry and checksummed artifact storage.
- Prediction store and model lifecycle APIs.
- Dedicated training and scoring worker entry points.

### Exit gate

- Crash and lease-recovery tests pass.
- Cross-tenant API and worker tests pass.
- Duplicate requests produce one logical run or prediction.
- Model rollback is demonstrated.

## Phase 3 - Failure-risk vertical slice

### Deliverables

- `transaction_start_v1` and `transaction_live_v1` feature contracts.
- Interpretable baseline model.
- Controlled model tournament.
- Chronological evaluation and calibration.
- Shadow scoring of newly changed transactions.
- Prediction dashboards and drift telemetry.

### Exit gate

- No leakage is detected.
- Baseline and candidate results meet the approved metric gates.
- Shadow p95 and p99 scoring latency meet the budget.
- Alert volume stays within the approved budget.

## Phase 4 - Incidents and notifications

### Deliverables

- Deduplicated incident lifecycle.
- Prediction-to-incident policy.
- Integration with the existing notification outbox.
- Operator acknowledgment, resolution, and root-cause feedback.

### Exit gate

- Repeated scoring produces one incident and bounded notifications.
- Notification outages recover without event loss.
- Operator feedback creates versioned labels.

## Phase 5 - Forecasting, duration, anomaly, and root cause

### Deliverables

- Duration regression pipeline.
- Demand-bucket feature pipeline and forecasting models.
- Streaming and retrospective anomaly detectors.
- Root-cause classifier after sufficient confirmed labels exist.
- Per-use-case monitoring and retraining policies.

### Exit gate

- Each use case beats its approved simple baseline.
- Forecast and anomaly backtests cover seasonal and incident periods.
- Root-cause results meet minimum class-support and precision requirements.

## Phase 6 - Governed copilot actions

### Deliverables

- Separate action proposal service.
- Versioned policy engine.
- Approval records with expiry and exact scope.
- Append-only audit events.
- Isolated allowlisted executors.
- Kill switch, cooldowns, blast-radius limits, dry runs, and postconditions.

### Exit gate

- Authentication and RBAC are fully enforced.
- Unknown actions fail closed.
- High-risk actions cannot execute without valid approval.
- Kill-switch and expired-approval tests pass.
- Every execution can be reconstructed from audit records.

## Phase 7 - Multiple-host scale-out

### Deliverables

- Shared S3-compatible artifact storage.
- Independent worker pools on additional hosts.
- PostgreSQL connection and resource governance.
- Optional read replica for analytics and operational reads.
- Broker evaluation based on measured queue load.

### Exit gate

- Terminating any worker does not lose work.
- Two workers do not duplicate predictions or actions.
- A host failure preserves API and critical ingestion recovery objectives.

## Suggested delivery slices

| Slice | Outcome |
| --- | --- |
| A | Capacity facts, label audit, security design |
| B | Partitioned and archived data plane |
| C | Durable ML jobs, registry, and artifacts |
| D | Failure-risk model in shadow mode |
| E | Incidents, notifications, and feedback |
| F | Forecasting, duration, anomaly, and root cause |
| G | Approval-gated copilot actions |
| H | Multi-host deployment |

## Definition of done for every phase

- Architecture decision and threat model are current.
- Migrations have forward and recovery procedures.
- Data-heavy queries are bounded and index-proven.
- E2E tests exercise the user-visible workflow.
- Load tests use realistic sustained and burst data.
- Metrics, logs, alerts, and runbooks exist.
- Tenant isolation tests pass.
- Failure injection and restart recovery are demonstrated.
- Documentation and the maintained ER diagram are updated with the implementation.

