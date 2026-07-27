# ADR-0005: Separate read-only copilot and action control plane

**Status:** Proposed

**Date:** 2026-07-27

## Context

The existing log copilot is intentionally read-only.
The target platform also needs incidents, notifications, pipeline triggers, retraining, and controlled remediation.

## Decision

Keep investigation tools read-only.
Route every side effect through a durable action proposal, policy evaluation, optional human approval, isolated executor, and append-only audit trail.
Unknown actions fail closed.

## Consequences

- A compromised or mistaken model cannot directly operate infrastructure.
- Actions are inspectable and idempotent.
- Authentication and RBAC become release blockers for execution.
- Additional workflow and executor components are required.

