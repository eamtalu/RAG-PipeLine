# ADR-0006: Separate process roles on one host first

**Status:** Proposed

**Date:** 2026-07-27

## Context

Training, ingestion, stitching, serving, scoring, and remediation have different resource and failure characteristics.
Placing them in one event loop or one singleton worker prevents safe scaling.

## Decision

Run API, ingestion, stitching, feature/scoring, training, archive, notification, and action-execution roles as separately supervised processes or containers on one host.
Use durable shared state so each role can later move to another host.

## Consequences

- Resource limits and failures are isolated.
- Multi-host evolution does not change workflow contracts.
- Deployment contains more service units or containers.
- Operational health and queue observability must cover every role.

