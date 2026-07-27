# ADR-0002: Durable PostgreSQL jobs before a message broker

**Status:** Proposed

**Date:** 2026-07-27

## Context

The current system already coordinates workers through PostgreSQL.
The initial target is one host with a path to several hosts.

## Decision

Implement leased, idempotent PostgreSQL job queues using bounded `FOR UPDATE SKIP LOCKED` claims.
Defer Kafka or another broker until measurements show that queue throughput, replay, or fan-out requires it.

## Consequences

- Initial operations and correctness remain simple.
- Jobs survive worker restarts and can scale across hosts.
- Lease recovery, heartbeats, retry limits, and dead-letter handling must be implemented carefully.
- A future broker adoption must preserve the same job and event contracts.

