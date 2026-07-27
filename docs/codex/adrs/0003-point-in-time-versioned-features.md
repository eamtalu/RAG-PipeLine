# ADR-0003: Point-in-time versioned feature contracts

**Status:** Proposed

**Date:** 2026-07-27

## Context

Failure prediction can accidentally use final status, error text, response content, or other information unavailable at prediction time.
That leakage would produce impressive offline metrics and poor real-world behavior.

## Decision

Define separate versioned feature contracts for transaction start, live transaction state, final analysis, and demand buckets.
Reconstruct offline features using event time and store a feature version and snapshot fingerprint with every prediction.

## Consequences

- Offline and online behavior can be compared reliably.
- Dataset construction becomes more explicit.
- Feature changes require new versions rather than silent mutation.
- Leakage tests become release gates.

