# ADR-0004: Model tournament and controlled promotion

**Status:** Proposed

**Date:** 2026-07-27

## Context

The platform must scan and compare models without automatically promoting an opaque or unstable candidate.

## Decision

Train a simple approved baseline and a bounded set of candidates against one frozen chronological dataset.
Register every candidate.
Require evaluation, shadow operation, and approval before production promotion.
Preserve the previous production model for rollback.

## Consequences

- Model selection is evidence-based and reproducible.
- Complex models must justify their operational cost.
- Promotion is slower but safer.
- Registry metadata and shadow telemetry are required.

