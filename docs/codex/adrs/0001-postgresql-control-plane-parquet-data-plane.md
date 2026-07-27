# ADR-0001: PostgreSQL control plane and Parquet analytical data plane

**Status:** Proposed

**Date:** 2026-07-27

## Context

The platform begins on one self-hosted machine, must retain 90 to 180 million raw rows at the stated rate, and must later scale across hosts.
Operational APIs need indexed row access while model training needs large columnar scans.

## Decision

Use PostgreSQL for operational records, durable job state, model metadata, predictions, incidents, and audit references.
Use Parquet behind an object-store interface for historical analytics, training datasets, and feature exports.
Use DuckDB initially to query Parquet.

## Consequences

- Serving and training workloads can be isolated.
- Artifacts remain portable to shared object storage and other analytical engines.
- Archive verification and dataset manifests become mandatory.
- Cross-store consistency is asynchronous and must use explicit watermarks and checksums.

