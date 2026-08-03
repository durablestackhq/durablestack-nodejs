# DurableStack Node.js Architecture

## Goals

- Match DurableStack runtime semantics used by .NET for run lifecycle, leasing, retries, recurring jobs, and retention.
- Keep external contract compatibility for telemetry and runtime-control payloads.
- Start with an explicit-registration, in-memory-first foundation.

## Phase 0 Delivered Here

- Canonical status/event/command constants.
- Core runtime/store interfaces.
- Option model and normalization.
- External payload validators for telemetry and runtime-control contract shapes.

## Phase 1 Delivered Here

- In-memory store implementing run lifecycle, recurring schedule materialization, lease fencing, and retention pruning.
- Runtime processor loop that claims due runs, executes handlers, applies retry behavior, and emits events.
- Runtime host with `start`/`stop` and API surfaces for client/admin/query.

## Phase 2 Delivered Here

- PostgreSQL provider structure and contracts:
  - table naming resolver with prefix handling,
  - baseline migration for jobs/runs/migrations/runtime-command receipts,
  - Postgres `DurableJobStore` implementation covering run, recurring, retention, and runtime-command receipt APIs.
- Postgres-focused validation:
  - integration tests for migration + enqueue/claim/succeed flow,
  - contract tests for lease fencing, lease reclaim, recurring slot race single-winner behavior, and runtime-command lease contention.
- CI coverage:
  - dedicated Postgres-enabled CI job executes Postgres tests with a service container.

## Design Notes

- Job registration is explicit (`registerJob`) for initial implementation.
- Recurring schedules use IANA time zones and 5-field or 6-field cron expressions.
- Completion writes are fenced to current lease owner to prevent stale workers from overwriting state.
- Retention only prunes terminal runs (`succeeded`, `failed`).

## Deferred From Initial Build

- Module/assembly-style job auto-discovery.
- Durable SQL providers beyond Postgres (MySQL/SQL Server/SQLite).
- Hosted ingestion transport and runtime-control sync worker.
- Framework adapters and convenience bundle package.
