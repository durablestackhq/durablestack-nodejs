# DurableStack Node.js Architecture (Phase 0-1)

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

## Design Notes

- Job registration is explicit (`registerJob`) for initial implementation.
- Recurring schedules use IANA time zones and 5-field or 6-field cron expressions.
- Completion writes are fenced to current lease owner to prevent stale workers from overwriting state.
- Retention only prunes terminal runs (`succeeded`, `failed`).

## Deferred From Initial Build

- Module/assembly-style job auto-discovery.
- Durable SQL providers (Postgres/MySQL/SQL Server/SQLite) and migration locking.
- Hosted ingestion transport and runtime-control sync worker.
- Framework adapters and convenience bundle package.
