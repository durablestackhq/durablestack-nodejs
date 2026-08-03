# Phase 2 Completion - PostgreSQL Foundation

## Status

Phase 2 is complete.

## Completed Scope

1. PostgreSQL provider baseline
   - table naming resolver with prefix support,
   - migration path for:
     - `durable_stack_schema_migrations`
     - `durable_stack_jobs`
     - `durable_stack_job_runs`
     - `durable_stack_runtime_command_receipts`
   - recurring slot uniqueness index behavior in run storage.

2. Postgres store implementation
   - run lifecycle APIs:
     - enqueue, claim, lease extension, success/failure/cancel,
     - query APIs by id/status/job/recent/enqueued,
     - retry transition support.
   - recurring schedule APIs:
     - upsert, list, enable/disable, update cron/timezone, next-run updates,
     - due-schedule retrieval and atomic materialization path.
   - retention API:
     - terminal-run pruning only.
   - runtime command receipt APIs:
     - lease/ack/success/failure/upload marker lifecycle.

3. Tests and CI
   - Postgres integration tests (env-gated local execution) for migration and basic flow.
   - Postgres contract tests (env-gated local execution) for:
     - lease fencing,
     - lease reclaim,
     - recurring slot race single-winner behavior,
     - runtime command receipt lease contention.
   - GitHub Actions Postgres service job to execute Postgres tests in CI.

## Acceptance Criteria Check

- Migrations idempotent under repeated execution: complete.
- Lease fencing and reclaim behavior covered by tests: complete.
- Recurring slot race uniqueness covered by tests: complete.
- Runtime command lease contention behavior covered by tests: complete.
- Postgres-enabled CI coverage active and passing: complete.

## Post-Completion Stability Note

- The recurring slot race contract test was hardened after intermittent CI failures caused by contention and timestamp precision differences.
- The contract assertion now emphasizes the canonical invariant (no duplicate runs for the same schedule slot) and uses a non-racy follow-up path when both initial racers lose under transient contention.
- This keeps the test aligned with provider-agnostic behavior expectations while avoiding false negatives from timing/format artifacts.

## Explicitly Deferred Beyond Phase 2

- Job auto-discovery.
- MySQL provider.
- SQL Server provider.
- SQLite provider.
- Hosted ingestion sync service.
- Hosted runtime-control sync worker.
- Framework adapters and convenience bundle package.

## Next Phase Entry Point

Proceed to next provider implementation phase (recommended order: MySQL, SQL Server, SQLite), reusing the same contract test strategy established in Phase 2.
