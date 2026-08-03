# Phase 5 Kickoff - Provider Parity (MySQL First)

## Status

In progress.

## Initial Deliverables Added

1. MySQL provider scaffolding
   - options model (`src/mysql/types.ts`),
   - table naming resolver (`src/mysql/table-names.ts`),
   - baseline migrator (`src/mysql/migrator.ts`),
   - runtime bootstrap (`src/mysql/runtime.ts`),
   - store class scaffold (`src/mysql/store.ts`).

2. Public exports
   - MySQL modules exported from `src/index.ts`.

3. Early test coverage
   - table prefix/name contract check,
   - migration table creation check (env-gated by `DURABLESTACK_TEST_MYSQL`),
   - scaffold guard asserting store methods are not yet implemented.

4. First functional MySQL store APIs
   - implemented run lifecycle/query subset:
     - `enqueue`, `claimDueRuns`, `markSucceeded`, `cancelRun`, `markFailed`,
     - `getRun`, `getRecentRuns`, `getRuns`, `getRunsByJobName`, `getRunsByStatus`, `getEnqueuedRuns`,
     - `tryEnqueueIfNoActiveRun`, `extendLease`, `pruneHistoricalRuns`.
   - remaining recurring/runtime-command receipt APIs are still pending.

5. MySQL contract test kickoff
   - env-gated contract tests added for:
     - lease fencing,
     - lease reclaim,
     - no-active-run enqueue dedupe,
     - recurring slot race single-winner behavior,
     - runtime-command lease single-winner contention.

8. CI readiness for MySQL tests
   - Added dedicated MySQL service job in GitHub Actions.
   - Added failure artifact/log capture for MySQL env-gated test runs.

6. P0 parity course-corrections applied
   - processor now applies recurring registration sync semantics for existing jobs/orphans,
   - processor now aborts local job execution when lease extension fails,
   - runtime-command receipt upload selection now excludes leased receipts and uploads only acknowledged/succeeded/failed states.

7. P1 and P2 parity course-corrections applied
   - retry behavior now supports per-registration mode (`fixed` / `backoff`) and per-registration initial delay,
   - default durable retention moved to 24h to align with durable provider expectations,
   - MySQL table prefix casing now preserves caller-provided prefix,
   - parity tests added for retry behavior, retention default, table prefix casing, and enqueued-run semantics.

## Next Implementation Steps

1. Implement MySQL store run lifecycle APIs.
2. Implement recurring APIs and atomic slot materialization semantics.
3. Implement runtime-command receipt lease/ack/completion APIs.
4. Add MySQL contract tests mirroring Postgres suite.
5. Add MySQL CI service job for env-enabled provider tests.
