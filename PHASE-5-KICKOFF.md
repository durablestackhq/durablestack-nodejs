# Phase 5 Kickoff - Provider Parity (MySQL First)

## Status

In progress (MySQL and SQL Server provider semantics implemented; SQLite remaining for provider parity phase).

## Delivered So Far

1. MySQL provider scaffolding
   - options model (`src/mysql/types.ts`),
   - table naming resolver (`src/mysql/table-names.ts`),
   - baseline migrator (`src/mysql/migrator.ts`),
   - runtime bootstrap (`src/mysql/runtime.ts`),
   - store class scaffold (`src/mysql/store.ts`).

2. Public exports
   - MySQL modules exported from `src/index.ts`.

3. MySQL store implementation
   - run lifecycle/query APIs:
     - `enqueue`, `claimDueRuns`, `markSucceeded`, `cancelRun`, `markFailed`,
     - `getRun`, `getRecentRuns`, `getRuns`, `getRunsByJobName`, `getRunsByStatus`, `getEnqueuedRuns`,
     - `tryEnqueueIfNoActiveRun`, `extendLease`, `pruneHistoricalRuns`.
   - recurring APIs:
     - `upsertRecurringJob`, `getRecurringJobs`, `setRecurringJobEnabled`, `updateRecurringJobSchedule`,
     - `getDueRecurringJobs`, `updateRecurringNextRun`, `tryMaterializeRecurringRun`.
   - runtime-command receipt APIs:
     - lease, ack, success/failure, upload marker lifecycle.

5. MySQL contract test kickoff
   - env-gated contract tests added for:
     - lease fencing,
     - lease reclaim,
     - no-active-run enqueue dedupe,
     - recurring slot race single-winner behavior,
     - runtime-command lease single-winner contention.

4. MySQL contract/integration coverage
   - env-gated contract tests include:
     - lease fencing,
     - lease reclaim,
     - no-active-run enqueue dedupe,
     - recurring slot race single-winner behavior,
     - recurring fallback materialization under contention,
     - runtime-command lease single-winner contention,
     - runtime-command lease re-acquisition after expiry,
     - runtime-command receipt ack/success/upload lifecycle,
     - recurring schedule admin state updates.
   - env-gated integration tests include:
     - migration baseline tables,
     - enqueue -> claim -> succeed flow,
     - migration concurrency safety for same-prefix parallel calls,
     - migration idempotence under repeated execution.

5. CI readiness for MySQL tests
   - Added dedicated MySQL service job in GitHub Actions.
   - Added failure artifact/log capture for MySQL env-gated test runs.
    - Added MySQL CI timeout and lock/process diagnostics on failure.

6. SQL Server provider scaffold kickoff
   - Added SQL Server provider modules:
     - `src/sqlserver/types.ts`
     - `src/sqlserver/table-names.ts`
     - `src/sqlserver/migrator.ts`
     - `src/sqlserver/store.ts`
     - `src/sqlserver/runtime.ts`
   - Added SQL Server public exports in `src/index.ts`.
   - Added env-gated SQL Server scaffold tests for:
     - table naming,
     - pool connect/close,
     - baseline migration table creation.

7. SQL Server run-lifecycle implementation kickoff
   - Implemented SQL Server store run/query subset:
     - `enqueue`, `claimDueRuns`, `markSucceeded`, `cancelRun`, `markFailed`,
     - `getRun`, `getRecentRuns`, `getRuns`, `getRunsByJobName`, `getRunsByStatus`, `getEnqueuedRuns`,
     - `tryEnqueueIfNoActiveRun`, `extendLease`, `pruneHistoricalRuns`.
   - Added env-gated SQL Server contract tests for:
     - lease fencing,
     - lease reclaim,
     - no-active-run enqueue dedupe.

8. SQL Server recurring/runtime-command implementation
   - Implemented SQL Server recurring APIs:
     - `upsertRecurringJob`, `getRecurringJobs`, `setRecurringJobEnabled`, `updateRecurringJobSchedule`,
     - `getDueRecurringJobs`, `updateRecurringNextRun`, `tryMaterializeRecurringRun`.
   - Implemented SQL Server runtime-command receipt APIs:
     - `tryLeaseRuntimeCommandReceipt`, `markRuntimeCommandAcknowledged`, `markRuntimeCommandSucceeded`,
     - `markRuntimeCommandFailed`, `getRuntimeCommandReceipts`, `markRuntimeCommandReceiptUploaded`.
   - Extended env-gated SQL Server contract tests for:
     - recurring slot race single-winner behavior,
     - runtime-command lease single-winner contention,
     - runtime-command receipt ack/success/upload lifecycle.

9. SQL Server CI readiness
   - Added dedicated SQL Server service job in GitHub Actions.
   - Added failure artifact/log capture for SQL Server env-gated test runs.

10. P0 parity course-corrections applied
   - processor now applies recurring registration sync semantics for existing jobs/orphans,
   - processor now aborts local job execution when lease extension fails,
   - runtime-command receipt upload selection now excludes leased receipts and uploads only acknowledged/succeeded/failed states.

11. P1 and P2 parity course-corrections applied
   - retry behavior now supports per-registration mode (`fixed` / `backoff`) and per-registration initial delay,
    - default durable retention moved to 24h to align with durable provider expectations,
    - MySQL table prefix casing now preserves caller-provided prefix,
    - parity tests added for retry behavior, retention default, table prefix casing, and enqueued-run semantics.

12. SQLite provider scaffold kickoff
   - Added SQLite provider modules:
     - `src/sqlite/types.ts`
     - `src/sqlite/table-names.ts`
     - `src/sqlite/migrator.ts`
     - `src/sqlite/store.ts`
     - `src/sqlite/runtime.ts`
   - Added SQLite public exports in `src/index.ts`.
   - Added env-gated SQLite scaffold tests for:
     - table naming,
     - store connect/close,
     - baseline migration table creation.

## Next Implementation Steps

1. Add SQLite CI coverage strategy (file-backed DB job or matrix extension).
2. Run full provider parity checklist closure (MySQL/SQL Server/SQLite).
3. Prepare Phase 5 completion document and architecture/status updates.
