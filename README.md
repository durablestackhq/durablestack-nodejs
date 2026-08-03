# DurableStack Node.js

This repository currently contains the Phase 0 and Phase 1 foundation for the DurableStack Node.js runtime:

- core runtime contracts and constants,
- external payload validators for telemetry/runtime-control shapes,
- in-memory runtime implementation with explicit job registration,
- worker processing loop with leasing, retries, recurring scheduling, and retention.

## Current status

- Implemented now:
  - explicit `registerJob` and `registerRecurring`
  - enqueue/schedule APIs
  - recurring schedule admin APIs
  - query APIs
  - in-memory store semantics (pending/leased/succeeded/failed)
  - lease heartbeat extension and completion fencing
  - event emission model with core event types
- Deferred to later phases:
  - SQL Server/SQLite providers
  - framework adapters and convenience bundle package

## Phase 5 (in progress)

- Provider parity kickoff started with MySQL-first scaffolding:
  - MySQL options/table naming/migrator/runtime/store scaffold,
  - env-gated MySQL migration/contract test coverage,
  - exported MySQL provider entry points.

Use env var `DURABLESTACK_TEST_MYSQL` to enable MySQL integration/contract tests locally/CI.

Phase 5 kickoff details are documented in `PHASE-5-KICKOFF.md`.

## Phase 4 (completed)

- Added opt-in autodiscovery foundation:
  - runtime option model for autodiscovery enablement and filters,
  - module discovery loader with include/exclude glob filtering,
  - strict job-definition validation and startup fail-on-error mode,
  - runtime startup wiring that loads discovered jobs before worker loop starts,
  - best-effort mode (`failOnError=false`) for mixed-validity module sets.

Phase 4 completion details are documented in `PHASE-4-COMPLETION.md`.

### Autodiscovery quick usage

```ts
import { createDurableStack } from "./src/index.js";

const runtime = createDurableStack({
  autodiscovery: {
    enabled: true,
    baseDir: process.cwd(),
    includeGlobs: ["src/jobs/**/*.jobs.mjs"],
    excludeGlobs: ["**/*.test.*"],
    exportName: "durableStackJobs",
    failOnError: true
  }
});
```

Module export shape:

```js
export const durableStackJobs = [
  {
    jobName: "send-email",
    maxAttempts: 3,
    handler: async (payload, context, signal) => {
      // job logic
    }
  }
];
```

## Phase 3 (completed)

- Hosted observability scaffolding added:
  - ingestion event sink queue and sync service,
  - runtime-control sync service and command processor,
  - tests for ingestion headers/payloads and runtime-control command flow.
- Runtime hardening added:
  - hosted ingestion sync and runtime-control sync now auto-start/stop with runtime lifecycle when tenant credentials are configured.
  - transient transport error handling is covered with bounded retry behavior.

Phase 3 completion details are documented in `PHASE-3-COMPLETION.md`.

## Phase 2 (completed)

- PostgreSQL provider foundation has been added with:
  - table naming resolver,
  - baseline idempotent migration,
  - Postgres `DurableJobStore` implementation,
  - Postgres contract/integration tests (env-gated locally),
  - CI Postgres service job that executes Postgres tests automatically.
- Recurring slot race contract test hardened to assert slot uniqueness invariants under contention without relying on strict timestamp string equality.

Use env var `DURABLESTACK_TEST_POSTGRES` to enable Postgres integration tests locally/CI.

Phase 2 completion details are documented in `PHASE-2-COMPLETION.md`.

## Quick start

```ts
import { createDurableStack } from "./src/index.js";

const runtime = createDurableStack({
  workerName: "node-worker-1",
  pollIntervalSeconds: 1
});

runtime.registerJob("send-email", async (payload) => {
  console.log("Running send-email", payload);
});

await runtime.start();
const runId = await runtime.enqueue("send-email", { userId: 123 });
console.log({ runId });
```

## Development

```bash
npm install
npm run typecheck
npm run test
```

## Docs

- Architecture and phase notes: `ARCHITECTURE.md`
- Contract definitions: `CONTRACTS.md`
- Parity checklist: `PARITY-CHECKLIST.md`
- Phase 4 autodiscovery scope plan: `PHASE-4-AUTODISCOVERY-PLAN.md`
- Phase 4 progress notes: `PHASE-4-PROGRESS.md`
- Phase 4 completion notes: `PHASE-4-COMPLETION.md`
