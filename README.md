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
  - job auto-discovery
  - MySQL/SQL Server/SQLite providers
  - framework adapters and convenience bundle package

## Phase 3 (in progress)

- Hosted observability scaffolding added:
  - ingestion event sink queue and sync service,
  - runtime-control sync service and command processor,
  - tests for ingestion headers/payloads and runtime-control command flow.
- Runtime hardening added:
  - hosted ingestion sync and runtime-control sync now auto-start/stop with runtime lifecycle when tenant credentials are configured.

## Phase 2 (completed)

- PostgreSQL provider foundation has been added with:
  - table naming resolver,
  - baseline idempotent migration,
  - Postgres `DurableJobStore` implementation,
  - Postgres contract/integration tests (env-gated locally),
  - CI Postgres service job that executes Postgres tests automatically.

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
