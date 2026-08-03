# DurableStack Node.js (Phase 0-1)

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
  - hosted ingestion transport and runtime-control sync workers

## Phase 2 (in progress)

- PostgreSQL provider scaffold has been added with:
  - table naming resolver,
  - baseline migration,
  - Postgres `DurableJobStore` implementation,
  - initial env-gated Postgres integration tests.

Use env var `DURABLESTACK_TEST_POSTGRES` to enable Postgres integration tests locally/CI.

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
