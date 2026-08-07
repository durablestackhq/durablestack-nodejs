# DurableStack (Node.js)

**Run durable background jobs in Node.js using the database you already have.**

DurableStack provides recurring scheduling, retries, distributed execution, and worker observability **without requiring Redis, RabbitMQ, or additional queue infrastructure**.

## Why DurableStack?

- **Database-native execution** — Use PostgreSQL, MySQL, SQL Server, or SQLite as the coordination layer.
- **Distributed-safe by default** — Lease-based claiming, heartbeats, and safe reclaim on failure.
- **Production observability** — OpenTelemetry-style event sinks plus optional hosted observability integration.
- **Cross-runtime direction** — Node.js runtime aligned to the same external contract semantics as the .NET runtime.

## Quick Start

```bash
npm install durablestack-nodejs
```

```ts
import { createDurableStack } from "durablestack-nodejs";

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

## Key Features

- Durable one-off, delayed, and recurring (cron) jobs with timezone support.
- Retry policies, terminal failure handling, and distributed worker coordination.
- Multi-provider support (PostgreSQL, MySQL, SQL Server, SQLite, InMemory).
- Runtime command control receipt lifecycle for schedule operations.
- Hosted observability/eventing integration plus custom sink support.
- Job autodiscovery (opt-in) with strict or best-effort startup modes.

## Runtime Command Control

When hosted eventing/runtime-control credentials are configured, runtimes can sync and process runtime-control commands.

- Run schedule now
- Pause/resume schedule
- Update cron and time zone

## Provider Test Envs

Use these environment variables to enable provider-specific tests locally or in CI:

- `DURABLESTACK_TEST_POSTGRES`
- `DURABLESTACK_TEST_MYSQL`
- `DURABLESTACK_TEST_SQLSERVER`
- `DURABLESTACK_TEST_SQLITE`

Note: SQLite relies on Node built-in `node:sqlite`. In runtimes where it is unavailable, SQLite tests are skipped.

## Development

```bash
npm install
npm run typecheck
npm run test
```

## Getting Started

- Architecture notes: `ARCHITECTURE.md`
- Contract definitions: `CONTRACTS.md`
- Contributing guide: `CONTRIBUTING.md`
- Security policy: `SECURITY.md`

## Status

Phase 5 provider parity is complete for Node.js (PostgreSQL, MySQL, SQL Server, SQLite, and InMemory).

Current focus is prerelease hardening and packaging for real-world testing.

---

**License**: MIT
**Contributing**: See `CONTRIBUTING.md`
