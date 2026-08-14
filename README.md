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
npm install durablestack
```

Point DurableStack at your database. The provider factory creates the store, applies migrations, and returns a runtime:

```ts
import { createDurableStackPostgres } from "durablestack";

const { runtime, closeStore } = await createDurableStackPostgres(
  { connectionString: process.env.DATABASE_URL! },
  { workerName: "node-worker-1", pollIntervalSeconds: 1 }
);

runtime.registerJob("send-email", async (payload) => {
  console.log("Running send-email", payload);
});

runtime.registerRecurring("nightly-report", "0 2 * * *", "America/New_York", async () => {
  console.log("Running nightly report");
});

await runtime.start();

const runId = await runtime.enqueue("send-email", { userId: 123 });
console.log({ runId });

// On shutdown: stop() drains in-flight jobs before returning.
process.on("SIGTERM", async () => {
  await runtime.stop();
  await closeStore();
});
```

Every provider follows the same shape — swap the factory and its connection options:

```ts
import {
  createDurableStackPostgres,
  createDurableStackMySql,
  createDurableStackSqlServer,
  createDurableStackSqlite
} from "durablestack";

await createDurableStackMySql({ connectionString: process.env.MYSQL_URL! });
await createDurableStackSqlServer({ connectionString: process.env.MSSQL_URL! });
await createDurableStackSqlite({ databasePath: "./data/jobs.db" });
```

For tests and local experiments, `createDurableStack()` returns an in-memory runtime with no database and no migrations. It is not durable across restarts — use a database provider for anything real.

```ts
import { createDurableStack } from "durablestack";

const runtime = createDurableStack({ workerName: "test-worker" });
```

## Requirements

- **Node.js 20 or later** for the PostgreSQL, MySQL, SQL Server, and in-memory providers.
- **Node.js 22.5 or later** for the SQLite provider, which is built on the built-in `node:sqlite` module. Node still marks `node:sqlite` as experimental and prints a warning on load; it may change in future Node releases. The module is imported lazily, so other providers are unaffected on older runtimes.

## Key Features

- Durable one-off, delayed, and recurring (cron) jobs with timezone support.
- Retry policies, terminal failure handling, and distributed worker coordination.
- Multi-provider support (PostgreSQL, MySQL, SQL Server, SQLite, InMemory).
- Runtime command control receipt lifecycle for schedule operations.
- Hosted observability/eventing integration plus custom sink support.
- Job autodiscovery (opt-in) with strict or best-effort startup modes.

## Sharing a Database with the .NET Runtime

DurableStack for Node.js and DurableStack for .NET can share the same database **instance**, but not the same tables. Jobs are defined in your application code, so a run enqueued by one runtime can never be executed by the other — each runtime therefore owns its own schema, and the schemas are intentionally not interchangeable.

If you run both runtimes against one database, give each its own tables with `databaseTablePrefix` (for example `node_` and `dotnet_`). The Node.js runtime verifies its schema at startup: if the tables at the configured prefix exist but were created by something else (such as the .NET runtime), startup fails with a clear error instead of failing later mid-operation.

## Runtime Command Control

When hosted eventing/runtime-control credentials are configured, runtimes can sync and process runtime-control commands.

- Run schedule now
- Pause/resume schedule
- Update cron and time zone

## Hosted Eventing Security

When `eventing.tenantId`/`eventing.clientSecret` are configured, DurableStack posts telemetry and syncs runtime-control commands to `eventing.ingestionApiBaseUrl`. Two defaults protect that traffic:

- **Error messages and stack traces are redacted by default.** Job failure events omit `errorMessage`/`errorDetail` unless `eventing.includeErrorDetail` is set to `true`, since exception text routinely contains connection strings, file paths, SQL fragments, or business data. The exception type name (e.g. `Error`, `TypeError`) is always included. This only affects events published to sinks — the run's own error message is still recorded in your database via `getRun`/`getRunsByStatus`, unredacted, since it never leaves your infrastructure. When enabled, detail is truncated to `eventing.maxErrorDetailLength` (default 4096 characters).
- **`ingestionApiBaseUrl` must use HTTPS.** Both the ingestion and runtime-control requests carry your tenant credentials as headers, so a plain `http://` base URL is rejected at startup unless it points at a loopback address (`localhost`, `127.0.0.0/8`, or `::1`) for local development against a test endpoint.

## Getting Started

- Architecture notes: `ARCHITECTURE.md`
- Contract definitions: `CONTRACTS.md`
- Contributing guide: `CONTRIBUTING.md`
- Security policy: `SECURITY.md`

## Status

Current focus is prerelease hardening and packaging for real-world testing.

---

**License**: MIT
**Contributing**: See `CONTRIBUTING.md`
