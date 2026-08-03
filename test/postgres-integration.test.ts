import test from "node:test";
import assert from "node:assert/strict";
import { PostgresDurableJobStore } from "../src/postgres/store.js";
import { migratePostgres } from "../src/postgres/migrator.js";
import { resolvePostgresTableNames } from "../src/postgres/table-names.js";

const connectionString = process.env.DURABLESTACK_TEST_POSTGRES;

test("postgres migration creates baseline tables (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const prefix = `it_${Date.now()}_`;
  const store = new PostgresDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  await migratePostgres(store.getPool(), prefix);

  const tables = resolvePostgresTableNames(prefix);
  const result = await store.getPool().query(
    `
      select table_name
      from information_schema.tables
      where table_schema = 'public'
        and table_name in ($1, $2, $3, $4)
    `,
    [tables.jobs, tables.runs, tables.migrations, tables.runtimeCommandReceipts]
  );

  const names = result.rows.map((x) => String(x.table_name));
  assert.ok(names.includes(tables.jobs));
  assert.ok(names.includes(tables.runs));
  assert.ok(names.includes(tables.migrations));
  assert.ok(names.includes(tables.runtimeCommandReceipts));

  await store.close();
});

test("postgres enqueue -> claim -> succeed flow (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const prefix = `it_run_${Date.now()}_`;
  const store = new PostgresDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  await migratePostgres(store.getPool(), prefix);

  const runId = await store.enqueue("job-a", "job-a", JSON.stringify({ ok: true }), new Date().toISOString(), 3);
  const claimed = await store.claimDueRuns("worker-a", 1, 30);

  assert.equal(claimed.length, 1);
  assert.equal(claimed[0]?.id, runId);

  const succeeded = await store.markSucceeded(runId, "worker-a");
  assert.equal(succeeded, true);

  const run = await store.getRun(runId);
  assert.equal(run?.status, "succeeded");

  await store.close();
});
