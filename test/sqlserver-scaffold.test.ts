import test from "node:test";
import assert from "node:assert/strict";
import { resolveSqlServerTableNames } from "../src/sqlserver/table-names.js";
import { createSqlServerPool, migrateSqlServer } from "../src/sqlserver/migrator.js";
import { SqlServerDurableJobStore } from "../src/sqlserver/store.js";

const connectionString = process.env.DURABLESTACK_TEST_SQLSERVER;

test("sqlserver table names apply prefix consistently", () => {
  const tables = resolveSqlServerTableNames("App_");
  assert.equal(tables.jobs, "App_durable_stack_jobs");
  assert.equal(tables.runs, "App_durable_stack_job_runs");
  assert.equal(tables.migrations, "App_durable_stack_schema_migrations");
  assert.equal(tables.runtimeCommandReceipts, "App_durable_stack_runtime_command_receipts");
});

test("sqlserver store can be constructed, connected, and closed (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_SQLSERVER is not set");
    return;
  }

  const store = new SqlServerDurableJobStore({ connectionString });
  await store.connect();
  await store.close();
  assert.ok(true);
});

test("sqlserver migration creates baseline tables (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_SQLSERVER is not set");
    return;
  }

  const prefix = `itsql_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const pool = await createSqlServerPool(connectionString);
  try {
    await migrateSqlServer(pool, prefix);
    const tables = resolveSqlServerTableNames(prefix);

    const result = await pool.request().query(`
      select [name] as table_name
      from sys.tables
      where [name] in ('${tables.jobs}', '${tables.runs}', '${tables.migrations}', '${tables.runtimeCommandReceipts}')
    `);

    const names = (result.recordset as Array<{ table_name: string }>).map((x) => x.table_name);
    assert.ok(names.includes(tables.jobs));
    assert.ok(names.includes(tables.runs));
    assert.ok(names.includes(tables.migrations));
    assert.ok(names.includes(tables.runtimeCommandReceipts));
  } finally {
    await pool.close();
  }
});
