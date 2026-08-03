import test from "node:test";
import assert from "node:assert/strict";
import { resolveMySqlTableNames } from "../src/mysql/table-names.js";
import { createMySqlPool, migrateMySql } from "../src/mysql/migrator.js";
import { MySqlDurableJobStore } from "../src/mysql/store.js";

const connectionString = process.env.DURABLESTACK_TEST_MYSQL;

test("mysql table names apply prefix consistently", () => {
  const tables = resolveMySqlTableNames("App_");
  assert.equal(tables.jobs, "App_durable_stack_jobs");
  assert.equal(tables.runs, "App_durable_stack_job_runs");
  assert.equal(tables.migrations, "App_durable_stack_schema_migrations");
  assert.equal(tables.runtimeCommandReceipts, "App_durable_stack_runtime_command_receipts");
});

test("mysql store can be constructed and closed", async () => {
  const store = new MySqlDurableJobStore({ connectionString: "mysql://root:root@localhost:3306/durable_stack" });
  await store.close();
  assert.ok(true);
});

test("mysql migration creates baseline tables (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const prefix = `itmy_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const pool = createMySqlPool(connectionString);
  try {
    await migrateMySql(pool, prefix);
    const tables = resolveMySqlTableNames(prefix);

    const [rows] = await pool.query(
      `
        select lower(table_name) as table_name
        from information_schema.tables
        where table_schema = database()
          and lower(table_name) in (?, ?, ?, ?)
      `,
      [
        tables.jobs.toLowerCase(),
        tables.runs.toLowerCase(),
        tables.migrations.toLowerCase(),
        tables.runtimeCommandReceipts.toLowerCase()
      ]
    );

    const names = (rows as Array<{ table_name: string }>).map((x) => x.table_name);
    assert.ok(names.includes(tables.jobs));
    assert.ok(names.includes(tables.runs));
    assert.ok(names.includes(tables.migrations));
    assert.ok(names.includes(tables.runtimeCommandReceipts));
  } finally {
    await pool.end();
  }
});
