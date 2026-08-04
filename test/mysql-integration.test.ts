import test from "node:test";
import assert from "node:assert/strict";
import { createMySqlPool, migrateMySql } from "../src/mysql/migrator.js";
import { MySqlDurableJobStore } from "../src/mysql/store.js";
import { resolveMySqlTableNames } from "../src/mysql/table-names.js";

const connectionString = process.env.DURABLESTACK_TEST_MYSQL;

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
    assert.ok(names.includes(tables.jobs.toLowerCase()));
    assert.ok(names.includes(tables.runs.toLowerCase()));
    assert.ok(names.includes(tables.migrations.toLowerCase()));
    assert.ok(names.includes(tables.runtimeCommandReceipts.toLowerCase()));
  } finally {
    await pool.end();
  }
});

test("mysql enqueue -> claim -> succeed flow (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const prefix = `itrn_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const store = new MySqlDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  try {
    await migrateMySql(store.getPool(), prefix);

    const runId = await store.enqueue("job-a", "job-a", JSON.stringify({ ok: true }), new Date().toISOString(), 3);
    const claimed = await store.claimDueRuns("worker-a", 1, 30);

    assert.equal(claimed.length, 1);
    assert.equal(claimed[0]?.id, runId);

    const succeeded = await store.markSucceeded(runId, "worker-a");
    assert.equal(succeeded, true);

    const run = await store.getRun(runId);
    assert.equal(run?.status, "succeeded");
  } finally {
    await store.close();
  }
});

test("mysql migration is safe under concurrent calls on same prefix (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const prefix = `itmc_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const pool = createMySqlPool(connectionString);
  try {
    await Promise.all([
      migrateMySql(pool, prefix),
      migrateMySql(pool, prefix),
      migrateMySql(pool, prefix),
      migrateMySql(pool, prefix)
    ]);

    const tables = resolveMySqlTableNames(prefix);
    const [rows] = await pool.query(`select count(*) as c from ${"`" + tables.migrations + "`"} where version = ?`, [1]);
    const count = Number((rows as Array<{ c: unknown }>)[0]?.c ?? 0);
    assert.equal(count, 1);
  } finally {
    await pool.end();
  }
});
