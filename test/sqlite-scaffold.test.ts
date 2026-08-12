import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { tmpdir } from "node:os";
import { mkdtemp, rm } from "node:fs/promises";
import { resolveSqliteTableNames } from "../src/sqlite/table-names.js";
import { createSqliteDatabase, migrateSqlite } from "../src/sqlite/migrator.js";
import { SqliteDurableJobStore } from "../src/sqlite/store.js";

const sqlitePath = process.env.DURABLESTACK_TEST_SQLITE;

async function supportsNodeSqlite(): Promise<boolean> {
  try {
    await import("node:sqlite");
    return true;
  } catch {
    return false;
  }
}

test("sqlite table names apply prefix consistently", () => {
  const tables = resolveSqliteTableNames("App_");
  assert.equal(tables.jobs, "App_durable_stack_jobs");
  assert.equal(tables.runs, "App_durable_stack_job_runs");
  assert.equal(tables.migrations, "App_durable_stack_schema_migrations");
  assert.equal(tables.runtimeCommandReceipts, "App_durable_stack_runtime_command_receipts");
});

test("sqlite store can be constructed, connected, and closed (env-gated)", async (t) => {
  if (!await supportsNodeSqlite()) {
    t.skip("node:sqlite is not available in this Node runtime");
    return;
  }

  if (!sqlitePath) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const store = new SqliteDurableJobStore({ databasePath: sqlitePath });
  await store.connect();
  await store.close();
  assert.ok(true);
});

test("sqlite migration creates baseline tables (env-gated)", async (t) => {
  if (!await supportsNodeSqlite()) {
    t.skip("node:sqlite is not available in this Node runtime");
    return;
  }

  if (!sqlitePath) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const tempDir = await mkdtemp(path.join(tmpdir(), "durablestack-sqlite-"));
  const dbPath = path.join(tempDir, `it_sqlite_${Date.now().toString(36)}.db`);
  const prefix = `itsqlite_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const db = await createSqliteDatabase(dbPath);
  try {
    await migrateSqlite(db, prefix);
    const tables = resolveSqliteTableNames(prefix);

    const rows = db.prepare("select name from sqlite_master where type = 'table' and name in (?, ?, ?, ?)").all(
      tables.jobs,
      tables.runs,
      tables.migrations,
      tables.runtimeCommandReceipts
    ) as Array<{ name: string }>;

    const names = rows.map((x) => x.name);
    assert.ok(names.includes(tables.jobs));
    assert.ok(names.includes(tables.runs));
    assert.ok(names.includes(tables.migrations));
    assert.ok(names.includes(tables.runtimeCommandReceipts));
  } finally {
    db.close();
    await rm(tempDir, { recursive: true, force: true });
  }
});
