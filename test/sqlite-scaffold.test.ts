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

test("sqlite migration rejects tables created with a foreign schema (env-gated)", async (t) => {
  if (!await supportsNodeSqlite()) {
    t.skip("node:sqlite is not available in this Node runtime");
    return;
  }

  if (!sqlitePath) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const tempDir = await mkdtemp(path.join(tmpdir(), "durablestack-sqlite-"));
  const dbPath = path.join(tempDir, `it_sqlite_foreign_${Date.now().toString(36)}.db`);
  const prefix = `itforeign_${Date.now().toString(36)}_`;
  const db = await createSqliteDatabase(dbPath);
  try {
    const tables = resolveSqliteTableNames(prefix);

    // Simulate a database prepared by a different runtime: same table names,
    // different columns, and a migrations ledger that already contains
    // version 1 (as the .NET runtime's incremental ledger does). The
    // create-if-not-exists migration passes straight over these tables, so
    // only the schema verification probes can catch the mismatch.
    db.exec(`create table "${tables.migrations}" (version integer primary key, applied_at_utc text not null);`);
    db.exec(`insert into "${tables.migrations}" (version, applied_at_utc) values (1, '2026-01-01T00:00:00Z');`);
    db.exec(`create table "${tables.jobs}" (id text primary key, name text not null, schedule_type text not null);`);
    db.exec(`create table "${tables.runs}" (id text primary key, job_id text null, error_detail text null);`);
    db.exec(`create table "${tables.runtimeCommandReceipts}" (command_id text primary key, uploaded_to_platform integer not null);`);

    await assert.rejects(
      () => migrateSqlite(db, prefix),
      /does not match the DurableStack Node\.js schema/
    );
  } finally {
    db.close();
    await rm(tempDir, { recursive: true, force: true });
  }
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

test("sqlite migration is idempotent under repeated execution (env-gated)", async (t) => {
  if (!await supportsNodeSqlite()) {
    t.skip("node:sqlite is not available in this Node runtime");
    return;
  }

  if (!sqlitePath) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const tempDir = await mkdtemp(path.join(tmpdir(), "durablestack-sqlite-"));
  const dbPath = path.join(tempDir, `it_sqlite_repeat_${Date.now().toString(36)}.db`);
  const prefix = `itsqliterepeat_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const db = await createSqliteDatabase(dbPath);
  try {
    await migrateSqlite(db, prefix);
    await migrateSqlite(db, prefix);
    await migrateSqlite(db, prefix);
    assert.ok(true);
  } finally {
    db.close();
    await rm(tempDir, { recursive: true, force: true });
  }
});
