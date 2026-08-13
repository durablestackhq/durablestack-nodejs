import { mkdir } from "node:fs/promises";
import path from "node:path";
import { buildSchemaProbes, createSchemaMismatchError } from "../schema-verification.js";
import { resolveSqliteTableNames, type SqliteTableNames } from "./table-names.js";

const SCHEMA_VERSION = 1;

export interface SqliteDatabaseLike {
  exec(sql: string): void;
  prepare(sql: string): {
    get(...params: unknown[]): unknown;
    all(...params: unknown[]): unknown;
    run(...params: unknown[]): unknown;
  };
  close(): void;
}

function qi(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export async function migrateSqlite(db: SqliteDatabaseLike, tablePrefix: string | undefined): Promise<void> {
  const tables = resolveSqliteTableNames(tablePrefix);
  applySqliteMigrations(db, tables);
  verifySqliteSchema(db, tables);
}

function verifySqliteSchema(db: SqliteDatabaseLike, tables: SqliteTableNames): void {
  for (const probe of buildSchemaProbes(tables, qi)) {
    try {
      db.prepare(probe.sql).all();
    } catch (error) {
      throw createSchemaMismatchError(probe.table, error);
    }
  }
}

function applySqliteMigrations(db: SqliteDatabaseLike, tables: SqliteTableNames): void {
  db.exec(`
    create table if not exists ${qi(tables.migrations)} (
      version integer primary key,
      applied_at_utc text not null
    );
  `);

  const existing = db.prepare(`select version from ${qi(tables.migrations)} where version = ?`).get(SCHEMA_VERSION) as
    | { version: number }
    | undefined;
  if (existing) {
    return;
  }

  db.exec("begin immediate transaction;");
  try {
    db.exec(`
      create table if not exists ${qi(tables.jobs)} (
        job_name text primary key,
        job_type text not null,
        cron_expression text not null,
        time_zone text not null,
        max_attempts integer not null,
        enabled integer not null,
        allow_concurrent_runs integer not null,
        retry_behavior text null,
        retry_initial_delay_seconds integer null,
        next_run_at_utc text not null,
        updated_at_utc text not null
      );
    `);

    db.exec(`
      create table if not exists ${qi(tables.runs)} (
        id text primary key,
        job_name text not null,
        job_type text not null,
        status text not null,
        scheduled_for_utc text not null,
        schedule_slot_utc text null,
        started_at_utc text null,
        completed_at_utc text null,
        attempt integer not null,
        max_attempts integer not null,
        lease_owner text null,
        lease_until_utc text null,
        payload_json text null,
        error_message text null,
        created_at_utc text not null
      );
    `);

    db.exec(`create index if not exists ${qi(`ix_${tables.runs}_status_scheduled`)} on ${qi(tables.runs)} (status, scheduled_for_utc asc);`);
    db.exec(`create index if not exists ${qi(`ix_${tables.runs}_job_name_scheduled`)} on ${qi(tables.runs)} (job_name, scheduled_for_utc desc);`);
    db.exec(`
      create unique index if not exists ${qi(`ix_${tables.runs}_recurring_slot_unique`)}
      on ${qi(tables.runs)} (job_name, schedule_slot_utc)
      where schedule_slot_utc is not null;
    `);

    db.exec(`
      create table if not exists ${qi(tables.runtimeCommandReceipts)} (
        command_id text primary key,
        status text not null,
        error_code text null,
        error_message text null,
        run_id text null,
        recorded_at_utc text not null,
        completed_at_utc text null,
        uploaded_at_utc text null,
        lease_owner text null,
        lease_until_utc text null
      );
    `);

    db.prepare(`
      insert into ${qi(tables.migrations)} (version, applied_at_utc)
      values (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      on conflict(version) do nothing
    `).run(SCHEMA_VERSION);

    db.exec("commit;");
  } catch (error) {
    db.exec("rollback;");
    throw error;
  }
}

export async function createSqliteDatabase(databasePath: string): Promise<SqliteDatabaseLike> {
  if (databasePath !== ":memory:") {
    const parent = path.dirname(databasePath);
    await mkdir(parent, { recursive: true });
  }

  let sqliteModule: typeof import("node:sqlite");
  try {
    sqliteModule = await import("node:sqlite");
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(
      `The DurableStack SQLite provider requires the built-in 'node:sqlite' module, which is unavailable on this runtime (${process.version}). `
      + `Upgrade to Node.js 22.5 or later, or use a different provider. Underlying error: ${detail}`
    );
  }

  return new sqliteModule.DatabaseSync(databasePath);
}
