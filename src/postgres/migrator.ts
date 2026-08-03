import type { Pool } from "pg";
import { resolvePostgresTableNames } from "./table-names.js";

const SCHEMA_VERSION = 1;

function q(name: string): string {
  return `"${name.replace(/"/g, "\"\"")}"`;
}

export async function migratePostgres(pool: Pool, tablePrefix: string | undefined): Promise<void> {
  const tables = resolvePostgresTableNames(tablePrefix);

  await pool.query(`
    create table if not exists ${q(tables.migrations)} (
      version integer primary key,
      applied_at_utc timestamptz not null
    );
  `);

  const existing = await pool.query(`select version from ${q(tables.migrations)} where version = $1`, [SCHEMA_VERSION]);
  if (existing.rowCount && existing.rowCount > 0) {
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("begin");

    await client.query(`
      create table if not exists ${q(tables.jobs)} (
        job_name text primary key,
        job_type text not null,
        cron_expression text not null,
        time_zone text not null,
        max_attempts integer not null,
        enabled boolean not null,
        allow_concurrent_runs boolean not null,
        retry_behavior text null,
        retry_initial_delay_seconds integer null,
        next_run_at_utc timestamptz not null,
        updated_at_utc timestamptz not null
      );
    `);

    await client.query(`
      create table if not exists ${q(tables.runs)} (
        id uuid primary key,
        job_name text not null,
        job_type text not null,
        status text not null,
        scheduled_for_utc timestamptz not null,
        schedule_slot_utc timestamptz null,
        started_at_utc timestamptz null,
        completed_at_utc timestamptz null,
        attempt integer not null,
        max_attempts integer not null,
        lease_owner text null,
        lease_until_utc timestamptz null,
        payload_json jsonb null,
        error_message text null,
        created_at_utc timestamptz not null
      );
    `);

    await client.query(`
      create index if not exists ${q(`ix_${tables.runs}_status_scheduled`)}
      on ${q(tables.runs)} (status, scheduled_for_utc asc);
    `);

    await client.query(`
      create index if not exists ${q(`ix_${tables.runs}_job_name_scheduled`)}
      on ${q(tables.runs)} (job_name, scheduled_for_utc desc);
    `);

    await client.query(`
      create unique index if not exists ${q(`ix_${tables.runs}_recurring_slot_unique`)}
      on ${q(tables.runs)} (job_name, schedule_slot_utc)
      where schedule_slot_utc is not null;
    `);

    await client.query(`
      create table if not exists ${q(tables.runtimeCommandReceipts)} (
        command_id text primary key,
        status text not null,
        error_code text null,
        error_message text null,
        run_id uuid null,
        recorded_at_utc timestamptz not null,
        completed_at_utc timestamptz null,
        uploaded_at_utc timestamptz null,
        lease_owner text null,
        lease_until_utc timestamptz null
      );
    `);

    await client.query(
      `insert into ${q(tables.migrations)} (version, applied_at_utc)
       values ($1, now())
       on conflict (version) do nothing`,
      [SCHEMA_VERSION]
    );

    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}
