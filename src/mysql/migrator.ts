import { createPool, type Pool } from "mysql2/promise";
import { resolveMySqlTableNames } from "./table-names.js";

const SCHEMA_VERSION = 1;
const MYSQL_IDENTIFIER_MAX_LENGTH = 64;

function qi(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``;
}

function migrationLockName(tablePrefix: string | undefined): string {
  const base = (tablePrefix && tablePrefix.trim()) ? tablePrefix.trim() : "default";
  const safe = base.replace(/[^A-Za-z0-9_]/g, "_");
  const raw = `durablestack_migrate_${safe}`;
  return raw.length <= MYSQL_IDENTIFIER_MAX_LENGTH ? raw : raw.slice(0, MYSQL_IDENTIFIER_MAX_LENGTH);
}

export async function migrateMySql(pool: Pool, tablePrefix: string | undefined): Promise<void> {
  const tables = resolveMySqlTableNames(tablePrefix);
  const lockName = migrationLockName(tablePrefix);

  const [lockRows] = await pool.query(`select get_lock(?, 30) as acquired`, [lockName]);
  const acquired = Number((lockRows as Array<{ acquired: unknown }>)[0]?.acquired ?? 0) === 1;
  if (!acquired) {
    throw new Error(`Failed to acquire MySQL migration lock '${lockName}'.`);
  }

  try {
    await pool.query(`
      create table if not exists ${qi(tables.migrations)} (
        version int primary key,
        applied_at_utc datetime(3) not null
      ) engine=InnoDB;
    `);

    const [existing] = await pool.query(`select version from ${qi(tables.migrations)} where version = ?`, [SCHEMA_VERSION]);
    const existingRows = existing as Array<{ version: number }>;
    if (existingRows.length > 0) {
      return;
    }

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      await conn.query(`
        create table if not exists ${qi(tables.jobs)} (
          job_name varchar(191) primary key,
          job_type varchar(191) not null,
          cron_expression varchar(255) not null,
          time_zone varchar(100) not null,
          max_attempts int not null,
          enabled tinyint(1) not null,
          allow_concurrent_runs tinyint(1) not null,
          retry_behavior varchar(32) null,
          retry_initial_delay_seconds int null,
          next_run_at_utc datetime(3) not null,
          updated_at_utc datetime(3) not null
        ) engine=InnoDB;
      `);

      await conn.query(`
        create table if not exists ${qi(tables.runs)} (
          id char(36) primary key,
          job_name varchar(191) not null,
          job_type varchar(191) not null,
          status varchar(32) not null,
          scheduled_for_utc datetime(3) not null,
          schedule_slot_utc datetime(3) null,
          started_at_utc datetime(3) null,
          completed_at_utc datetime(3) null,
          attempt int not null,
          max_attempts int not null,
          lease_owner varchar(191) null,
          lease_until_utc datetime(3) null,
          payload_json json null,
          error_message text null,
          created_at_utc datetime(3) not null,
          index ix_runs_status_scheduled (status, scheduled_for_utc),
          index ix_runs_job_name_scheduled (job_name, scheduled_for_utc),
          unique key ux_runs_recurring_slot (job_name, schedule_slot_utc)
        ) engine=InnoDB;
      `);

      await conn.query(`
        create table if not exists ${qi(tables.runtimeCommandReceipts)} (
          command_id varchar(191) primary key,
          status varchar(32) not null,
          error_code varchar(100) null,
          error_message text null,
          run_id char(36) null,
          recorded_at_utc datetime(3) not null,
          completed_at_utc datetime(3) null,
          uploaded_at_utc datetime(3) null,
          lease_owner varchar(191) null,
          lease_until_utc datetime(3) null
        ) engine=InnoDB;
      `);

      await conn.query(
        `insert into ${qi(tables.migrations)} (version, applied_at_utc)
         values (?, utc_timestamp(3))
         on duplicate key update applied_at_utc = applied_at_utc`,
        [SCHEMA_VERSION]
      );

      await conn.commit();
    } catch (error) {
      await conn.rollback();
      throw error;
    } finally {
      conn.release();
    }
  } finally {
    await pool.query("select release_lock(?)", [lockName]);
  }
}

export function createMySqlPool(connectionString: string): Pool {
  return createPool({
    uri: connectionString,
    timezone: "Z",
    dateStrings: true,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });
}
