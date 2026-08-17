import sql from "mssql";
import { buildSchemaProbes, createSchemaMismatchError } from "../schema-verification.js";
import { resolveSqlServerTableNames, type SqlServerTableNames } from "./table-names.js";

const SCHEMA_VERSION = 1;
const SQLSERVER_MIGRATION_LOCK_TIMEOUT_MS = 30_000;

function qi(name: string): string {
  return `[${name.replace(/]/g, "]]")}]`;
}

function ql(name: string): string {
  return `'${name.replace(/'/g, "''")}'`;
}

export async function migrateSqlServer(pool: sql.ConnectionPool, tablePrefix: string | undefined): Promise<void> {
  const tables = resolveSqlServerTableNames(tablePrefix);
  await applySqlServerMigrations(pool, tablePrefix, tables);
  await verifySqlServerSchema(pool, tables);
}

function migrationLockName(tablePrefix: string | undefined): string {
  const base = (tablePrefix && tablePrefix.trim()) ? tablePrefix.trim() : "default";
  const safe = base.replace(/[^A-Za-z0-9_]/g, "_");
  return `durablestack_migrate_${safe}`;
}

async function verifySqlServerSchema(pool: sql.ConnectionPool, tables: SqlServerTableNames): Promise<void> {
  for (const probe of buildSchemaProbes(tables, qi)) {
    try {
      await pool.request().query(probe.sql);
    } catch (error) {
      throw createSchemaMismatchError(probe.table, error);
    }
  }
}

async function applySqlServerMigrations(
  pool: sql.ConnectionPool,
  tablePrefix: string | undefined,
  tables: SqlServerTableNames
): Promise<void> {
  const tx = pool.transaction();
  await tx.begin();
  try {
    const lock = await tx.request()
      .input("resource", sql.NVarChar(255), migrationLockName(tablePrefix))
      .input("timeoutMs", sql.Int, SQLSERVER_MIGRATION_LOCK_TIMEOUT_MS)
      .query(`
        declare @result int;
        exec @result = sp_getapplock
          @Resource = @resource,
          @LockMode = 'Exclusive',
          @LockOwner = 'Transaction',
          @LockTimeout = @timeoutMs;
        select @result as result;
      `);

    const lockResult = Number((lock.recordset?.[0] as { result?: unknown } | undefined)?.result ?? -999);
    if (lockResult < 0) {
      throw new Error(`Failed to acquire SQL Server migration lock '${migrationLockName(tablePrefix)}'. Result=${lockResult}`);
    }

    await tx.request().query(`
      if object_id(${ql(tables.migrations)}, 'U') is null
      begin
        create table ${qi(tables.migrations)} (
          version int not null primary key,
          applied_at_utc datetime2(3) not null
        );
      end
    `);

    const existing = await tx.request()
      .input("version", SCHEMA_VERSION)
      .query(`select version from ${qi(tables.migrations)} where version = @version`);

    if ((existing.recordset?.length ?? 0) > 0) {
      await tx.commit();
      return;
    }

    await tx.request().query(`
      if object_id(${ql(tables.jobs)}, 'U') is null
      begin
        create table ${qi(tables.jobs)} (
          job_name nvarchar(256) not null primary key,
          job_type nvarchar(256) not null,
          cron_expression nvarchar(256) not null,
          time_zone nvarchar(128) not null,
          max_attempts int not null,
          enabled bit not null,
          allow_concurrent_runs bit not null,
          retry_behavior nvarchar(32) null,
          retry_initial_delay_seconds int null,
          next_run_at_utc datetime2(3) not null,
          updated_at_utc datetime2(3) not null
        );
      end
    `);

    await tx.request().query(`
      if object_id(${ql(tables.runs)}, 'U') is null
      begin
        create table ${qi(tables.runs)} (
          id uniqueidentifier not null primary key,
          job_name nvarchar(256) not null,
          job_type nvarchar(256) not null,
          status nvarchar(32) not null,
          scheduled_for_utc datetime2(3) not null,
          schedule_slot_utc datetime2(3) null,
          started_at_utc datetime2(3) null,
          completed_at_utc datetime2(3) null,
          attempt int not null,
          max_attempts int not null,
          lease_owner nvarchar(256) null,
          lease_until_utc datetime2(3) null,
          payload_json nvarchar(max) null,
          error_message nvarchar(max) null,
          created_at_utc datetime2(3) not null
        );

        create index ${qi(`ix_${tables.runs}_status_scheduled`)} on ${qi(tables.runs)} (status, scheduled_for_utc asc);
        create index ${qi(`ix_${tables.runs}_job_name_scheduled`)} on ${qi(tables.runs)} (job_name, scheduled_for_utc desc);
        create unique index ${qi(`ix_${tables.runs}_recurring_slot_unique`)}
          on ${qi(tables.runs)} (job_name, schedule_slot_utc)
          where schedule_slot_utc is not null;
      end
    `);

    await tx.request().query(`
      if object_id(${ql(tables.runtimeCommandReceipts)}, 'U') is null
      begin
        create table ${qi(tables.runtimeCommandReceipts)} (
          command_id nvarchar(256) not null primary key,
          status nvarchar(32) not null,
          error_code nvarchar(128) null,
          error_message nvarchar(max) null,
          run_id uniqueidentifier null,
          recorded_at_utc datetime2(3) not null,
          completed_at_utc datetime2(3) null,
          uploaded_at_utc datetime2(3) null,
          lease_owner nvarchar(256) null,
          lease_until_utc datetime2(3) null
        );
      end
    `);

    await tx.request()
      .input("version", SCHEMA_VERSION)
      .query(`
        if not exists (select 1 from ${qi(tables.migrations)} where version = @version)
        begin
          insert into ${qi(tables.migrations)} (version, applied_at_utc)
          values (@version, sysutcdatetime());
        end
      `);

    await tx.commit();
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

export async function createSqlServerPool(connectionString: string): Promise<sql.ConnectionPool> {
  const pool = new sql.ConnectionPool(connectionString);
  await pool.connect();
  return pool;
}
