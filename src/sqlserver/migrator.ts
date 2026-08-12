import sql from "mssql";
import { resolveSqlServerTableNames } from "./table-names.js";

const SCHEMA_VERSION = 1;

function qi(name: string): string {
  return `[${name.replace(/]/g, "]]")}]`;
}

function ql(name: string): string {
  return `'${name.replace(/'/g, "''")}'`;
}

export async function migrateSqlServer(pool: sql.ConnectionPool, tablePrefix: string | undefined): Promise<void> {
  const tables = resolveSqlServerTableNames(tablePrefix);

  await pool.request().query(`
    if object_id(${ql(tables.migrations)}, 'U') is null
    begin
      create table ${qi(tables.migrations)} (
        version int not null primary key,
        applied_at_utc datetime2(3) not null
      );
    end
  `);

  const existing = await pool.request()
    .input("version", SCHEMA_VERSION)
    .query(`select version from ${qi(tables.migrations)} where version = @version`);

  if ((existing.recordset?.length ?? 0) > 0) {
    return;
  }

  const tx = pool.transaction();
  await tx.begin();
  try {
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
