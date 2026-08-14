const JOBS_COLUMNS = [
  "job_name",
  "job_type",
  "cron_expression",
  "time_zone",
  "max_attempts",
  "enabled",
  "allow_concurrent_runs",
  "retry_behavior",
  "retry_initial_delay_seconds",
  "next_run_at_utc",
  "updated_at_utc"
] as const;

const RUNS_COLUMNS = [
  "id",
  "job_name",
  "job_type",
  "status",
  "scheduled_for_utc",
  "schedule_slot_utc",
  "started_at_utc",
  "completed_at_utc",
  "attempt",
  "max_attempts",
  "lease_owner",
  "lease_until_utc",
  "payload_json",
  "error_message",
  "created_at_utc"
] as const;

const RECEIPTS_COLUMNS = [
  "command_id",
  "status",
  "error_code",
  "error_message",
  "run_id",
  "recorded_at_utc",
  "completed_at_utc",
  "uploaded_at_utc",
  "lease_owner",
  "lease_until_utc"
] as const;

export interface SchemaProbe {
  table: string;
  sql: string;
}

/**
 * Builds one zero-row select per table listing every column this runtime
 * requires. Running the probes proves the tables at the configured prefix
 * actually match the Node.js schema: `create table if not exists` migrations
 * pass silently over tables created by something else (most notably the
 * DurableStack .NET runtime, which shares the default table names but uses a
 * different, runtime-specific schema).
 */
export function buildSchemaProbes(
  tables: { jobs: string; runs: string; runtimeCommandReceipts: string },
  quoteIdentifier: (name: string) => string
): SchemaProbe[] {
  const probe = (table: string, columns: readonly string[]): SchemaProbe => ({
    table,
    sql: `select ${columns.map(quoteIdentifier).join(", ")} from ${quoteIdentifier(table)} where 1 = 0`
  });

  return [
    probe(tables.jobs, JOBS_COLUMNS),
    probe(tables.runs, RUNS_COLUMNS),
    probe(tables.runtimeCommandReceipts, RECEIPTS_COLUMNS)
  ];
}

export function createSchemaMismatchError(table: string, cause: unknown): Error {
  const detail = cause instanceof Error ? cause.message : String(cause);
  return new Error(
    `DurableStack schema verification failed for table '${table}': ${detail}. `
    + "The table exists but does not match the DurableStack Node.js schema. It may have been created by the "
    + "DurableStack .NET runtime, an incompatible version, or another application. The Node.js and .NET runtimes "
    + "use separate schemas by design (jobs are defined in code, so runs are never portable between runtimes); "
    + "they can share a database, but not tables. Configure a distinct databaseTablePrefix for each runtime."
  );
}
