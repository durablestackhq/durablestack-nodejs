import { normalizePrefix } from "../utils.js";

const BASE = {
  jobs: "durable_stack_jobs",
  runs: "durable_stack_job_runs",
  migrations: "durable_stack_schema_migrations",
  runtimeCommandReceipts: "durable_stack_runtime_command_receipts"
} as const;

function build(prefix: string | undefined, base: string): string {
  if (!prefix) {
    return base;
  }

  return `${prefix}${base}`;
}

export interface SqlServerTableNames {
  jobs: string;
  runs: string;
  migrations: string;
  runtimeCommandReceipts: string;
}

export function resolveSqlServerTableNames(prefix: string | undefined): SqlServerTableNames {
  const normalized = normalizePrefix(prefix);
  return {
    jobs: build(normalized, BASE.jobs),
    runs: build(normalized, BASE.runs),
    migrations: build(normalized, BASE.migrations),
    runtimeCommandReceipts: build(normalized, BASE.runtimeCommandReceipts)
  };
}
