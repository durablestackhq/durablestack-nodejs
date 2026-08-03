import { migratePostgres } from "./migrator.js";
import { PostgresDurableJobStore } from "./store.js";
import type { PostgresDurableStackOptions } from "./types.js";
import { createDurableStackWithStore, type DurableStackRuntime } from "../runtime.js";
import type { DurableStackEventSink, DurableStackOptions } from "../types.js";

export interface PostgresRuntimeHandle {
  runtime: DurableStackRuntime;
  closeStore(): Promise<void>;
}

export async function createDurableStackPostgres(
  postgres: PostgresDurableStackOptions,
  options?: DurableStackOptions,
  sinks?: DurableStackEventSink[]
): Promise<PostgresRuntimeHandle> {
  const store = new PostgresDurableJobStore(postgres);
  await migratePostgres(store.getPool(), postgres.databaseTablePrefix);

  const runtime = createDurableStackWithStore(store, options, sinks);
  return {
    runtime,
    closeStore: () => store.close()
  };
}
