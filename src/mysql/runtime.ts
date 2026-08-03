import { migrateMySql } from "./migrator.js";
import { MySqlDurableJobStore } from "./store.js";
import type { MySqlDurableStackOptions } from "./types.js";
import { createDurableStackWithStore, type DurableStackRuntime } from "../runtime.js";
import type { DurableStackEventSink, DurableStackOptions } from "../types.js";

export interface MySqlRuntimeHandle {
  runtime: DurableStackRuntime;
  closeStore(): Promise<void>;
}

export async function createDurableStackMySql(
  mySql: MySqlDurableStackOptions,
  options?: DurableStackOptions,
  sinks?: DurableStackEventSink[]
): Promise<MySqlRuntimeHandle> {
  const store = new MySqlDurableJobStore(mySql);
  await migrateMySql(store.getPool(), mySql.databaseTablePrefix);

  const runtime = createDurableStackWithStore(store, options, sinks);
  return {
    runtime,
    closeStore: () => store.close()
  };
}
