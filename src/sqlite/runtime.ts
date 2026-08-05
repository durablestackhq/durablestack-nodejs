import { migrateSqlite } from "./migrator.js";
import { SqliteDurableJobStore } from "./store.js";
import type { SqliteDurableStackOptions } from "./types.js";
import { createDurableStackWithStore, type DurableStackRuntime } from "../runtime.js";
import type { DurableStackEventSink, DurableStackOptions } from "../types.js";

export interface SqliteRuntimeHandle {
  runtime: DurableStackRuntime;
  closeStore(): Promise<void>;
}

export async function createDurableStackSqlite(
  sqlite: SqliteDurableStackOptions,
  options?: DurableStackOptions,
  sinks?: DurableStackEventSink[]
): Promise<SqliteRuntimeHandle> {
  const store = new SqliteDurableJobStore(sqlite);
  await store.connect();
  await migrateSqlite(store.getDatabase(), sqlite.databaseTablePrefix);

  const runtime = createDurableStackWithStore(store, options, sinks);
  return {
    runtime,
    closeStore: () => store.close()
  };
}
