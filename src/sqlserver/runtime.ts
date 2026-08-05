import { migrateSqlServer } from "./migrator.js";
import { SqlServerDurableJobStore } from "./store.js";
import type { SqlServerDurableStackOptions } from "./types.js";
import { createDurableStackWithStore, type DurableStackRuntime } from "../runtime.js";
import type { DurableStackEventSink, DurableStackOptions } from "../types.js";

export interface SqlServerRuntimeHandle {
  runtime: DurableStackRuntime;
  closeStore(): Promise<void>;
}

export async function createDurableStackSqlServer(
  sqlServer: SqlServerDurableStackOptions,
  options?: DurableStackOptions,
  sinks?: DurableStackEventSink[]
): Promise<SqlServerRuntimeHandle> {
  const store = new SqlServerDurableJobStore(sqlServer);
  await store.connect();
  await migrateSqlServer(store.getPool(), sqlServer.databaseTablePrefix);

  const runtime = createDurableStackWithStore(store, options, sinks);
  return {
    runtime,
    closeStore: () => store.close()
  };
}
