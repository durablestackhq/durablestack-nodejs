import test from "node:test";
import assert from "node:assert/strict";
import { PostgresDurableJobStore } from "../src/postgres/store.js";
import { migratePostgres } from "../src/postgres/migrator.js";

const connectionString = process.env.DURABLESTACK_TEST_POSTGRES;

async function createIsolatedStore(prefixBase: string): Promise<PostgresDurableJobStore> {
  if (!connectionString) {
    throw new Error("DURABLESTACK_TEST_POSTGRES is not set");
  }

  const prefix = `${prefixBase}_${Date.now()}_${Math.floor(Math.random() * 10000)}_`;
  const store = new PostgresDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  await migratePostgres(store.getPool(), prefix);
  return store;
}

function isSameSlot(a: string | undefined, b: string): boolean {
  if (!a) {
    return false;
  }
  return Math.abs(Date.parse(a) - Date.parse(b)) <= 1;
}

test("postgres lease fencing blocks stale completion write (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_fence");
  try {
    const runId = await store.enqueue("job-a", "job-a", undefined, new Date().toISOString(), 3);
    const claimed = await store.claimDueRuns("worker-a", 1, 30);
    assert.equal(claimed.length, 1);

    const stale = await store.markSucceeded(runId, "worker-b");
    assert.equal(stale, false);

    const current = await store.markSucceeded(runId, "worker-a");
    assert.equal(current, true);
  } finally {
    await store.close();
  }
});

test("postgres expired lease can be reclaimed by another worker (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_reclaim");
  try {
    const runId = await store.enqueue("job-a", "job-a", undefined, new Date().toISOString(), 3);
    const first = await store.claimDueRuns("worker-a", 1, 1);
    assert.equal(first.length, 1);
    assert.equal(first[0]?.id, runId);

    await new Promise((resolve) => setTimeout(resolve, 1200));

    const second = await store.claimDueRuns("worker-b", 1, 30);
    assert.equal(second.length, 1);
    assert.equal(second[0]?.id, runId);
    assert.equal(second[0]?.leaseOwner, "worker-b");
    assert.equal(second[0]?.attempt, 2);
  } finally {
    await store.close();
  }
});

test("postgres recurring slot materialization is single-winner under race (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_slot");
  try {
    const registration = {
      jobName: "recurring-a",
      jobType: "recurring-a",
      maxAttempts: 3,
      handler: async () => {},
      recurring: {
        cronExpression: "*/1 * * * *",
        timeZone: "UTC",
        enabled: true,
        allowConcurrentRuns: true
      }
    };

    const slot = new Date().toISOString();
    const next = new Date(Date.now() + 60_000).toISOString();
    await store.upsertRecurringJob(registration, slot);

    const [state] = await store.getRecurringJobs(true);
    assert.ok(state);

    const [a, b] = await Promise.all([
      store.tryMaterializeRecurringRun(state, registration, next),
      store.tryMaterializeRecurringRun(state, registration, next)
    ]);

    const successCount = [a, b].filter(Boolean).length;
    assert.ok(successCount <= 1, `expected at most one winner, got ${successCount}`);

    const runs = await store.getRunsByJobName("recurring-a", 10);
    const slotRuns = runs.filter((r) => isSameSlot(r.scheduleSlotUtc, slot));
    assert.ok(slotRuns.length <= 1, `expected at most one slot run, got ${slotRuns.length}`);

    // Under transient contention/serialization conflicts both racers can return false.
    // A follow-up non-racy attempt should still be able to materialize exactly one slot run.
    if (slotRuns.length === 0) {
      const [latest] = await store.getRecurringJobs(true);
      assert.ok(latest);
      const nextAfterLatest = new Date(Date.parse(latest.nextRunAtUtc) + 60_000).toISOString();
      const eventual = await store.tryMaterializeRecurringRun(latest, registration, nextAfterLatest);
      assert.equal(eventual, true);

      const runsAfter = await store.getRunsByJobName("recurring-a", 10);
      const slotRunsAfter = runsAfter.filter((r) => isSameSlot(r.scheduleSlotUtc, slot));
      assert.equal(slotRunsAfter.length, 1);
    }
  } finally {
    await store.close();
  }
});

test("postgres runtime command lease is single-winner under contention (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_cmdlease");
  try {
    const [a, b] = await Promise.all([
      store.tryLeaseRuntimeCommandReceipt("cmd-1", "worker-a", 30),
      store.tryLeaseRuntimeCommandReceipt("cmd-1", "worker-b", 30)
    ]);

    const successCount = [a, b].filter(Boolean).length;
    assert.equal(successCount, 1);
  } finally {
    await store.close();
  }
});
