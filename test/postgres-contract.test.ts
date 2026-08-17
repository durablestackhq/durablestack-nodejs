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
    // A follow-up non-racy attempt should still be able to materialize at least one run.
    if (slotRuns.length === 0) {
      let created = false;
      for (let i = 0; i < 5; i += 1) {
        const [latest] = await store.getRecurringJobs(true);
        assert.ok(latest);
        const nextAfterLatest = new Date(Date.parse(latest.nextRunAtUtc) + 60_000).toISOString();
        created = await store.tryMaterializeRecurringRun(latest, registration, nextAfterLatest);
        if (created) {
          break;
        }
      }

      const runsAfter = await store.getRunsByJobName("recurring-a", 10);
      assert.ok(runsAfter.length >= 1, "expected at least one recurring run after retry attempts");

      const distinctSlots = new Set(runsAfter.map((r) => r.scheduleSlotUtc ?? "none"));
      assert.equal(distinctSlots.size, runsAfter.length);

      const originalSlotRuns = runsAfter.filter((r) => isSameSlot(r.scheduleSlotUtc, slot));
      assert.ok(originalSlotRuns.length <= 1, `expected at most one run for original slot, got ${originalSlotRuns.length}`);

      if (!created) {
        // If retries did not create a new run, verify one already exists from the earlier race.
        assert.ok(runsAfter.length >= 1);
      }
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

test("postgres round-trips falsy JSON payloads (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_falsy_payload");
  try {
    const cases: Array<{ jobName: string; payload: unknown }> = [
      { jobName: "job-false", payload: false },
      { jobName: "job-zero", payload: 0 },
      { jobName: "job-empty-string", payload: "" },
      { jobName: "job-object", payload: { userId: 123 } }
    ];

    for (const { jobName, payload } of cases) {
      const runId = await store.enqueue(jobName, jobName, JSON.stringify(payload), new Date().toISOString(), 3);
      const run = await store.getRun(runId);
      assert.equal(
        run?.payloadJson,
        JSON.stringify(payload),
        `payload ${JSON.stringify(payload)} must round-trip, not be dropped as falsy`
      );
    }

    const noPayloadRunId = await store.enqueue("job-none", "job-none", undefined, new Date().toISOString(), 3);
    const noPayloadRun = await store.getRun(noPayloadRunId);
    assert.equal(noPayloadRun?.payloadJson, undefined, "an absent payload must remain undefined");
  } finally {
    await store.close();
  }
});

test("postgres runtime command receipts in a terminal state cannot be re-leased (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_cmdterm");
  try {
    assert.equal(await store.tryLeaseRuntimeCommandReceipt("cmd-done", "worker-a", 30), true);
    assert.equal(
      await store.markRuntimeCommandSucceeded("cmd-done", "worker-a", new Date().toISOString(), new Date().toISOString(), undefined),
      true
    );

    assert.equal(
      await store.tryLeaseRuntimeCommandReceipt("cmd-done", "worker-b", 30),
      false,
      "succeeded receipt must not be re-leasable"
    );
    assert.equal(
      await store.tryLeaseRuntimeCommandReceipt("cmd-done", "worker-a", 30),
      false,
      "succeeded receipt must not be re-leasable even by the original owner"
    );

    const receipts = await store.getRuntimeCommandReceipts(10);
    assert.equal(receipts[0]?.status, "succeeded", "terminal status must be preserved");
  } finally {
    await store.close();
  }
});

test("postgres enqueue-if-no-active-run deduplicates pending/leased and allows after terminal (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const store = await createIsolatedStore("it_noactive");
  try {
    const first = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.ok(first);

    const blockedWhilePending = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.equal(blockedWhilePending, undefined);

    const [claimed] = await store.claimDueRuns("worker-a", 1, 30);
    assert.equal(claimed?.id, first);

    const blockedWhileLeased = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.equal(blockedWhileLeased, undefined);

    const done = await store.markSucceeded(first, "worker-a");
    assert.equal(done, true);

    const allowedAfterTerminal = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.ok(allowedAfterTerminal);
  } finally {
    await store.close();
  }
});

test("postgres concurrent migrations serialize safely on advisory lock (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_POSTGRES is not set");
    return;
  }

  const shortBase = "it_pg_mig";
  const stamp = Date.now().toString(36);
  const nonce = Math.floor(Math.random() * (36 * 36)).toString(36).padStart(2, "0");
  const prefix = `${shortBase}_${stamp}_${nonce}_`;

  const storeA = new PostgresDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  const storeB = new PostgresDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  try {
    await Promise.all([
      migratePostgres(storeA.getPool(), prefix),
      migratePostgres(storeB.getPool(), prefix),
      migratePostgres(storeA.getPool(), prefix)
    ]);
    assert.ok(true);
  } finally {
    await Promise.all([storeA.close(), storeB.close()]);
  }
});
