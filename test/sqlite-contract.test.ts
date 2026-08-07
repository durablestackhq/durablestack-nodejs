import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { tmpdir } from "node:os";
import { mkdtemp, rm } from "node:fs/promises";
import { SqliteDurableJobStore } from "../src/sqlite/store.js";
import { migrateSqlite } from "../src/sqlite/migrator.js";

const enabled = process.env.DURABLESTACK_TEST_SQLITE;

type IsolatedStore = {
  store: SqliteDurableJobStore;
  cleanup(): Promise<void>;
};

async function createIsolatedStore(prefixBase: string): Promise<IsolatedStore> {
  if (!enabled) {
    throw new Error("DURABLESTACK_TEST_SQLITE is not set");
  }

  const tempDir = await mkdtemp(path.join(tmpdir(), `durablestack-sqlite-${prefixBase}-`));
  const dbPath = path.join(tempDir, "store.db");
  const prefix = `${prefixBase.slice(0, 8)}_${Date.now().toString(36)}_${Math.floor(Math.random() * 1296).toString(36).padStart(2, "0")}_`;
  const store = new SqliteDurableJobStore({ databasePath: dbPath, databaseTablePrefix: prefix });
  await store.connect();
  try {
    await migrateSqlite(store.getDatabase(), prefix);
    return {
      store,
      cleanup: async () => {
        await store.close();
        await rm(tempDir, { recursive: true, force: true });
      }
    };
  } catch (error) {
    await store.close();
    await rm(tempDir, { recursive: true, force: true });
    throw error;
  }
}

function isSameSlot(a: string | undefined, b: string): boolean {
  if (!a) {
    return false;
  }
  return Math.abs(Date.parse(a) - Date.parse(b)) <= 1;
}

test("sqlite lease fencing blocks stale completion write (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_fence");
  const { store } = isolated;
  try {
    const runId = await store.enqueue("job-a", "job-a", undefined, new Date().toISOString(), 3);
    const claimed = await store.claimDueRuns("worker-a", 1, 30);
    assert.equal(claimed.length, 1);

    const stale = await store.markSucceeded(runId, "worker-b");
    assert.equal(stale, false);

    const current = await store.markSucceeded(runId, "worker-a");
    assert.equal(current, true);
  } finally {
    await isolated.cleanup();
  }
});

test("sqlite expired lease can be reclaimed by another worker (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_recl");
  const { store } = isolated;
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
    await isolated.cleanup();
  }
});

test("sqlite enqueue-if-no-active-run deduplicates pending/leased and allows after terminal (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_dedu");
  const { store } = isolated;
  try {
    const first = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.ok(first);

    const blockedWhilePending = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.equal(blockedWhilePending, undefined);

    const [claimed] = await store.claimDueRuns("worker-a", 1, 30);
    assert.equal(claimed?.id, first);

    const blockedWhileLeased = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.equal(blockedWhileLeased, undefined);

    const done = await store.markSucceeded(first!, "worker-a");
    assert.equal(done, true);

    const allowedAfterTerminal = await store.tryEnqueueIfNoActiveRun("job-a", "job-a", undefined, new Date().toISOString(), 3);
    assert.ok(allowedAfterTerminal);
  } finally {
    await isolated.cleanup();
  }
});

test("sqlite recurring slot materialization is single-winner under race (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_slot");
  const { store } = isolated;
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
    }
  } finally {
    await isolated.cleanup();
  }
});

test("sqlite runtime command lease is single-winner under contention (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_cmdl");
  const { store } = isolated;
  try {
    const [a, b] = await Promise.all([
      store.tryLeaseRuntimeCommandReceipt("cmd-1", "worker-a", 30),
      store.tryLeaseRuntimeCommandReceipt("cmd-1", "worker-b", 30)
    ]);

    const successCount = [a, b].filter(Boolean).length;
    assert.equal(successCount, 1);
  } finally {
    await isolated.cleanup();
  }
});

test("sqlite runtime command lease re-acquisition works after expiry (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_cmdx");
  const { store } = isolated;
  try {
    const first = await store.tryLeaseRuntimeCommandReceipt("cmd-expire", "worker-a", 1);
    assert.equal(first, true);

    await new Promise((resolve) => setTimeout(resolve, 1200));

    const second = await store.tryLeaseRuntimeCommandReceipt("cmd-expire", "worker-b", 30);
    assert.equal(second, true);
  } finally {
    await isolated.cleanup();
  }
});

test("sqlite runtime command receipts support lease, ack, completion, and upload mark (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_rcpt");
  const { store } = isolated;
  try {
    const leased = await store.tryLeaseRuntimeCommandReceipt("cmd-1", "worker-a", 30);
    assert.equal(leased, true);

    const acked = await store.markRuntimeCommandAcknowledged("cmd-1", "worker-a", new Date().toISOString());
    assert.equal(acked, true);

    const succeeded = await store.markRuntimeCommandSucceeded(
      "cmd-1",
      "worker-a",
      new Date().toISOString(),
      new Date().toISOString(),
      "run-123"
    );
    assert.equal(succeeded, true);

    const receipts = await store.getRuntimeCommandReceipts(10);
    assert.equal(receipts.length, 1);
    assert.equal(receipts[0]?.status, "succeeded");

    const uploaded = await store.markRuntimeCommandReceiptUploaded("cmd-1");
    assert.equal(uploaded, true);

    const afterUpload = await store.getRuntimeCommandReceipts(10);
    assert.equal(afterUpload.length, 0);
  } finally {
    await isolated.cleanup();
  }
});

test("sqlite recurring schedule admin APIs update state (env-gated)", async (t) => {
  if (!enabled) {
    t.skip("DURABLESTACK_TEST_SQLITE is not set");
    return;
  }

  const isolated = await createIsolatedStore("it_sqlt_sched");
  const { store } = isolated;
  try {
    const registration = {
      jobName: "recurring-admin",
      jobType: "recurring-admin",
      maxAttempts: 3,
      handler: async () => {},
      recurring: {
        cronExpression: "*/1 * * * *",
        timeZone: "UTC",
        enabled: true,
        allowConcurrentRuns: false
      }
    };

    const firstNext = new Date(Date.now() + 60_000).toISOString();
    await store.upsertRecurringJob(registration, firstNext);

    const disabled = await store.setRecurringJobEnabled("recurring-admin", false, undefined);
    assert.equal(disabled, true);

    const updatedSchedule = await store.updateRecurringJobSchedule(
      "recurring-admin",
      "*/5 * * * *",
      "UTC",
      new Date(Date.now() + 120_000).toISOString()
    );
    assert.equal(updatedSchedule, true);

    const rows = await store.getRecurringJobs(true);
    const state = rows.find((x) => x.jobName === "recurring-admin");
    assert.ok(state);
    assert.equal(state?.enabled, false);
    assert.equal(state?.cronExpression, "*/5 * * * *");
  } finally {
    await isolated.cleanup();
  }
});
