import test from "node:test";
import assert from "node:assert/strict";
import { MySqlDurableJobStore } from "../src/mysql/store.js";
import { migrateMySql } from "../src/mysql/migrator.js";

const connectionString = process.env.DURABLESTACK_TEST_MYSQL;

function isSameSlot(a: string | undefined, b: string): boolean {
  if (!a) {
    return false;
  }
  return Math.abs(Date.parse(a) - Date.parse(b)) <= 1;
}

async function createIsolatedStore(prefixBase: string): Promise<{ store: MySqlDurableJobStore; prefix: string }> {
  if (!connectionString) {
    throw new Error("DURABLESTACK_TEST_MYSQL is not set");
  }

  const shortBase = prefixBase.slice(0, 8);
  const stamp = Date.now().toString(36);
  const nonce = Math.floor(Math.random() * (36 * 36)).toString(36).padStart(2, "0");
  const prefix = `${shortBase}_${stamp}_${nonce}_`;
  const store = new MySqlDurableJobStore({ connectionString, databaseTablePrefix: prefix });
  try {
    await migrateMySql(store.getPool(), prefix);
    return { store, prefix };
  } catch (error) {
    await store.close();
    throw error;
  }
}

test("mysql lease fencing blocks stale completion write (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_fence");
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

test("mysql expired lease can be reclaimed by another worker (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_reclaim");
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

test("mysql enqueue-if-no-active-run deduplicates pending/leased and allows after terminal (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_noactive");
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
    await store.close();
  }
});

test("mysql recurring slot materialization is single-winner under race (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_slot");
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

      const originalSlotRuns = runsAfter.filter((r) => isSameSlot(r.scheduleSlotUtc, slot));
      assert.ok(originalSlotRuns.length <= 1, `expected at most one run for original slot, got ${originalSlotRuns.length}`);
    }
  } finally {
    await store.close();
  }
});

test("mysql runtime command lease is single-winner under contention (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_cmdlease");
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

test("mysql runtime command receipts in a terminal state cannot be re-leased (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_cmdterm");
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

test("mysql schema migration is idempotent under repeated execution (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store, prefix } = await createIsolatedStore("it_mysql_migid");
  try {
    await migrateMySql(store.getPool(), prefix);
    await migrateMySql(store.getPool(), prefix);
    assert.ok(true);
  } finally {
    await store.close();
  }
});

test("mysql concurrent migrations do not deadlock on the advisory lock (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store, prefix } = await createIsolatedStore("it_mysql_migpar");
  try {
    // GET_LOCK is per-connection; if the lock were acquired and released on
    // different pooled connections, the second migrator would time out.
    await Promise.all([
      migrateMySql(store.getPool(), prefix),
      migrateMySql(store.getPool(), prefix),
      migrateMySql(store.getPool(), prefix)
    ]);
    assert.ok(true);
  } finally {
    await store.close();
  }
});

test("mysql recurring race fallback eventually creates a run under contention (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_slotfb");
  try {
    const registration = {
      jobName: "recurring-fallback",
      jobType: "recurring-fallback",
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
    await store.upsertRecurringJob(registration, slot);
    const [state] = await store.getRecurringJobs(true);
    assert.ok(state);

    const created = await store.tryMaterializeRecurringRun(state, registration, new Date(Date.now() + 60_000).toISOString());
    if (!created) {
      for (let i = 0; i < 5; i += 1) {
        const [latest] = await store.getRecurringJobs(true);
        assert.ok(latest);
        const next = new Date(Date.parse(latest.nextRunAtUtc) + 60_000).toISOString();
        const result = await store.tryMaterializeRecurringRun(latest, registration, next);
        if (result) {
          break;
        }
      }
    }

    const runs = await store.getRunsByJobName("recurring-fallback", 20);
    assert.ok(runs.length >= 1);
    const distinctSlots = new Set(runs.map((x) => x.scheduleSlotUtc ?? "none"));
    assert.equal(distinctSlots.size, runs.length);
  } finally {
    await store.close();
  }
});

test("mysql runtime command lease re-acquisition works after expiry (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_cmdexp");
  try {
    const first = await store.tryLeaseRuntimeCommandReceipt("cmd-expire", "worker-a", 1);
    assert.equal(first, true);

    await new Promise((resolve) => setTimeout(resolve, 1200));

    const second = await store.tryLeaseRuntimeCommandReceipt("cmd-expire", "worker-b", 30);
    assert.equal(second, true);
  } finally {
    await store.close();
  }
});

test("mysql runtime command receipts support lease, ack, completion, and upload mark (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_rcpt");
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
    await store.close();
  }
});

test("mysql recurring schedule admin APIs update state (env-gated)", async (t) => {
  if (!connectionString) {
    t.skip("DURABLESTACK_TEST_MYSQL is not set");
    return;
  }

  const { store } = await createIsolatedStore("it_mysql_sched");
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
    await store.close();
  }
});
