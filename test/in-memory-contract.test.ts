import test from "node:test";
import assert from "node:assert/strict";
import { InMemoryDurableJobStore } from "../src/in-memory-store.js";

test("lease fencing rejects stale worker completion writes", async () => {
  const store = new InMemoryDurableJobStore();
  const runId = await store.enqueue("job-a", "job-a", undefined, new Date().toISOString(), 3);

  const [claimed] = await store.claimDueRuns("worker-a", 1, 30);
  assert.ok(claimed);

  const staleSuccess = await store.markSucceeded(runId, "worker-b");
  assert.equal(staleSuccess, false);

  const currentSuccess = await store.markSucceeded(runId, "worker-a");
  assert.equal(currentSuccess, true);

  const final = await store.getRun(runId);
  assert.equal(final?.status, "succeeded");
});

test("expired lease can be reclaimed by another worker", async () => {
  const store = new InMemoryDurableJobStore();
  const runId = await store.enqueue("job-a", "job-a", undefined, new Date().toISOString(), 3);

  const [first] = await store.claimDueRuns("worker-a", 1, 1);
  assert.ok(first);

  await new Promise((resolve) => setTimeout(resolve, 1100));

  const reclaimed = await store.claimDueRuns("worker-b", 1, 30);
  assert.equal(reclaimed.length, 1);
  assert.equal(reclaimed[0]?.id, runId);
  assert.equal(reclaimed[0]?.leaseOwner, "worker-b");
  assert.equal(reclaimed[0]?.attempt, 2);
});

test("failed run can transition back to pending for retry and then succeed", async () => {
  const store = new InMemoryDurableJobStore();
  const runId = await store.enqueue("job-a", "job-a", undefined, new Date().toISOString(), 3);

  const [claimed] = await store.claimDueRuns("worker-a", 1, 30);
  assert.ok(claimed);

  const retryAt = new Date(Date.now() + 250).toISOString();
  const markedRetry = await store.markFailed(runId, "worker-a", "transient", true, retryAt);
  assert.equal(markedRetry, true);

  const afterFail = await store.getRun(runId);
  assert.equal(afterFail?.status, "pending");

  await new Promise((resolve) => setTimeout(resolve, 280));

  const [retryClaim] = await store.claimDueRuns("worker-a", 1, 30);
  assert.ok(retryClaim);
  assert.equal(retryClaim.id, runId);
  assert.equal(retryClaim.attempt, 2);

  const success = await store.markSucceeded(runId, "worker-a");
  assert.equal(success, true);
});

test("recurring materialization enforces slot uniqueness", async () => {
  const store = new InMemoryDurableJobStore();

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

  const first = await store.tryMaterializeRecurringRun(state, registration, next);
  assert.equal(first, true);

  const second = await store.tryMaterializeRecurringRun(state, registration, next);
  assert.equal(second, false);
});

test("retention pruning deletes terminal runs but preserves active runs", async () => {
  const store = new InMemoryDurableJobStore();

  const pendingId = await store.enqueue("pending", "pending", undefined, new Date(Date.now() + 3_600_000).toISOString(), 3);
  const leasedId = await store.enqueue("leased", "leased", undefined, new Date().toISOString(), 3);
  const succeededId = await store.enqueue("done", "done", undefined, new Date().toISOString(), 3);
  const failedId = await store.enqueue("failed", "failed", undefined, new Date().toISOString(), 3);

  const leasedClaim = await store.claimDueRuns("worker-a", 1, 60);
  assert.equal(leasedClaim[0]?.id, leasedId);

  const successClaim = await store.claimDueRuns("worker-b", 1, 60);
  assert.equal(successClaim[0]?.id, succeededId);
  await store.markSucceeded(succeededId, "worker-b");

  const failedClaim = await store.claimDueRuns("worker-c", 1, 60);
  assert.equal(failedClaim[0]?.id, failedId);
  await store.markFailed(failedId, "worker-c", "terminal", false, undefined);

  const pruned = await store.pruneHistoricalRuns(new Date(Date.now() + 1000).toISOString(), 100);
  assert.ok(pruned >= 2);

  const pending = await store.getRun(pendingId);
  const leased = await store.getRun(leasedId);
  const succeeded = await store.getRun(succeededId);
  const failed = await store.getRun(failedId);

  assert.equal(pending?.status, "pending");
  assert.equal(leased?.status, "leased");
  assert.equal(succeeded, undefined);
  assert.equal(failed, undefined);
});

test("runtime command receipts support lease, ack, completion, and upload mark", async () => {
  const store = new InMemoryDurableJobStore();

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
});
