import test from "node:test";
import assert from "node:assert/strict";
import { createDurableStack } from "../src/runtime.js";
import { InMemoryDurableJobStore } from "../src/in-memory-store.js";
import { DurableStackProcessor } from "../src/processor.js";
import { DurableJobRegistry } from "../src/registry.js";
import { normalizeOptions, getEffectiveRunRetentionSeconds } from "../src/options.js";
import { resolveMySqlTableNames } from "../src/mysql/table-names.js";
import { addSeconds, nowIso } from "../src/utils.js";

test("durable retention default is 24h when not configured", () => {
  const options = normalizeOptions(undefined);
  assert.equal(getEffectiveRunRetentionSeconds(options), 86_400);
});

test("mysql table name resolver preserves prefix casing", () => {
  const tables = resolveMySqlTableNames("App_");
  assert.equal(tables.jobs, "App_durable_stack_jobs");
  assert.equal(tables.runs, "App_durable_stack_job_runs");
});

test("fixed retry behavior keeps approximately constant retry spacing", async () => {
  const store = new InMemoryDurableJobStore();
  const registry = new DurableJobRegistry();

  const retryAtValues: string[] = [];
  registry.register({
    jobName: "fixed-retry",
    jobType: "fixed-retry",
    maxAttempts: 3,
    retryBehavior: "fixed",
    retryInitialDelaySeconds: 1,
    handler: async () => {
      throw new Error("fail");
    }
  });

  const options = normalizeOptions({
    workerName: "worker-fixed",
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    leaseDurationSeconds: 5,
    retryJitterEnabled: false,
    retryDelaySeconds: 1,
    retryMaxDelaySeconds: 300
  });

  const processor = new DurableStackProcessor(store, registry, options, []);
  const runId = await store.enqueue("fixed-retry", "fixed-retry", undefined, nowIso(), 3);

  for (let i = 0; i < 120 && retryAtValues.length < 2; i += 1) {
    await processor.processOnce(new AbortController().signal);
    const run = await store.getRun(runId);
    if (!run) {
      break;
    }
    if (run.status === "pending") {
      if (retryAtValues.length === 0 || retryAtValues[retryAtValues.length - 1] !== run.scheduledForUtc) {
        retryAtValues.push(run.scheduledForUtc);
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  assert.equal(retryAtValues.length >= 2, true);
  const firstRetryAt = Date.parse(retryAtValues[0]!);
  const secondRetryAt = Date.parse(retryAtValues[1]!);

  const spacing = secondRetryAt - firstRetryAt;
  assert.ok(spacing >= 900 && spacing <= 1300, `expected ~1000ms spacing, got ${spacing}`);
});

test("backoff retry behavior increases delay by attempt", async () => {
  const store = new InMemoryDurableJobStore();
  const registry = new DurableJobRegistry();

  const retryAtValues: string[] = [];
  registry.register({
    jobName: "backoff-retry",
    jobType: "backoff-retry",
    maxAttempts: 4,
    retryBehavior: "backoff",
    retryInitialDelaySeconds: 1,
    handler: async () => {
      throw new Error("fail");
    }
  });

  const options = normalizeOptions({
    workerName: "worker-backoff",
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    leaseDurationSeconds: 5,
    retryJitterEnabled: false,
    retryDelaySeconds: 1,
    retryMaxDelaySeconds: 300
  });

  const processor = new DurableStackProcessor(store, registry, options, []);
  const runId = await store.enqueue("backoff-retry", "backoff-retry", undefined, addSeconds(nowIso(), -2), 4);

  for (let i = 0; i < 180 && retryAtValues.length < 2; i += 1) {
    await processor.processOnce(new AbortController().signal);
    const run = await store.getRun(runId);
    if (!run) {
      break;
    }
    if (run.status === "pending") {
      if (retryAtValues.length === 0 || retryAtValues[retryAtValues.length - 1] !== run.scheduledForUtc) {
        retryAtValues.push(run.scheduledForUtc);
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  assert.equal(retryAtValues.length >= 2, true);
  const firstRetryAt = Date.parse(retryAtValues[0]!);
  const secondRetryAt = Date.parse(retryAtValues[1]!);

  const spacing = secondRetryAt - firstRetryAt;
  assert.ok(spacing >= 1800 && spacing <= 2600, `expected ~2000ms spacing, got ${spacing}`);
});

test("runtime getEnqueuedRuns only returns pending one-off runs", async () => {
  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 3,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2
  });

  let hold = true;
  runtime.registerJob("slow", async () => {
    while (hold) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  });

  await runtime.start();
  const runId = await runtime.enqueue("slow");

  await new Promise((resolve) => setTimeout(resolve, 200));
  const enqueued = await runtime.getEnqueuedRuns(20);
  assert.equal(enqueued.find((x) => x.id === runId), undefined);

  hold = false;
  await runtime.stop();
});
