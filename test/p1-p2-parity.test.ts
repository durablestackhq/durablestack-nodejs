import test from "node:test";
import assert from "node:assert/strict";
import { createDurableStack } from "../src/runtime.js";
import { InMemoryDurableJobStore } from "../src/in-memory-store.js";
import { DurableStackProcessor } from "../src/processor.js";
import { DurableJobRegistry } from "../src/registry.js";
import { normalizeOptions, getEffectiveRunRetentionSeconds } from "../src/options.js";
import { resolveMySqlTableNames } from "../src/mysql/table-names.js";
import { addSeconds, nowIso } from "../src/utils.js";
import { EVENT_TYPES } from "../src/constants.js";
import type { DurableStackEvent, DurableStackEventSink } from "../src/types.js";

test("durable retention default is 24h when not configured", () => {
  const options = normalizeOptions(undefined);
  assert.equal(getEffectiveRunRetentionSeconds(options), 86_400);
});

test("getEffectiveRunRetentionSeconds falls back to the default for zero or negative values", () => {
  // A zero or negative runRetentionSeconds must be treated as unset, not taken literally:
  // taken literally, the prune sweep's cutoff timestamp would be >= now, deleting every
  // currently completed run on the next sweep. Matches the .NET runtime's
  // DurableStackRetentionOptions.GetEffectiveRunRetention (seconds <= 0 -> default).
  const zero = normalizeOptions({ retention: { runRetentionSeconds: 0 } });
  assert.equal(getEffectiveRunRetentionSeconds(zero), 86_400);

  const negative = normalizeOptions({ retention: { runRetentionSeconds: -60 } });
  assert.equal(getEffectiveRunRetentionSeconds(negative), 86_400);

  const positive = normalizeOptions({ retention: { runRetentionSeconds: 3_600 } });
  assert.equal(getEffectiveRunRetentionSeconds(positive), 3_600);
});

test("shutdownDrainTimeoutSeconds accepts zero as a meaningful value but rejects negatives", () => {
  // Zero means "don't wait for in-flight runs to drain at all" and must be honored
  // literally, not silently coerced to the default the way ensurePositive would.
  const zero = normalizeOptions({ shutdownDrainTimeoutSeconds: 0 });
  assert.equal(zero.shutdownDrainTimeoutSeconds, 0);

  const negative = normalizeOptions({ shutdownDrainTimeoutSeconds: -5 });
  assert.equal(negative.shutdownDrainTimeoutSeconds, 10, "a negative value is meaningless and must fall back to the default");

  const positive = normalizeOptions({ shutdownDrainTimeoutSeconds: 30 });
  assert.equal(positive.shutdownDrainTimeoutSeconds, 30);

  const defaulted = normalizeOptions(undefined);
  assert.equal(defaulted.shutdownDrainTimeoutSeconds, 10);
});

test("autodiscovery baseDir defaults to the process working directory, not the package's own install location", () => {
  const options = normalizeOptions(undefined);
  assert.equal(
    options.autodiscovery.baseDir,
    process.cwd(),
    "job files live in the consuming application; scanning must default to cwd, not a path derived from this package's own location (which resolves into node_modules once installed as a dependency)"
  );
});

test("autodiscovery baseDir honors an explicit override", () => {
  const options = normalizeOptions({ autodiscovery: { baseDir: "/custom/jobs/dir" } });
  assert.equal(options.autodiscovery.baseDir, "/custom/jobs/dir");
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

test("retry behavior defaults to fixed delay when unspecified, matching the .NET runtime", async () => {
  const store = new InMemoryDurableJobStore();
  const registry = new DurableJobRegistry();

  const retryAtValues: string[] = [];
  registry.register({
    jobName: "default-retry",
    jobType: "default-retry",
    maxAttempts: 3,
    retryInitialDelaySeconds: 1,
    handler: async () => {
      throw new Error("fail");
    }
  });

  const options = normalizeOptions({
    workerName: "worker-default",
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    leaseDurationSeconds: 5,
    retryJitterEnabled: false,
    retryDelaySeconds: 1,
    retryMaxDelaySeconds: 300
  });

  const processor = new DurableStackProcessor(store, registry, options, []);
  const runId = await store.enqueue("default-retry", "default-retry", undefined, nowIso(), 3);

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
  assert.ok(spacing >= 900 && spacing <= 1300, `expected ~1000ms constant spacing (fixed default), got ${spacing}`);
});

test("a run claimed for an unregistered job name is retried like any other failure, not failed permanently", async () => {
  const store = new InMemoryDurableJobStore();
  // An empty registry: simulates a worker in a rolling deployment that hasn't
  // picked up the job definition another worker enqueued this run for.
  const registry = new DurableJobRegistry();

  const events: DurableStackEvent[] = [];
  const sink: DurableStackEventSink = {
    publish: async (event) => {
      events.push(event);
    }
  };

  const options = normalizeOptions({
    workerName: "worker-missing-registration",
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    leaseDurationSeconds: 5,
    retryJitterEnabled: false,
    retryDelaySeconds: 1,
    retryMaxDelaySeconds: 300
  });

  const processor = new DurableStackProcessor(store, registry, options, [sink]);
  const runId = await store.enqueue("unregistered-job", "unregistered-job", undefined, nowIso(), 3);

  await processor.processOnce(new AbortController().signal);
  await new Promise((resolve) => setTimeout(resolve, 25));

  const run = await store.getRun(runId);
  assert.ok(run, "the run must still exist");
  assert.equal(run?.status, "pending", "attempt 1 of 3 must be retried, not failed permanently");
  assert.ok(run?.scheduledForUtc, "a retry time must be scheduled");

  const failedEvent = events.find((e) => e.eventType === EVENT_TYPES.JOB_FAILED && e.runId === runId);
  const retriedEvent = events.find((e) => e.eventType === EVENT_TYPES.JOB_RETRIED && e.runId === runId);
  assert.ok(failedEvent, "a job_failed event must be published, same as any other execution failure");
  assert.ok(retriedEvent, "a job_retried event must be published since attempts remain");
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
