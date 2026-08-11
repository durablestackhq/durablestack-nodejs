import test from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createDurableStack, createDurableStackWithStore } from "../src/runtime.js";
import { InMemoryDurableJobStore } from "../src/in-memory-store.js";
import { DurableStackProcessor } from "../src/processor.js";
import { DurableJobRegistry } from "../src/registry.js";
import { normalizeOptions } from "../src/options.js";

const fixturesDir = join(fileURLToPath(new URL(".", import.meta.url)), "fixtures", "autodiscovery");

test("enqueue and process a one-off job", async () => {
  let count = 0;

  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 2,
    maxConcurrentRuns: 2,
    shutdownDrainTimeoutSeconds: 2
  });

  runtime.registerJob("increment", async () => {
    count += 1;
  });

  await runtime.start();
  const runId = await runtime.enqueue("increment", { value: 1 });

  let run;
  for (let i = 0; i < 40; i += 1) {
    run = await runtime.getRun(runId);
    if (run?.status === "succeeded") {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  await runtime.stop();

  assert.equal(count, 1);
  assert.ok(run);
  assert.equal(run?.status, "succeeded");
});

test("recurring job materializes and executes", async () => {
  let hit = 0;

  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2
  });

  runtime.registerRecurring("heartbeat", "*/1 * * * * *", "UTC", async () => {
    hit += 1;
  });

  await runtime.start();
  await new Promise((resolve) => setTimeout(resolve, 1300));
  await runtime.stop();

  assert.ok(hit >= 1);
});

test("autodiscovery registers a discovered job before processing", async () => {
  (globalThis as { __autodiscoveryHit?: number }).__autodiscoveryHit = 0;

  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2,
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/valid.jobs.mjs"],
      excludeGlobs: []
    }
  });

  await runtime.start();
  const runId = await runtime.enqueue("auto-valid", { value: 1 });

  let run;
  for (let i = 0; i < 40; i += 1) {
    run = await runtime.getRun(runId);
    if (run?.status === "succeeded") {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  await runtime.stop();

  assert.equal((globalThis as { __autodiscoveryHit?: number }).__autodiscoveryHit, 1);
  assert.equal(run?.status, "succeeded");
});

test("autodiscovery include/exclude filters are respected", async () => {
  (globalThis as { __autodiscoveryHit?: number }).__autodiscoveryHit = 0;

  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2,
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/*.jobs.mjs"],
      excludeGlobs: ["**/excluded.jobs.mjs", "**/invalid.jobs.mjs"]
    }
  });

  await runtime.start();
  await assert.rejects(async () => runtime.enqueue("auto-excluded"), /No registered job named/);

  const runId = await runtime.enqueue("auto-valid", { value: 1 });
  let run;
  for (let i = 0; i < 40; i += 1) {
    run = await runtime.getRun(runId);
    if (run?.status === "succeeded") {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  await runtime.stop();

  assert.equal((globalThis as { __autodiscoveryHit?: number }).__autodiscoveryHit, 1);
  assert.equal(run?.status, "succeeded");
});

test("autodiscovery fails startup when failOnError is true", async () => {
  const runtime = createDurableStack({
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/invalid.jobs.mjs"],
      failOnError: true
    }
  });

  await assert.rejects(async () => runtime.start(), /Autodiscovery failed:/);
});

test("autodiscovery recurring job materializes and executes", async () => {
  (globalThis as { __autodiscoveryRecurringHit?: number }).__autodiscoveryRecurringHit = 0;

  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2,
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/recurring.jobs.mjs"],
      excludeGlobs: []
    }
  });

  await runtime.start();
  await new Promise((resolve) => setTimeout(resolve, 1300));
  await runtime.stop();

  assert.ok(((globalThis as { __autodiscoveryRecurringHit?: number }).__autodiscoveryRecurringHit ?? 0) >= 1);
});

test("autodiscovery duplicate name conflicts with explicit registration", async () => {
  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2,
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/duplicate.jobs.mjs"],
      excludeGlobs: [],
      failOnError: true
    }
  });

  runtime.registerJob("duplicate-job", async () => {});
  await assert.rejects(async () => runtime.start(), /already registered/);
});

test("autodiscovery continue-on-error skips invalid modules when failOnError is false", async () => {
  (globalThis as { __autodiscoveryHit?: number }).__autodiscoveryHit = 0;

  const runtime = createDurableStack({
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2,
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/valid.jobs.mjs", "**/invalid.jobs.mjs"],
      excludeGlobs: [],
      failOnError: false
    }
  });

  await runtime.start();
  const runId = await runtime.enqueue("auto-valid", { value: 1 });

  let run;
  for (let i = 0; i < 40; i += 1) {
    run = await runtime.getRun(runId);
    if (run?.status === "succeeded") {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  await runtime.stop();

  assert.equal((globalThis as { __autodiscoveryHit?: number }).__autodiscoveryHit, 1);
  assert.equal(run?.status, "succeeded");
});

test("autodiscovery maxModules guard fails startup", async () => {
  const runtime = createDurableStack({
    autodiscovery: {
      enabled: true,
      baseDir: fixturesDir,
      includeGlobs: ["**/*.jobs.mjs"],
      excludeGlobs: [],
      maxModules: 1,
      failOnError: true
    }
  });

  await assert.rejects(async () => runtime.start(), /exceeds maxModules/);
});

test("processor initializes recurring with KeepDatabase and disables orphans by default", async () => {
  const store = new InMemoryDurableJobStore();

  await store.upsertRecurringJob({
    jobName: "shared",
    jobType: "shared",
    maxAttempts: 3,
    recurring: {
      cronExpression: "*/1 * * * *",
      timeZone: "UTC",
      enabled: true,
      allowConcurrentRuns: false
    },
    handler: async () => {}
  }, new Date(Date.now() + 60_000).toISOString());

  await store.upsertRecurringJob({
    jobName: "orphan",
    jobType: "orphan",
    maxAttempts: 3,
    recurring: {
      cronExpression: "*/1 * * * *",
      timeZone: "UTC",
      enabled: true,
      allowConcurrentRuns: false
    },
    handler: async () => {}
  }, new Date(Date.now() + 60_000).toISOString());

  const existingBefore = (await store.getRecurringJobs(true)).find((x) => x.jobName === "shared");
  const existingNext = existingBefore?.nextRunAtUtc;
  assert.ok(existingNext);

  const registry = new DurableJobRegistry();
  registry.register({
    jobName: "shared",
    jobType: "shared",
    maxAttempts: 3,
    recurring: {
      cronExpression: "*/5 * * * *",
      timeZone: "UTC",
      enabled: true,
      allowConcurrentRuns: false
    },
    handler: async () => {}
  });

  const options = normalizeOptions({
    recurring: {
      registrationSync: {
        existingJobBehavior: "KeepDatabase",
        orphanedJobBehavior: "Disable"
      }
    }
  });

  const processor = new DurableStackProcessor(store, registry, options, []);
  await processor.initializeRecurringJobs();

  const sharedAfter = (await store.getRecurringJobs(true)).find((x) => x.jobName === "shared");
  assert.equal(sharedAfter?.nextRunAtUtc, existingNext);

  const orphanAfter = (await store.getRecurringJobs(true)).find((x) => x.jobName === "orphan");
  assert.equal(orphanAfter?.enabled, false);
});

test("processor initialize recurring UpdateFromCode updates existing schedule", async () => {
  const store = new InMemoryDurableJobStore();

  await store.upsertRecurringJob({
    jobName: "shared",
    jobType: "shared",
    maxAttempts: 3,
    recurring: {
      cronExpression: "*/1 * * * *",
      timeZone: "UTC",
      enabled: true,
      allowConcurrentRuns: false
    },
    handler: async () => {}
  }, new Date(Date.now() + 60_000).toISOString());

  const registry = new DurableJobRegistry();
  registry.register({
    jobName: "shared",
    jobType: "shared",
    maxAttempts: 3,
    recurring: {
      cronExpression: "*/5 * * * *",
      timeZone: "UTC",
      enabled: true,
      allowConcurrentRuns: false
    },
    handler: async () => {}
  });

  const options = normalizeOptions({
    recurring: {
      registrationSync: {
        existingJobBehavior: "UpdateFromCode",
        orphanedJobBehavior: "Ignore"
      }
    }
  });

  const processor = new DurableStackProcessor(store, registry, options, []);
  await processor.initializeRecurringJobs();

  const sharedAfter = (await store.getRecurringJobs(true)).find((x) => x.jobName === "shared");
  assert.equal(sharedAfter?.cronExpression, "*/5 * * * *");
});

test("runtime cancels handler when lease extension fails", async () => {
  class LeaseLosingStore extends InMemoryDurableJobStore {
    private extendCalls = 0;

    override async extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
      this.extendCalls += 1;
      if (this.extendCalls >= 2) {
        return false;
      }
      return super.extendLease(runId, workerName, leaseDurationSeconds);
    }
  }

  const store = new LeaseLosingStore();
  const runtime = createDurableStackWithStore(store, {
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 1,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2
  });

  let abortSeen = false;
  runtime.registerJob("slow", async (_payload, _ctx, signal) => {
    while (!signal.aborted) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    abortSeen = true;
    throw new Error("lease_lost_abort");
  });

  await runtime.start();
  const runId = await runtime.enqueue("slow");

  await new Promise((resolve) => setTimeout(resolve, 1700));
  const run = await runtime.getRun(runId);
  await runtime.stop();

  assert.equal(abortSeen, true);
  assert.ok(run);
  assert.notEqual(run?.status, "succeeded");
});

test("runtime shutdown does not record failed run for cancellation", async () => {
  const store = new InMemoryDurableJobStore();
  const runtime = createDurableStackWithStore(store, {
    pollIntervalSeconds: 0.1,
    leaseDurationSeconds: 2,
    claimBatchSize: 1,
    maxConcurrentRuns: 1,
    shutdownDrainTimeoutSeconds: 2
  });

  runtime.registerJob("shutdown-sensitive", async (_payload, _ctx, signal) => {
    while (!signal.aborted) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error("aborted_by_shutdown");
  });

  await runtime.start();
  const runId = await runtime.enqueue("shutdown-sensitive");

  let leasedObserved = false;
  for (let i = 0; i < 20; i += 1) {
    const run = await runtime.getRun(runId);
    if (run?.status === "leased") {
      leasedObserved = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  await runtime.stop();
  const afterStop = await runtime.getRun(runId);

  assert.equal(leasedObserved, true);
  assert.ok(afterStop);
  assert.equal(afterStop?.status, "leased");
});
