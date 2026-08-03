import test from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createDurableStack } from "../src/runtime.js";

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
