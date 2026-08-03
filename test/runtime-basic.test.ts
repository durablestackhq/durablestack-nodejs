import test from "node:test";
import assert from "node:assert/strict";
import { createDurableStack } from "../src/runtime.js";

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
