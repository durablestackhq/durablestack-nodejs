import test from "node:test";
import assert from "node:assert/strict";
import { InMemoryDurableJobStore } from "../src/in-memory-store.js";
import { normalizeOptions } from "../src/options.js";
import { IngestionDurableStackEventSink, IngestionEventSyncService } from "../src/observability/ingestion.js";
import { RuntimeControlSyncService, type RuntimeControlAdmin } from "../src/observability/runtime-control.js";
import { EVENT_TYPES } from "../src/constants.js";
import { createDurableStackWithStore } from "../src/runtime.js";

test("ingestion sync posts batch with auth headers", async () => {
  const captured: Array<{ url: string; headers: Record<string, string>; body: string }> = [];

  const options = normalizeOptions({
    workerName: "worker-a",
    eventing: {
      tenantId: "tenant-1",
      clientSecret: "secret-1",
      serviceName: "svc",
      ingestionApiBaseUrl: "http://127.0.0.1:1",
      ingestionPath: "/v1/events/batch",
      ingestionMaxBatchSize: 100,
      ingestionMaxRequestBodyBytes: 1_000_000,
      ingestionMaxRetryAttempts: 2,
      ingestionFlushIntervalSeconds: 5
    }
  });

  const sink = new IngestionDurableStackEventSink();
  await sink.publish({
    eventId: "evt-1",
    eventType: EVENT_TYPES.JOB_STARTED,
    eventVersion: 2,
    occurredAtUtc: new Date().toISOString(),
    runId: "run-1",
    jobName: "job-a",
    attempt: 1,
    maxAttempts: 3,
    workerName: "worker-a"
  });

  const service = new IngestionEventSyncService(
    sink,
    options,
    async (request) => {
      captured.push(request);
      return {
        status: 200,
        bodyText: JSON.stringify({ acceptedCount: 1, rejectedCount: 0, serverTimeUtc: new Date().toISOString(), isDuplicate: false })
      };
    },
    "Node.js",
    "20.0.0"
  );

  await service.flushOnce(new AbortController().signal);

  assert.equal(captured.length, 1);
  const req = captured[0]!;
  assert.equal(req.url, "http://127.0.0.1:1/v1/events/batch");
  assert.equal(req.headers["X-DurableStack-TenantId"], "tenant-1");
  assert.equal(req.headers["X-DurableStack-ClientSecret"], "secret-1");

  const body = JSON.parse(req.body);
  assert.equal(body.tenantId, "tenant-1");
  assert.equal(body.events.length, 1);
  assert.equal(body.events[0].runtime, "Node.js");
});

test("runtime control sync processes set_schedule_enabled command and records success receipt", async () => {
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
      allowConcurrentRuns: false
    }
  };

  await store.upsertRecurringJob(registration, new Date(Date.now() + 60_000).toISOString());

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: (includeDisabled) => store.getRecurringJobs(includeDisabled),
    setScheduledJobEnabled: (jobName, enabled) => store.setRecurringJobEnabled(jobName, enabled, undefined),
    updateScheduledJobCron: (jobName, cronExpression, timeZone) =>
      store.updateRecurringJobSchedule(jobName, cronExpression, timeZone, new Date(Date.now() + 60_000).toISOString()),
    runScheduledJobNow: (jobName) => store.tryEnqueueIfNoActiveRun(jobName, jobName, undefined, new Date().toISOString(), 3)
  };

  const options = normalizeOptions({
    workerName: "worker-a",
    eventing: {
      tenantId: "tenant-1",
      clientSecret: "secret-1",
      ingestionApiBaseUrl: "https://api.example.com",
      runtimeControlSyncPath: "/v1/runtime/control/sync",
      runtimeControlSyncIntervalSeconds: 5,
      runtimeControlCommandLeaseDurationSeconds: 30,
      runtimeControlMaxReceiptUpload: 200
    }
  });

  const service = new RuntimeControlSyncService(
    store,
    admin,
    options,
    async () => ({
      status: 200,
      bodyText: JSON.stringify({
        serverTimeUtc: new Date().toISOString(),
        commands: [
          {
            commandId: "cmd-1",
            commandType: "set_schedule_enabled",
            payloadJson: JSON.stringify({ jobName: "recurring-a", enabled: false }),
            issuedAtUtc: new Date().toISOString()
          }
        ]
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  const jobs = await store.getRecurringJobs(true);
  assert.equal(jobs[0]?.enabled, false);

  const receipts = await store.getRuntimeCommandReceipts(10);
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0]?.status, "succeeded");
});

test("runtime start auto-wires hosted sinks/services when credentials are configured", async () => {
  const store = new InMemoryDurableJobStore();

  const runtime = createDurableStackWithStore(store, {
    workerName: "worker-hosted",
    pollIntervalSeconds: 0.1,
    eventing: {
      tenantId: "tenant-1",
      clientSecret: "secret-1",
      ingestionApiBaseUrl: "https://api.example.com",
      ingestionMaxRetryAttempts: 1,
      ingestionFlushIntervalSeconds: 5,
      runtimeControlEnabled: true,
      runtimeControlMaxReceiptUpload: 10,
      runtimeControlCommandLeaseDurationSeconds: 1,
      runtimeControlSyncPath: "/v1/runtime/control/sync",
      ingestionPath: "/v1/events/batch",
      runtimeControlSyncIntervalSeconds: 5
    }
  });

  runtime.registerJob("noop", async () => {});

  await runtime.start();
  await new Promise((resolve) => setTimeout(resolve, 50));
  await runtime.stop();

  assert.ok(true);
});
