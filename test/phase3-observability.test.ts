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

  await sink.publish({
    eventId: "evt-hb-1",
    eventType: EVENT_TYPES.WORKER_HEARTBEAT,
    eventVersion: 2,
    occurredAtUtc: new Date().toISOString(),
    workerName: "worker-a"
  });

  await sink.publish({
    eventId: "evt-hb-2",
    eventType: EVENT_TYPES.WORKER_HEARTBEAT,
    eventVersion: 2,
    occurredAtUtc: new Date().toISOString(),
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
  assert.equal(body.events.length, 2);
  const nonHeartbeat = body.events.find((x: { eventType: string }) => x.eventType === EVENT_TYPES.JOB_STARTED);
  const heartbeatBatch = body.events.find((x: { eventType: string }) => x.eventType === "worker_heartbeat_batch");
  assert.ok(nonHeartbeat);
  assert.ok(heartbeatBatch);
  assert.equal(nonHeartbeat.runtime, "Node.js");
  assert.equal(heartbeatBatch.runtime, "Node.js");

  const payload = JSON.parse(nonHeartbeat.payloadJson);
  assert.equal(payload.errorDetail, undefined);
  assert.equal(payload.message, undefined);

  const heartbeatPayload = JSON.parse(heartbeatBatch.payloadJson);
  assert.equal(heartbeatPayload.heartbeatCount, 2);
});

test("ingestion sync retries transient 429 responses and succeeds", async () => {
  const attempts: Array<{ url: string; body: string }> = [];

  const options = normalizeOptions({
    workerName: "worker-a",
    eventing: {
      tenantId: "tenant-1",
      clientSecret: "secret-1",
      ingestionApiBaseUrl: "https://api.example.com",
      ingestionPath: "/v1/events/batch",
      ingestionMaxBatchSize: 100,
      ingestionMaxRequestBodyBytes: 1_000_000,
      ingestionMaxRetryAttempts: 3,
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

  let call = 0;
  const service = new IngestionEventSyncService(
    sink,
    options,
    async (request) => {
      attempts.push({ url: request.url, body: request.body });
      call += 1;
      if (call < 3) {
        return { status: 429, bodyText: "too many requests" };
      }
      return {
        status: 200,
        bodyText: JSON.stringify({ acceptedCount: 1, rejectedCount: 0, serverTimeUtc: new Date().toISOString(), isDuplicate: false })
      };
    },
    "Node.js",
    "20.0.0"
  );

  await service.flushOnce(new AbortController().signal);

  assert.equal(attempts.length, 3);
  assert.ok(attempts.every((x) => x.url === "https://api.example.com/v1/events/batch"));
  assert.equal(new Set(attempts.map((x) => x.body)).size, 1);
});

test("ingestion sync does not retry unauthorized responses", async () => {
  let attempts = 0;

  const options = normalizeOptions({
    workerName: "worker-a",
    eventing: {
      tenantId: "tenant-1",
      clientSecret: "secret-1",
      ingestionApiBaseUrl: "https://api.example.com",
      ingestionPath: "/v1/events/batch",
      ingestionMaxBatchSize: 100,
      ingestionMaxRequestBodyBytes: 1_000_000,
      ingestionMaxRetryAttempts: 4,
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

  const service = new IngestionEventSyncService(sink, options, async () => {
    attempts += 1;
    return { status: 401, bodyText: "unauthorized" };
  });

  await service.flushOnce(new AbortController().signal);

  assert.equal(attempts, 1);
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

test("runtime control sync retries transient 5xx response then processes commands", async () => {
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
      runtimeControlMaxReceiptUpload: 200,
      ingestionMaxRetryAttempts: 3
    }
  });

  let attempts = 0;
  const service = new RuntimeControlSyncService(
    store,
    admin,
    options,
    async () => {
      attempts += 1;
      if (attempts === 1) {
        return { status: 500, bodyText: "temporary server error" };
      }
      return {
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
      };
    }
  );

  await service.syncOnce(new AbortController().signal);

  assert.equal(attempts, 2);
  const jobs = await store.getRecurringJobs(true);
  assert.equal(jobs[0]?.enabled, false);

  const receipts = await store.getRuntimeCommandReceipts(10);
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0]?.status, "succeeded");
});

test("runtime control sync does not retry unauthorized responses", async () => {
  const store = new InMemoryDurableJobStore();

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => false,
    updateScheduledJobCron: async () => false,
    runScheduledJobNow: async () => undefined
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
      runtimeControlMaxReceiptUpload: 200,
      ingestionMaxRetryAttempts: 4
    }
  });

  let attempts = 0;
  const service = new RuntimeControlSyncService(
    store,
    admin,
    options,
    async () => {
      attempts += 1;
      return { status: 403, bodyText: "forbidden" };
    }
  );

  await service.syncOnce(new AbortController().signal);

  assert.equal(attempts, 1);
});

test("runtime control sync skips expired commands", async () => {
  const store = new InMemoryDurableJobStore();

  const adminCalls = {
    setEnabled: 0,
    updateCron: 0,
    runNow: 0
  };

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => {
      adminCalls.setEnabled += 1;
      return true;
    },
    updateScheduledJobCron: async () => {
      adminCalls.updateCron += 1;
      return true;
    },
    runScheduledJobNow: async () => {
      adminCalls.runNow += 1;
      return "run-1";
    }
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
            commandId: "cmd-expired",
            commandType: "set_schedule_enabled",
            payloadJson: JSON.stringify({ jobName: "recurring-a", enabled: false }),
            issuedAtUtc: new Date(Date.now() - 30_000).toISOString(),
            expiresAtUtc: new Date(Date.now() - 5_000).toISOString()
          }
        ]
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  assert.equal(adminCalls.setEnabled, 0);
  assert.equal(adminCalls.updateCron, 0);
  assert.equal(adminCalls.runNow, 0);

  const receipts = await store.getRuntimeCommandReceipts(10);
  assert.equal(receipts.length, 0);
});

test("runtime control sync records failed receipt for blank command type", async () => {
  const store = new InMemoryDurableJobStore();

  const adminCalls = {
    setEnabled: 0,
    updateCron: 0,
    runNow: 0
  };

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => {
      adminCalls.setEnabled += 1;
      return true;
    },
    updateScheduledJobCron: async () => {
      adminCalls.updateCron += 1;
      return true;
    },
    runScheduledJobNow: async () => {
      adminCalls.runNow += 1;
      return "run-1";
    }
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
            commandId: "cmd-blank-type",
            commandType: "   ",
            payloadJson: "{}",
            issuedAtUtc: new Date().toISOString()
          }
        ]
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  assert.equal(adminCalls.setEnabled, 0);
  assert.equal(adminCalls.updateCron, 0);
  assert.equal(adminCalls.runNow, 0);

  const receipts = await store.getRuntimeCommandReceipts(10);
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0]?.commandId, "cmd-blank-type");
  assert.equal(receipts[0]?.status, "failed");
  assert.equal(receipts[0]?.errorCode, "invalid_command_type");
});

test("runtime control sync records failed receipt for malformed payload json", async () => {
  const store = new InMemoryDurableJobStore();

  const adminCalls = {
    setEnabled: 0,
    updateCron: 0,
    runNow: 0
  };

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => {
      adminCalls.setEnabled += 1;
      return true;
    },
    updateScheduledJobCron: async () => {
      adminCalls.updateCron += 1;
      return true;
    },
    runScheduledJobNow: async () => {
      adminCalls.runNow += 1;
      return "run-1";
    }
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
            commandId: "cmd-bad-payload",
            commandType: "set_schedule_enabled",
            payloadJson: "{",
            issuedAtUtc: new Date().toISOString()
          }
        ]
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  assert.equal(adminCalls.setEnabled, 0);
  assert.equal(adminCalls.updateCron, 0);
  assert.equal(adminCalls.runNow, 0);

  const receipts = await store.getRuntimeCommandReceipts(10);
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0]?.commandId, "cmd-bad-payload");
  assert.equal(receipts[0]?.status, "failed");
  assert.equal(receipts[0]?.errorCode, "invalid_payload");
});

test("runtime control sync uploads existing receipts before processing new commands", async () => {
  const store = new InMemoryDurableJobStore();

  const leased = await store.tryLeaseRuntimeCommandReceipt("cmd-prev", "worker-a", 30);
  assert.equal(leased, true);
  const acked = await store.markRuntimeCommandAcknowledged("cmd-prev", "worker-a", new Date().toISOString());
  assert.equal(acked, true);
  const succeeded = await store.markRuntimeCommandSucceeded(
    "cmd-prev",
    "worker-a",
    new Date().toISOString(),
    new Date().toISOString(),
    undefined
  );
  assert.equal(succeeded, true);

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => true,
    updateScheduledJobCron: async () => true,
    runScheduledJobNow: async () => "run-1"
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
            commandId: "cmd-new",
            commandType: "run_schedule_now",
            payloadJson: JSON.stringify({ jobName: "recurring-a" }),
            issuedAtUtc: new Date().toISOString()
          }
        ]
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  const pendingUploadAfterSync = await store.getRuntimeCommandReceipts(10);
  assert.equal(pendingUploadAfterSync.length, 1);
  assert.equal(pendingUploadAfterSync[0]?.commandId, "cmd-new");
  assert.equal(pendingUploadAfterSync[0]?.status, "succeeded");
});

test("runtime control sync does not upload leased-only receipts", async () => {
  const store = new InMemoryDurableJobStore();

  const leased = await store.tryLeaseRuntimeCommandReceipt("cmd-leased", "worker-a", 30);
  assert.equal(leased, true);

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => true,
    updateScheduledJobCron: async () => true,
    runScheduledJobNow: async () => "run-1"
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
        commands: []
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  const uploadedVisible = await store.getRuntimeCommandReceipts(10);
  assert.equal(uploadedVisible.length, 0);

  const leaseCanBeRetaken = await store.tryLeaseRuntimeCommandReceipt("cmd-leased", "worker-b", 30);
  assert.equal(leaseCanBeRetaken, false);
});

test("runtime control sync sends normalized runtime version without leading v", async () => {
  const store = new InMemoryDurableJobStore();

  const admin: RuntimeControlAdmin = {
    listScheduledJobs: async () => [],
    setScheduledJobEnabled: async () => true,
    updateScheduledJobCron: async () => true,
    runScheduledJobNow: async () => "run-1"
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

  let capturedBody = "";
  const service = new RuntimeControlSyncService(
    store,
    admin,
    options,
    async (request) => {
      capturedBody = request.body;
      return {
        status: 200,
        bodyText: JSON.stringify({
          serverTimeUtc: new Date().toISOString(),
          commands: []
        })
      };
    },
    "Node.js",
    "v20.18.0"
  );

  await service.syncOnce(new AbortController().signal);

  const payload = JSON.parse(capturedBody);
  assert.equal(payload.runtime, "Node.js");
  assert.equal(payload.runtimeVersion, "20.18.0");
});

test("runtime control sync processes PascalCase command payload fields", async () => {
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
        ServerTimeUtc: new Date().toISOString(),
        Commands: [
          {
            CommandId: "cmd-pascal-1",
            CommandType: "update_schedule_cron",
            PayloadJson: JSON.stringify({
              JobName: "recurring-a",
              CronExpression: "*/2 * * * *",
              TimeZone: "UTC"
            }),
            IssuedAtUtc: new Date().toISOString()
          }
        ]
      })
    })
  );

  await service.syncOnce(new AbortController().signal);

  const jobs = await store.getRecurringJobs(true);
  assert.equal(jobs[0]?.cronExpression, "*/2 * * * *");
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
