import { EVENT_TYPES, RUN_STATUS } from "./constants.js";
import { getNextOccurrenceUtc, validateIanaTimeZone } from "./cron.js";
import { NoOpDurableStackEventSink } from "./event-sink.js";
import { InMemoryDurableJobStore } from "./in-memory-store.js";
import { normalizeOptions } from "./options.js";
import { loadAutodiscoveredJobs } from "./autodiscovery.js";
import { createIngestionEventing, type IngestionEventSyncService } from "./observability/ingestion.js";
import { RuntimeControlSyncService, type RuntimeControlAdmin } from "./observability/runtime-control.js";
import { DurableStackProcessor } from "./processor.js";
import { DurableJobRegistry } from "./registry.js";
import type {
  DurableJobHandler,
  DurableJobStore,
  DurableStackEventSink,
  DurableStackOptions,
  JobRunRecord,
  RecurringJobState,
  RetryBehavior
} from "./types.js";
import { nowIso, randomJittered, safeJsonStringify, sleep } from "./utils.js";

export interface RegisterJobOptions {
  maxAttempts?: number;
  retryBehavior?: RetryBehavior;
  retryInitialDelaySeconds?: number;
}

export interface RegisterRecurringOptions {
  maxAttempts?: number;
  enabled?: boolean;
  allowConcurrentRuns?: boolean;
  retryBehavior?: RetryBehavior;
  retryInitialDelaySeconds?: number;
}

export interface DurableStackRuntime {
  registerJob(jobName: string, handler: DurableJobHandler, options?: RegisterJobOptions): DurableStackRuntime;
  registerRecurring(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    handler: DurableJobHandler,
    options?: RegisterRecurringOptions
  ): DurableStackRuntime;

  enqueue(jobName: string, payload?: unknown): Promise<string>;
  schedule(jobName: string, payload: unknown, runAtUtc: string): Promise<string>;
  getRun(runId: string): Promise<JobRunRecord | undefined>;
  getRecentRuns(take?: number): Promise<JobRunRecord[]>;
  getRunsByStatus(status: keyof typeof RUN_STATUS | RunStatusString, take?: number): Promise<JobRunRecord[]>;
  getRunsByJobName(jobName: string, take?: number): Promise<JobRunRecord[]>;
  getEnqueuedRuns(take?: number): Promise<JobRunRecord[]>;

  listScheduledJobs(includeDisabled?: boolean): Promise<RecurringJobState[]>;
  setScheduledJobEnabled(jobName: string, enabled: boolean): Promise<boolean>;
  updateScheduledJobCron(jobName: string, cronExpression: string, timeZone: string): Promise<boolean>;
  runScheduledJobNow(jobName: string): Promise<string | undefined>;

  start(): Promise<void>;
  stop(): Promise<void>;
}

interface ManagedBackgroundService {
  start(): void;
  stop(): Promise<void>;
}

type RunStatusString = "pending" | "leased" | "succeeded" | "failed";

class DurableStackRuntimeImpl implements DurableStackRuntime {
  private readonly registry = new DurableJobRegistry();
  private readonly processor: DurableStackProcessor;
  private readonly sinks: DurableStackEventSink[];
  private readonly ingestionSyncService: IngestionEventSyncService | undefined;
  private readonly runtimeControlSyncService: RuntimeControlSyncService | undefined;
  private readonly managedServices: ManagedBackgroundService[];
  private running = false;
  private autodiscoveryLoaded = false;
  private loopPromise: Promise<void> | undefined;
  private abortController: AbortController | undefined;

  public constructor(
    private readonly store: DurableJobStore,
    private readonly options: ReturnType<typeof normalizeOptions>,
    sinks: DurableStackEventSink[]
  ) {
    const configuredSinks = sinks.length > 0 ? [...sinks] : [];

    const hasHostedCredentials =
      Boolean(this.options.eventing.tenantId && this.options.eventing.tenantId.trim())
      && Boolean(this.options.eventing.clientSecret && this.options.eventing.clientSecret.trim());

    if (hasHostedCredentials) {
      const ingestion = createIngestionEventing(this.options);
      configuredSinks.push(ingestion.sink);
      this.ingestionSyncService = ingestion.service;

      const admin: RuntimeControlAdmin = {
        listScheduledJobs: (includeDisabled) => this.listScheduledJobs(includeDisabled),
        setScheduledJobEnabled: (jobName, enabled) => this.setScheduledJobEnabled(jobName, enabled),
        updateScheduledJobCron: (jobName, cronExpression, timeZone) => this.updateScheduledJobCron(jobName, cronExpression, timeZone),
        runScheduledJobNow: (jobName) => this.runScheduledJobNow(jobName)
      };

      this.runtimeControlSyncService = new RuntimeControlSyncService(
        this.store,
        admin,
        this.options
      );
    }

    this.managedServices = [];
    if (this.runtimeControlSyncService) {
      this.managedServices.push(this.runtimeControlSyncService);
    }
    if (this.ingestionSyncService) {
      this.managedServices.push(this.ingestionSyncService);
    }

    this.sinks = configuredSinks.length > 0 ? configuredSinks : [new NoOpDurableStackEventSink()];
    this.processor = new DurableStackProcessor(store, this.registry, options, this.sinks);
  }

  public registerJob(jobName: string, handler: DurableJobHandler, options?: RegisterJobOptions): DurableStackRuntime {
    this.registry.register({
      jobName,
      jobType: jobName,
      maxAttempts: Math.max(1, Math.floor(options?.maxAttempts ?? 3)),
      retryBehavior: options?.retryBehavior,
      retryInitialDelaySeconds: options?.retryInitialDelaySeconds,
      handler
    });
    return this;
  }

  public registerRecurring(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    handler: DurableJobHandler,
    options?: RegisterRecurringOptions
  ): DurableStackRuntime {
    validateIanaTimeZone(timeZone);
    this.registry.register({
      jobName,
      jobType: jobName,
      maxAttempts: Math.max(1, Math.floor(options?.maxAttempts ?? 3)),
      handler,
      recurring: {
        cronExpression,
        timeZone,
        enabled: options?.enabled ?? true,
        allowConcurrentRuns: options?.allowConcurrentRuns ?? false,
        retryBehavior: options?.retryBehavior,
        retryInitialDelaySeconds: options?.retryInitialDelaySeconds
      }
    });
    return this;
  }

  public async enqueue(jobName: string, payload?: unknown): Promise<string> {
    const registration = this.registry.get(jobName);
    if (!registration) {
      throw new Error(`No registered job named '${jobName}'.`);
    }

    return this.store.enqueue(jobName, registration.jobType, safeJsonStringify(payload), nowIso(), registration.maxAttempts);
  }

  public async schedule(jobName: string, payload: unknown, runAtUtc: string): Promise<string> {
    const registration = this.registry.get(jobName);
    if (!registration) {
      throw new Error(`No registered job named '${jobName}'.`);
    }

    return this.store.enqueue(jobName, registration.jobType, safeJsonStringify(payload), runAtUtc, registration.maxAttempts);
  }

  public async getRun(runId: string): Promise<JobRunRecord | undefined> {
    return this.store.getRun(runId);
  }

  public async getRecentRuns(take = 100): Promise<JobRunRecord[]> {
    return this.store.getRecentRuns(Math.max(1, take));
  }

  public async getRunsByStatus(status: keyof typeof RUN_STATUS | RunStatusString, take = 100): Promise<JobRunRecord[]> {
    const normalized = String(status).toLowerCase() as RunStatusString;
    if (!Object.values(RUN_STATUS).includes(normalized)) {
      throw new Error(`Invalid status '${status}'.`);
    }
    return this.store.getRunsByStatus(normalized, Math.max(1, take));
  }

  public async getRunsByJobName(jobName: string, take = 100): Promise<JobRunRecord[]> {
    return this.store.getRunsByJobName(jobName, Math.max(1, take));
  }

  public async getEnqueuedRuns(take = 100): Promise<JobRunRecord[]> {
    return this.store.getEnqueuedRuns(Math.max(1, take));
  }

  public async listScheduledJobs(includeDisabled = true): Promise<RecurringJobState[]> {
    return this.store.getRecurringJobs(includeDisabled);
  }

  public async setScheduledJobEnabled(jobName: string, enabled: boolean): Promise<boolean> {
    let nextRun: string | undefined;
    if (enabled) {
      const schedules = await this.store.getRecurringJobs(true);
      const current = schedules.find((x) => x.jobName === jobName);
      if (!current) {
        return false;
      }
      nextRun = getNextOccurrenceUtc(current.cronExpression, current.timeZone, nowIso());
    }

    return this.store.setRecurringJobEnabled(jobName, enabled, nextRun);
  }

  public async updateScheduledJobCron(jobName: string, cronExpression: string, timeZone: string): Promise<boolean> {
    validateIanaTimeZone(timeZone);
    const next = getNextOccurrenceUtc(cronExpression, timeZone, nowIso());
    return this.store.updateRecurringJobSchedule(jobName, cronExpression, timeZone, next);
  }

  public async runScheduledJobNow(jobName: string): Promise<string | undefined> {
    const schedules = await this.store.getRecurringJobs(true);
    const schedule = schedules.find((x) => x.jobName === jobName);
    if (!schedule) {
      return undefined;
    }

    return this.store.tryEnqueueIfNoActiveRun(jobName, schedule.jobType, undefined, nowIso(), schedule.maxAttempts);
  }

  public async start(): Promise<void> {
    if (this.running) {
      return;
    }

    await this.loadAutodiscoveredJobsIfEnabled();

    this.running = true;
    this.abortController = new AbortController();
    await this.processor.initializeRecurringJobs();

    for (const service of this.managedServices) {
      service.start();
    }

    this.loopPromise = (async () => {
      while (this.running && this.abortController && !this.abortController.signal.aborted) {
        await this.processor.processOnce(this.abortController.signal);
        await this.emitHeartbeat();

        const delaySeconds = randomJittered(
          this.options.pollIntervalSeconds,
          this.options.pollJitterEnabled,
          this.options.pollJitterRatio
        );

        await sleep(delaySeconds * 1000);
      }
    })();
  }

  private async loadAutodiscoveredJobsIfEnabled(): Promise<void> {
    if (this.autodiscoveryLoaded || !this.options.autodiscovery.enabled) {
      return;
    }

    this.autodiscoveryLoaded = true;
    if (this.options.autodiscovery.includeGlobs.length === 0) {
      return;
    }

    try {
      const discovered = await loadAutodiscoveredJobs({
        baseDir: this.options.autodiscovery.baseDir,
        includeGlobs: this.options.autodiscovery.includeGlobs,
        excludeGlobs: this.options.autodiscovery.excludeGlobs,
        exportName: this.options.autodiscovery.exportName,
        maxModules: this.options.autodiscovery.maxModules,
        failOnError: this.options.autodiscovery.failOnError
      });

      for (const definition of discovered) {
        this.registry.register({
          jobName: definition.jobName,
          jobType: definition.jobType ?? definition.jobName,
          maxAttempts: Math.max(1, Math.floor(definition.maxAttempts ?? 3)),
          retryBehavior: definition.retryBehavior,
          retryInitialDelaySeconds: definition.retryInitialDelaySeconds,
          handler: definition.handler,
          recurring: definition.recurring
        });
      }
    } catch (error) {
      if (this.options.autodiscovery.failOnError) {
        const message = error instanceof Error ? error.message : "Autodiscovery failed.";
        throw new Error(`Autodiscovery failed: ${message}`);
      }
    }
  }

  public async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;
    if (this.abortController) {
      this.abortController.abort();
    }

    if (this.loopPromise) {
      await this.loopPromise;
    }

    for (const service of this.managedServices) {
      await service.stop();
    }

    await this.processor.drainInFlightRuns(this.options.shutdownDrainTimeoutSeconds);
  }

  private async emitHeartbeat(): Promise<void> {
    const event = {
      eventId: `hb-${Date.now()}-${Math.random().toString(16).slice(2)}`,
      eventType: EVENT_TYPES.WORKER_HEARTBEAT,
      eventVersion: 2,
      occurredAtUtc: nowIso(),
      workerName: this.options.workerName
    };

    for (const sink of this.sinks) {
      try {
        await sink.publish(event);
      } catch {
        // isolate sink errors
      }
    }
  }
}

export function createDurableStack(options?: DurableStackOptions): DurableStackRuntime {
  const normalized = normalizeOptions(options);
  const store = new InMemoryDurableJobStore();
  return new DurableStackRuntimeImpl(store, normalized, [new NoOpDurableStackEventSink()]);
}

export function createDurableStackWithStore(
  store: DurableJobStore,
  options?: DurableStackOptions,
  sinks?: DurableStackEventSink[]
): DurableStackRuntime {
  const normalized = normalizeOptions(options);
  return new DurableStackRuntimeImpl(store, normalized, sinks ?? [new NoOpDurableStackEventSink()]);
}
