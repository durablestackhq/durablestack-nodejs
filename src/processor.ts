import { EVENT_TYPES, RUN_STATUS } from "./constants.js";
import { getNextOccurrenceUtc, validateIanaTimeZone } from "./cron.js";
import { getEffectiveRunRetentionSeconds } from "./options.js";
import { DurableJobRegistry } from "./registry.js";
import type { DurableJobStore, DurableStackEvent, DurableStackEventSink, JobRunRecord, NormalizedDurableStackOptions } from "./types.js";
import { addSeconds, generateId, nowIso, randomJittered, safeJsonParse, sleep } from "./utils.js";

function truncateText(value: string, maxLength: number): string {
  if (maxLength <= 0) {
    return "";
  }
  return value.length <= maxLength ? value : value.slice(0, maxLength);
}

/**
 * Exception messages and stack traces routinely contain sensitive values
 * (connection details, file paths, SQL fragments, business data). Unless the
 * deployment opts in via `includeErrorDetail`, this text must never leave the
 * process in a published event — only the exception type name does.
 */
function sanitizeErrorTextForPublish(
  text: string | undefined,
  options: NormalizedDurableStackOptions
): string | undefined {
  if (!text) {
    return text;
  }
  if (!options.eventing.includeErrorDetail) {
    return undefined;
  }
  return truncateText(text, Math.max(1, options.eventing.maxErrorDetailLength));
}

function getErrorDetail(error: unknown): string | undefined {
  if (error instanceof Error && typeof error.stack === "string" && error.stack.trim().length > 0) {
    return error.stack;
  }
  if (typeof error === "string" && error.trim().length > 0) {
    return error;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return undefined;
  }
}

export class DurableStackProcessor {
  private readonly inFlight = new Set<Promise<void>>();
  private nextRetentionSweepAtUtc = "1970-01-01T00:00:00.000Z";

  public constructor(
    private readonly store: DurableJobStore,
    private readonly registry: DurableJobRegistry,
    private readonly options: NormalizedDurableStackOptions,
    private readonly sinks: DurableStackEventSink[]
  ) {}

  public async initializeRecurringJobs(): Promise<void> {
    const now = nowIso();
    const registeredRecurring = this.registry.getAll().filter((x) => Boolean(x.recurring));
    const existing = await this.store.getRecurringJobs(true);
    const existingByName = new Map(existing.map((x) => [x.jobName, x]));

    for (const registration of registeredRecurring) {
      if (!registration.recurring) {
        continue;
      }

      validateIanaTimeZone(registration.recurring.timeZone);
      const current = existingByName.get(registration.jobName);
      if (current && this.options.recurring.registrationSync.existingJobBehavior === "KeepDatabase") {
        continue;
      }

      const next = getNextOccurrenceUtc(registration.recurring.cronExpression, registration.recurring.timeZone, now);
      await this.store.upsertRecurringJob(registration, next);
    }

    if (this.options.recurring.registrationSync.orphanedJobBehavior === "Disable") {
      const registeredNames = new Set(registeredRecurring.map((x) => x.jobName));
      for (const row of existing) {
        if (!registeredNames.has(row.jobName) && row.enabled) {
          await this.store.setRecurringJobEnabled(row.jobName, false, undefined);
        }
      }
    }
  }

  public async processOnce(signal: AbortSignal): Promise<number> {
    if (signal.aborted) {
      return 0;
    }

    await this.pruneHistoricalRunsIfDue();
    await this.materializeDueRecurringRuns();

    const available = Math.max(0, this.options.maxConcurrentRuns - this.inFlight.size);
    if (available <= 0) {
      return 0;
    }

    const claimCount = Math.max(1, Math.min(available, this.options.claimBatchSize));
    const claimed = await this.store.claimDueRuns(this.options.workerName, claimCount, this.options.leaseDurationSeconds);

    for (const run of claimed) {
      await this.publish({
        eventId: generateId(),
        eventType: EVENT_TYPES.JOB_CLAIMED,
        eventVersion: 2,
        occurredAtUtc: nowIso(),
        runId: run.id,
        jobName: run.jobName,
        attempt: run.attempt,
        maxAttempts: run.maxAttempts,
        workerName: this.options.workerName
      });

      const task = this.executeRunSafely(run, signal);
      this.inFlight.add(task);
      task.finally(() => this.inFlight.delete(task));
    }

    return claimed.length;
  }

  public async drainInFlightRuns(timeoutSeconds: number): Promise<void> {
    const deadline = Date.now() + Math.max(0, timeoutSeconds) * 1000;
    while (this.inFlight.size > 0) {
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        return;
      }
      await Promise.race([...this.inFlight, sleep(remainingMs)]);
    }
  }

  public get inFlightCount(): number {
    return this.inFlight.size;
  }

  private async materializeDueRecurringRuns(): Promise<void> {
    const now = nowIso();
    const due = await this.store.getDueRecurringJobs(now, 100);
    for (const recurring of due) {
      const registration = this.registry.get(recurring.jobName);
      if (!registration || !registration.recurring) {
        continue;
      }

      let next = getNextOccurrenceUtc(recurring.cronExpression, recurring.timeZone, recurring.nextRunAtUtc);
      if (this.options.recurring.catchUpPolicy === "SkipMissed") {
        while (Date.parse(next) <= Date.parse(now)) {
          next = getNextOccurrenceUtc(recurring.cronExpression, recurring.timeZone, next);
        }
      }

      await this.store.tryMaterializeRecurringRun(recurring, registration, next);
    }
  }

  private async pruneHistoricalRunsIfDue(): Promise<void> {
    if (!this.options.retention.enabled) {
      return;
    }

    const now = nowIso();
    if (Date.parse(now) < Date.parse(this.nextRetentionSweepAtUtc)) {
      return;
    }

    this.nextRetentionSweepAtUtc = addSeconds(now, this.options.retention.sweepIntervalSeconds);
    const retentionSeconds = getEffectiveRunRetentionSeconds(this.options);
    const completedBeforeUtc = new Date(Date.now() - (retentionSeconds * 1000)).toISOString();
    await this.store.pruneHistoricalRuns(completedBeforeUtc, this.options.retention.deleteBatchSize);
  }

  private async executeRunSafely(run: JobRunRecord, signal: AbortSignal): Promise<void> {
    try {
      await this.executeRun(run, signal);
    } catch {
      // swallowed intentionally for worker loop isolation
    }
  }

  private async executeRun(run: JobRunRecord, signal: AbortSignal): Promise<void> {
    // A missing registration is handled inside the try/catch below like any other job
    // failure — retried, and eventful — rather than failed immediately. In a rolling
    // deployment, a worker that hasn't yet picked up a new job definition can claim a run
    // for it; failing non-retryable here would permanently kill that run instead of letting
    // a worker that does have the registration pick it up on a later attempt.
    const registration = this.registry.get(run.jobName);

    const started = Date.now();
    await this.publish({
      eventId: generateId(),
      eventType: EVENT_TYPES.JOB_STARTED,
      eventVersion: 2,
      occurredAtUtc: nowIso(),
      runId: run.id,
      jobName: run.jobName,
      attempt: run.attempt,
      maxAttempts: run.maxAttempts,
      workerName: this.options.workerName
    });

    const localAbort = new AbortController();
    const combinedAbort = new AbortController();

    const propagateAbort = () => {
      if (!combinedAbort.signal.aborted) {
        combinedAbort.abort();
      }
    };

    const signalAbortListener = () => propagateAbort();
    signal.addEventListener("abort", signalAbortListener, { once: true });
    localAbort.signal.addEventListener("abort", propagateAbort, { once: true });
    if (signal.aborted) {
      propagateAbort();
    }

    let leaseLost = false;
    const heartbeatHandle = setInterval(() => {
      void (async () => {
        try {
          const extended = await this.store.extendLease(run.id, this.options.workerName, this.options.leaseDurationSeconds);
          if (!extended) {
            // The store explicitly refused the extension: another worker holds the
            // lease, or the run was cancelled. That is unambiguous lease loss.
            leaseLost = true;
            localAbort.abort();
          }
        } catch (error) {
          // A thrown error only means the extension attempt failed transiently (a
          // dropped connection, a timeout) — it says nothing about who currently
          // owns the lease. Treating it as loss would cancel healthy long-running
          // jobs on a momentary blip; log and retry at the next heartbeat instead,
          // matching the .NET runtime's LeaseHeartbeatJobRunner.
          const message = error instanceof Error ? error.message : String(error);
          console.warn(
            `DurableStack lease heartbeat failed; retrying at next heartbeat interval. RunId=${run.id} JobName=${run.jobName}. ${message}`
          );
        }
      })();
    }, Math.max(250, Math.floor(this.options.leaseDurationSeconds * 500)));

    try {
      if (!registration) {
        throw new Error(`No registered job named '${run.jobName}'.`);
      }

      const payload = safeJsonParse<unknown>(run.payloadJson);
      await registration.handler(payload, {
        runId: run.id,
        jobName: run.jobName,
        attempt: run.attempt,
        workerName: this.options.workerName
      }, combinedAbort.signal);

      // A handler that returns normally has completed its work, even if an
      // abort was signaled while it ran; only a lost lease forfeits the write.
      // Handlers that bail out on abort are expected to throw.
      if (leaseLost) {
        return;
      }

      const recorded = await this.store.markSucceeded(run.id, this.options.workerName);
      if (recorded) {
        await this.publish({
          eventId: generateId(),
          eventType: EVENT_TYPES.JOB_SUCCEEDED,
          eventVersion: 2,
          occurredAtUtc: nowIso(),
          runId: run.id,
          jobName: run.jobName,
          attempt: run.attempt,
          maxAttempts: run.maxAttempts,
          workerName: this.options.workerName,
          durationMs: Date.now() - started
        });
      }
    } catch (error) {
      if (signal.aborted && combinedAbort.signal.aborted && !leaseLost) {
        return;
      }

      // `message` is stored locally via markFailed (run history within the user's own
      // database) and is never redacted. Only the copies placed on the published event
      // — which can leave the process via a custom sink or the hosted ingestion
      // client — are sanitized, matching the .NET runtime's default-redacted behavior.
      const message = error instanceof Error ? error.message : "Unknown job failure";
      const publishedErrorMessage = sanitizeErrorTextForPublish(message, this.options);
      const publishedErrorDetail = sanitizeErrorTextForPublish(getErrorDetail(error) ?? message, this.options);
      const shouldRetry = run.attempt < run.maxAttempts;
      // Matches the .NET runtime's default (RetryBehavior.FixedDelay) when a job doesn't
      // specify a retry behavior explicitly. `registration` may be undefined here (the
      // "no registration" failure above), in which case the global defaults apply.
      const retryBehavior = registration?.retryBehavior ?? registration?.recurring?.retryBehavior ?? "fixed";
      const initialDelay = registration?.retryInitialDelaySeconds
        ?? registration?.recurring?.retryInitialDelaySeconds
        ?? this.options.retryDelaySeconds;
      const delayBase = Math.max(1, initialDelay);
      const retrySeconds = retryBehavior === "fixed"
        ? delayBase
        : Math.min(this.options.retryMaxDelaySeconds, delayBase * Math.pow(2, Math.max(0, run.attempt - 1)));
      const retryDelay = shouldRetry
        ? randomJittered(
            retrySeconds,
            this.options.retryJitterEnabled,
            this.options.retryJitterRatio)
        : 0;

      const retryAtUtc = shouldRetry ? addSeconds(nowIso(), retryDelay) : undefined;
      const recorded = await this.store.markFailed(run.id, this.options.workerName, message, shouldRetry, retryAtUtc);
      if (recorded) {
        await this.publish({
          eventId: generateId(),
          eventType: EVENT_TYPES.JOB_FAILED,
          eventVersion: 2,
          occurredAtUtc: nowIso(),
          runId: run.id,
          jobName: run.jobName,
          attempt: run.attempt,
          maxAttempts: run.maxAttempts,
          workerName: this.options.workerName,
          errorType: error instanceof Error ? error.name : "Error",
          errorMessage: publishedErrorMessage,
          errorDetail: publishedErrorDetail,
          durationMs: Date.now() - started,
          retryAtUtc
        });

        if (shouldRetry && retryAtUtc) {
          await this.publish({
            eventId: generateId(),
            eventType: EVENT_TYPES.JOB_RETRIED,
            eventVersion: 2,
            occurredAtUtc: nowIso(),
            runId: run.id,
            jobName: run.jobName,
            attempt: run.attempt,
            maxAttempts: run.maxAttempts,
            workerName: this.options.workerName,
            retryAtUtc
          });
          await this.publish({
            eventId: generateId(),
            eventType: EVENT_TYPES.RETRY_SCHEDULED,
            eventVersion: 2,
            occurredAtUtc: nowIso(),
            runId: run.id,
            jobName: run.jobName,
            attempt: run.attempt,
            maxAttempts: run.maxAttempts,
            workerName: this.options.workerName,
            retryAtUtc,
            message: retryAtUtc
          });
        }
      }
    } finally {
      clearInterval(heartbeatHandle);
      signal.removeEventListener("abort", signalAbortListener);
    }
  }

  private async publish(event: DurableStackEvent): Promise<void> {
    for (const sink of this.sinks) {
      try {
        await sink.publish(event);
      } catch {
        // isolate sink failures
      }
    }
  }
}
