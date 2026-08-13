import {
  RUN_STATUS,
  RUNTIME_COMMAND_RECEIPT_STATUS,
  type RunStatus
} from "./constants.js";
import type {
  DurableJobRegistration,
  DurableJobStore,
  JobRunRecord,
  RecurringJobState,
  RuntimeCommandReceiptRecord
} from "./types.js";
import { addSeconds, generateId, nowIso, toDate } from "./utils.js";

type InternalRun = JobRunRecord;

function cloneRun(run: InternalRun): JobRunRecord {
  return { ...run };
}

function compareIsoAsc(a: string, b: string): number {
  return toDate(a).getTime() - toDate(b).getTime();
}

function isActiveStatus(status: RunStatus): boolean {
  return status === RUN_STATUS.PENDING || status === RUN_STATUS.LEASED;
}

export class InMemoryDurableJobStore implements DurableJobStore {
  private readonly runs = new Map<string, InternalRun>();
  private readonly recurring = new Map<string, RecurringJobState>();
  private readonly runtimeReceipts = new Map<string, RuntimeCommandReceiptRecord>();

  async enqueue(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string> {
    const id = generateId();
    this.runs.set(id, {
      id,
      jobName,
      jobType,
      status: RUN_STATUS.PENDING,
      scheduledForUtc,
      attempt: 0,
      maxAttempts: Math.max(1, Math.floor(maxAttempts)),
      payloadJson
    });
    return id;
  }

  async claimDueRuns(workerName: string, batchSize: number, leaseDurationSeconds: number): Promise<JobRunRecord[]> {
    const now = nowIso();
    const due = Array.from(this.runs.values())
      .filter((run) => {
        if (run.status === RUN_STATUS.PENDING) {
          return compareIsoAsc(run.scheduledForUtc, now) <= 0;
        }

        if (run.status === RUN_STATUS.LEASED) {
          if (!run.leaseUntilUtc) {
            return true;
          }

          return compareIsoAsc(run.leaseUntilUtc, now) <= 0;
        }

        return false;
      })
      .sort((a, b) => compareIsoAsc(a.scheduledForUtc, b.scheduledForUtc));

    const claimed: JobRunRecord[] = [];
    for (const run of due) {
      if (claimed.length >= batchSize) {
        break;
      }

      if (run.attempt >= run.maxAttempts) {
        run.status = RUN_STATUS.FAILED;
        run.completedAtUtc = now;
        run.errorMessage = run.errorMessage ?? "Run exceeded max attempts before claim";
        run.leaseOwner = undefined;
        run.leaseUntilUtc = undefined;
        continue;
      }

      run.status = RUN_STATUS.LEASED;
      run.attempt += 1;
      run.startedAtUtc = run.startedAtUtc ?? now;
      run.leaseOwner = workerName;
      run.leaseUntilUtc = addSeconds(now, leaseDurationSeconds);
      claimed.push(cloneRun(run));
    }

    return claimed;
  }

  async markSucceeded(runId: string, workerName: string): Promise<boolean> {
    const run = this.runs.get(runId);
    if (!run) {
      return false;
    }

    if (run.status !== RUN_STATUS.LEASED || run.leaseOwner !== workerName) {
      return false;
    }

    run.status = RUN_STATUS.SUCCEEDED;
    run.completedAtUtc = nowIso();
    run.leaseOwner = undefined;
    run.leaseUntilUtc = undefined;
    run.errorMessage = undefined;
    return true;
  }

  async cancelRun(runId: string): Promise<boolean> {
    const run = this.runs.get(runId);
    if (!run) {
      return false;
    }

    if (run.status === RUN_STATUS.SUCCEEDED || run.status === RUN_STATUS.FAILED) {
      return false;
    }

    run.status = RUN_STATUS.FAILED;
    run.completedAtUtc = nowIso();
    run.errorMessage = "Run cancelled";
    run.leaseOwner = undefined;
    run.leaseUntilUtc = undefined;
    return true;
  }

  async markFailed(
    runId: string,
    workerName: string,
    errorMessage: string,
    retry: boolean,
    retryAtUtc: string | undefined
  ): Promise<boolean> {
    const run = this.runs.get(runId);
    if (!run) {
      return false;
    }

    if (run.status !== RUN_STATUS.LEASED || run.leaseOwner !== workerName) {
      return false;
    }

    run.errorMessage = errorMessage;
    run.leaseOwner = undefined;
    run.leaseUntilUtc = undefined;

    if (retry && retryAtUtc && run.attempt < run.maxAttempts) {
      run.status = RUN_STATUS.PENDING;
      run.scheduledForUtc = retryAtUtc;
      return true;
    }

    run.status = RUN_STATUS.FAILED;
    run.completedAtUtc = nowIso();
    return true;
  }

  async getRun(runId: string): Promise<JobRunRecord | undefined> {
    const run = this.runs.get(runId);
    return run ? cloneRun(run) : undefined;
  }

  async getRecentRuns(take: number): Promise<JobRunRecord[]> {
    return Array.from(this.runs.values())
      .sort((a, b) => compareIsoAsc(b.scheduledForUtc, a.scheduledForUtc))
      .slice(0, Math.max(1, take))
      .map(cloneRun);
  }

  async getRuns(): Promise<JobRunRecord[]> {
    return Array.from(this.runs.values()).map(cloneRun);
  }

  async getRunsByJobName(jobName: string, take: number): Promise<JobRunRecord[]> {
    return Array.from(this.runs.values())
      .filter((run) => run.jobName === jobName)
      .sort((a, b) => compareIsoAsc(b.scheduledForUtc, a.scheduledForUtc))
      .slice(0, Math.max(1, take))
      .map(cloneRun);
  }

  async getRunsByStatus(status: RunStatus, take: number): Promise<JobRunRecord[]> {
    return Array.from(this.runs.values())
      .filter((run) => run.status === status)
      .sort((a, b) => compareIsoAsc(b.scheduledForUtc, a.scheduledForUtc))
      .slice(0, Math.max(1, take))
      .map(cloneRun);
  }

  async getEnqueuedRuns(take: number): Promise<JobRunRecord[]> {
    return Array.from(this.runs.values())
      .filter((run) => run.status === RUN_STATUS.PENDING && !run.scheduleSlotUtc)
      .sort((a, b) => compareIsoAsc(b.scheduledForUtc, a.scheduledForUtc))
      .slice(0, Math.max(1, take))
      .map(cloneRun);
  }

  async tryEnqueueIfNoActiveRun(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string | undefined> {
    const hasActive = Array.from(this.runs.values()).some((run) => run.jobName === jobName && isActiveStatus(run.status));
    if (hasActive) {
      return undefined;
    }

    return this.enqueue(jobName, jobType, payloadJson, scheduledForUtc, maxAttempts);
  }

  async getRecurringJobs(includeDisabled: boolean): Promise<RecurringJobState[]> {
    return Array.from(this.recurring.values())
      .filter((job) => includeDisabled || job.enabled)
      .sort((a, b) => a.jobName.localeCompare(b.jobName))
      .map((job) => ({ ...job }));
  }

  async setRecurringJobEnabled(jobName: string, enabled: boolean, nextRunAtUtc: string | undefined): Promise<boolean> {
    const row = this.recurring.get(jobName);
    if (!row) {
      return false;
    }

    row.enabled = enabled;
    if (nextRunAtUtc) {
      row.nextRunAtUtc = nextRunAtUtc;
    }
    return true;
  }

  async updateRecurringJobSchedule(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const row = this.recurring.get(jobName);
    if (!row) {
      return false;
    }

    row.cronExpression = cronExpression;
    row.timeZone = timeZone;
    row.nextRunAtUtc = nextRunAtUtc;
    return true;
  }

  async pruneHistoricalRuns(completedBeforeUtc: string, batchSize: number): Promise<number> {
    const terminal = Array.from(this.runs.values())
      .filter((run) =>
        (run.status === RUN_STATUS.SUCCEEDED || run.status === RUN_STATUS.FAILED) &&
        run.completedAtUtc &&
        compareIsoAsc(run.completedAtUtc, completedBeforeUtc) < 0)
      .sort((a, b) => compareIsoAsc(a.completedAtUtc ?? a.scheduledForUtc, b.completedAtUtc ?? b.scheduledForUtc));

    let deleted = 0;
    for (const run of terminal) {
      if (deleted >= batchSize) {
        break;
      }
      this.runs.delete(run.id);
      deleted += 1;
    }
    return deleted;
  }

  async upsertRecurringJob(registration: DurableJobRegistration, nextRunAtUtc: string): Promise<void> {
    const recurring = registration.recurring;
    if (!recurring) {
      return;
    }

    const existing = this.recurring.get(registration.jobName);
    const next: RecurringJobState = {
      jobName: registration.jobName,
      jobType: registration.jobType,
      cronExpression: recurring.cronExpression,
      timeZone: recurring.timeZone,
      maxAttempts: registration.maxAttempts,
      enabled: recurring.enabled ?? true,
      allowConcurrentRuns: recurring.allowConcurrentRuns ?? false,
      retryBehavior: recurring.retryBehavior,
      retryInitialDelaySeconds: recurring.retryInitialDelaySeconds,
      nextRunAtUtc: existing?.nextRunAtUtc ?? nextRunAtUtc
    };

    this.recurring.set(registration.jobName, next);
  }

  async getDueRecurringJobs(nowUtc: string, batchSize: number): Promise<RecurringJobState[]> {
    return Array.from(this.recurring.values())
      .filter((job) => job.enabled && compareIsoAsc(job.nextRunAtUtc, nowUtc) <= 0)
      .sort((a, b) => compareIsoAsc(a.nextRunAtUtc, b.nextRunAtUtc))
      .slice(0, Math.max(1, batchSize))
      .map((job) => ({ ...job }));
  }

  async updateRecurringNextRun(jobName: string, nextRunAtUtc: string): Promise<void> {
    const recurring = this.recurring.get(jobName);
    if (!recurring) {
      return;
    }
    recurring.nextRunAtUtc = nextRunAtUtc;
  }

  async tryMaterializeRecurringRun(
    recurring: RecurringJobState,
    registration: DurableJobRegistration,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const stored = this.recurring.get(recurring.jobName);
    if (!stored || stored.nextRunAtUtc !== recurring.nextRunAtUtc || !stored.enabled) {
      return false;
    }

    const hasSameSlot = Array.from(this.runs.values()).some(
      (run) => run.jobName === recurring.jobName && run.scheduleSlotUtc === recurring.nextRunAtUtc
    );
    if (hasSameSlot) {
      return false;
    }

    if (!stored.allowConcurrentRuns) {
      const active = Array.from(this.runs.values()).some(
        (run) => run.jobName === recurring.jobName && isActiveStatus(run.status)
      );
      if (active) {
        return false;
      }
    }

    const runId = generateId();
    this.runs.set(runId, {
      id: runId,
      jobName: recurring.jobName,
      jobType: registration.jobType,
      status: RUN_STATUS.PENDING,
      scheduledForUtc: recurring.nextRunAtUtc,
      scheduleSlotUtc: recurring.nextRunAtUtc,
      attempt: 0,
      maxAttempts: recurring.maxAttempts,
      payloadJson: undefined
    });

    stored.nextRunAtUtc = nextRunAtUtc;
    return true;
  }

  async extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const run = this.runs.get(runId);
    if (!run) {
      return false;
    }

    if (run.status !== RUN_STATUS.LEASED || run.leaseOwner !== workerName) {
      return false;
    }

    run.leaseUntilUtc = addSeconds(nowIso(), leaseDurationSeconds);
    return true;
  }

  async tryLeaseRuntimeCommandReceipt(commandId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const now = nowIso();
    const existing = this.runtimeReceipts.get(commandId);
    if (!existing) {
      this.runtimeReceipts.set(commandId, {
        commandId,
        status: RUNTIME_COMMAND_RECEIPT_STATUS.LEASED,
        recordedAtUtc: now,
        leaseOwner: workerName,
        leaseUntilUtc: addSeconds(now, leaseDurationSeconds)
      });
      return true;
    }

    const terminal =
      existing.status === RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED ||
      existing.status === RUNTIME_COMMAND_RECEIPT_STATUS.FAILED;
    if (terminal) {
      return false;
    }

    if (!existing.leaseUntilUtc || compareIsoAsc(existing.leaseUntilUtc, now) <= 0 || existing.leaseOwner === workerName) {
      existing.status = RUNTIME_COMMAND_RECEIPT_STATUS.LEASED;
      existing.recordedAtUtc = now;
      existing.leaseOwner = workerName;
      existing.leaseUntilUtc = addSeconds(now, leaseDurationSeconds);
      return true;
    }

    return false;
  }

  async markRuntimeCommandAcknowledged(commandId: string, workerName: string, recordedAtUtc: string): Promise<boolean> {
    const existing = this.runtimeReceipts.get(commandId);
    if (!existing || existing.leaseOwner !== workerName) {
      return false;
    }
    existing.status = RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED;
    existing.recordedAtUtc = recordedAtUtc;
    return true;
  }

  async markRuntimeCommandSucceeded(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    runId: string | undefined
  ): Promise<boolean> {
    const existing = this.runtimeReceipts.get(commandId);
    if (!existing || existing.leaseOwner !== workerName) {
      return false;
    }
    existing.status = RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED;
    existing.recordedAtUtc = recordedAtUtc;
    existing.completedAtUtc = completedAtUtc;
    existing.runId = runId;
    existing.leaseOwner = undefined;
    existing.leaseUntilUtc = undefined;
    return true;
  }

  async markRuntimeCommandFailed(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    errorCode: string | undefined,
    errorMessage: string | undefined
  ): Promise<boolean> {
    const existing = this.runtimeReceipts.get(commandId);
    if (!existing || existing.leaseOwner !== workerName) {
      return false;
    }
    existing.status = RUNTIME_COMMAND_RECEIPT_STATUS.FAILED;
    existing.recordedAtUtc = recordedAtUtc;
    existing.completedAtUtc = completedAtUtc;
    existing.errorCode = errorCode;
    existing.errorMessage = errorMessage;
    existing.leaseOwner = undefined;
    existing.leaseUntilUtc = undefined;
    return true;
  }

  async getRuntimeCommandReceipts(take: number): Promise<RuntimeCommandReceiptRecord[]> {
    return Array.from(this.runtimeReceipts.values())
      .filter((receipt) =>
        !receipt.uploadedAtUtc
        && (
          receipt.status === RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED
          || receipt.status === RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED
          || receipt.status === RUNTIME_COMMAND_RECEIPT_STATUS.FAILED
        )
      )
      .sort((a, b) => compareIsoAsc(a.recordedAtUtc, b.recordedAtUtc))
      .slice(0, Math.max(1, take))
      .map((receipt) => ({ ...receipt }));
  }

  async markRuntimeCommandReceiptUploaded(commandId: string): Promise<boolean> {
    const existing = this.runtimeReceipts.get(commandId);
    if (!existing) {
      return false;
    }

    existing.uploadedAtUtc = nowIso();
    return true;
  }
}
