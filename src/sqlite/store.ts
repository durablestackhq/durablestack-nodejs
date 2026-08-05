import { DatabaseSync } from "node:sqlite";
import type {
  DurableJobRegistration,
  DurableJobStore,
  JobRunRecord,
  RecurringJobState,
  RuntimeCommandReceiptRecord
} from "../types.js";
import type { RunStatus } from "../constants.js";
import type { SqliteDurableStackOptions } from "./types.js";
import { createSqliteDatabase } from "./migrator.js";
import { resolveSqliteTableNames } from "./table-names.js";

function notImplemented(): never {
  throw new Error("SQLite provider methods are not implemented yet.");
}

export class SqliteDurableJobStore implements DurableJobStore {
  private db: DatabaseSync | undefined;
  private readonly options: SqliteDurableStackOptions;
  private readonly tables: {
    jobs: string;
    runs: string;
    migrations: string;
    runtimeCommandReceipts: string;
  };

  public constructor(options: SqliteDurableStackOptions) {
    this.options = options;
    this.tables = resolveSqliteTableNames(options.databaseTablePrefix);
  }

  public async connect(): Promise<void> {
    if (!this.db) {
      this.db = await createSqliteDatabase(this.options.databasePath);
    }
  }

  public getDatabase(): DatabaseSync {
    if (!this.db) {
      throw new Error("SQLite store is not connected.");
    }
    return this.db;
  }

  public getTables(): { jobs: string; runs: string; migrations: string; runtimeCommandReceipts: string } {
    return this.tables;
  }

  public async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = undefined;
    }
  }

  public async enqueue(
    _jobName: string,
    _jobType: string,
    _payloadJson: string | undefined,
    _scheduledForUtc: string,
    _maxAttempts: number
  ): Promise<string> {
    return notImplemented();
  }

  public async claimDueRuns(_workerName: string, _batchSize: number, _leaseDurationSeconds: number): Promise<JobRunRecord[]> {
    return notImplemented();
  }

  public async markSucceeded(_runId: string, _workerName: string): Promise<boolean> {
    return notImplemented();
  }

  public async cancelRun(_runId: string): Promise<boolean> {
    return notImplemented();
  }

  public async markFailed(
    _runId: string,
    _workerName: string,
    _errorMessage: string,
    _retry: boolean,
    _retryAtUtc: string | undefined
  ): Promise<boolean> {
    return notImplemented();
  }

  public async getRun(_runId: string): Promise<JobRunRecord | undefined> {
    return notImplemented();
  }

  public async getRecentRuns(_take: number): Promise<JobRunRecord[]> {
    return notImplemented();
  }

  public async getRuns(): Promise<JobRunRecord[]> {
    return notImplemented();
  }

  public async getRunsByJobName(_jobName: string, _take: number): Promise<JobRunRecord[]> {
    return notImplemented();
  }

  public async getRunsByStatus(_status: RunStatus, _take: number): Promise<JobRunRecord[]> {
    return notImplemented();
  }

  public async getEnqueuedRuns(_take: number): Promise<JobRunRecord[]> {
    return notImplemented();
  }

  public async tryEnqueueIfNoActiveRun(
    _jobName: string,
    _jobType: string,
    _payloadJson: string | undefined,
    _scheduledForUtc: string,
    _maxAttempts: number
  ): Promise<string | undefined> {
    return notImplemented();
  }

  public async getRecurringJobs(_includeDisabled: boolean): Promise<RecurringJobState[]> {
    return notImplemented();
  }

  public async setRecurringJobEnabled(
    _jobName: string,
    _enabled: boolean,
    _nextRunAtUtc: string | undefined
  ): Promise<boolean> {
    return notImplemented();
  }

  public async updateRecurringJobSchedule(
    _jobName: string,
    _cronExpression: string,
    _timeZone: string,
    _nextRunAtUtc: string
  ): Promise<boolean> {
    return notImplemented();
  }

  public async pruneHistoricalRuns(_completedBeforeUtc: string, _batchSize: number): Promise<number> {
    return notImplemented();
  }

  public async upsertRecurringJob(_registration: DurableJobRegistration, _nextRunAtUtc: string): Promise<void> {
    return notImplemented();
  }

  public async getDueRecurringJobs(_nowUtc: string, _batchSize: number): Promise<RecurringJobState[]> {
    return notImplemented();
  }

  public async updateRecurringNextRun(_jobName: string, _nextRunAtUtc: string): Promise<void> {
    return notImplemented();
  }

  public async tryMaterializeRecurringRun(
    _recurring: RecurringJobState,
    _registration: DurableJobRegistration,
    _nextRunAtUtc: string
  ): Promise<boolean> {
    return notImplemented();
  }

  public async extendLease(_runId: string, _workerName: string, _leaseDurationSeconds: number): Promise<boolean> {
    return notImplemented();
  }

  public async tryLeaseRuntimeCommandReceipt(
    _commandId: string,
    _workerName: string,
    _leaseDurationSeconds: number
  ): Promise<boolean> {
    return notImplemented();
  }

  public async markRuntimeCommandAcknowledged(
    _commandId: string,
    _workerName: string,
    _recordedAtUtc: string
  ): Promise<boolean> {
    return notImplemented();
  }

  public async markRuntimeCommandSucceeded(
    _commandId: string,
    _workerName: string,
    _recordedAtUtc: string,
    _completedAtUtc: string,
    _runId: string | undefined
  ): Promise<boolean> {
    return notImplemented();
  }

  public async markRuntimeCommandFailed(
    _commandId: string,
    _workerName: string,
    _recordedAtUtc: string,
    _completedAtUtc: string,
    _errorCode: string | undefined,
    _errorMessage: string | undefined
  ): Promise<boolean> {
    return notImplemented();
  }

  public async getRuntimeCommandReceipts(_take: number): Promise<RuntimeCommandReceiptRecord[]> {
    return notImplemented();
  }

  public async markRuntimeCommandReceiptUploaded(_commandId: string): Promise<boolean> {
    return notImplemented();
  }
}
