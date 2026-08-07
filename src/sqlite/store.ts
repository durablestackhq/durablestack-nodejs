import { DatabaseSync } from "node:sqlite";
import {
  RUN_STATUS,
  RUNTIME_COMMAND_RECEIPT_STATUS,
  type RunStatus
} from "../constants.js";
import type {
  DurableJobRegistration,
  DurableJobStore,
  JobRunRecord,
  RecurringJobState,
  RuntimeCommandReceiptRecord
} from "../types.js";
import { addSeconds, generateId } from "../utils.js";
import type { SqliteDurableStackOptions } from "./types.js";
import { createSqliteDatabase } from "./migrator.js";
import { resolveSqliteTableNames } from "./table-names.js";

function nowIso(): string {
  return new Date().toISOString();
}

function q(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function toIsoUtc(value: unknown): string | undefined {
  if (value === null || typeof value === "undefined") {
    return undefined;
  }
  const text = String(value).trim();
  if (!text) {
    return undefined;
  }
  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const withZone = /z$|[+-]\d\d:?\d\d$/i.test(normalized) ? normalized : `${normalized}Z`;
  return new Date(withZone).toISOString();
}

function toDateIso(value: unknown): string {
  const iso = toIsoUtc(value);
  if (!iso) {
    throw new Error("Expected datetime value.");
  }
  return iso;
}

function toBool(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  return String(value) === "1" || String(value).toLowerCase() === "true";
}

function changes(result: unknown): number {
  const rowChanges = Number((result as { changes?: number })?.changes ?? 0);
  return Number.isFinite(rowChanges) ? rowChanges : 0;
}

function rowToRun(row: Record<string, unknown>): JobRunRecord {
  return {
    id: String(row.id),
    jobName: String(row.job_name),
    jobType: String(row.job_type),
    status: String(row.status) as RunStatus,
    scheduledForUtc: toDateIso(row.scheduled_for_utc),
    scheduleSlotUtc: toIsoUtc(row.schedule_slot_utc),
    startedAtUtc: toIsoUtc(row.started_at_utc),
    completedAtUtc: toIsoUtc(row.completed_at_utc),
    attempt: Number(row.attempt),
    maxAttempts: Number(row.max_attempts),
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: toIsoUtc(row.lease_until_utc),
    payloadJson: row.payload_json ? String(row.payload_json) : undefined,
    errorMessage: row.error_message ? String(row.error_message) : undefined
  };
}

function rowToRecurring(row: Record<string, unknown>): RecurringJobState {
  return {
    jobName: String(row.job_name),
    jobType: String(row.job_type),
    cronExpression: String(row.cron_expression),
    timeZone: String(row.time_zone),
    maxAttempts: Number(row.max_attempts),
    enabled: toBool(row.enabled),
    allowConcurrentRuns: toBool(row.allow_concurrent_runs),
    retryBehavior: row.retry_behavior ? String(row.retry_behavior) as "fixed" | "backoff" : undefined,
    retryInitialDelaySeconds: row.retry_initial_delay_seconds ? Number(row.retry_initial_delay_seconds) : undefined,
    nextRunAtUtc: toDateIso(row.next_run_at_utc)
  };
}

function rowToReceipt(row: Record<string, unknown>): RuntimeCommandReceiptRecord {
  return {
    commandId: String(row.command_id),
    status: String(row.status) as RuntimeCommandReceiptRecord["status"],
    errorCode: row.error_code ? String(row.error_code) : undefined,
    errorMessage: row.error_message ? String(row.error_message) : undefined,
    runId: row.run_id ? String(row.run_id) : undefined,
    recordedAtUtc: toDateIso(row.recorded_at_utc),
    completedAtUtc: toIsoUtc(row.completed_at_utc),
    uploadedAtUtc: toIsoUtc(row.uploaded_at_utc),
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: toIsoUtc(row.lease_until_utc)
  };
}

function isBusyOrConstraint(error: unknown): boolean {
  const code = String((error as { code?: string })?.code ?? "");
  return code === "SQLITE_BUSY" || code === "SQLITE_CONSTRAINT";
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

  private getDb(): DatabaseSync {
    if (!this.db) {
      throw new Error("SQLite store is not connected.");
    }
    return this.db;
  }

  public getDatabase(): DatabaseSync {
    return this.getDb();
  }

  public async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = undefined;
    }
  }

  public async enqueue(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string> {
    const db = this.getDb();
    const id = generateId();
    db.prepare(`
      insert into ${q(this.tables.runs)}
      (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
      values (?, ?, ?, 'pending', ?, null, null, null, 0, ?, null, null, ?, null, ?)
    `).run(id, jobName, jobType, scheduledForUtc, Math.max(1, Math.floor(maxAttempts)), payloadJson ?? null, nowIso());
    return id;
  }

  public async claimDueRuns(workerName: string, batchSize: number, leaseDurationSeconds: number): Promise<JobRunRecord[]> {
    const db = this.getDb();
    const now = nowIso();
    const leaseUntil = addSeconds(now, Math.max(1, Math.floor(leaseDurationSeconds)));

    db.exec("begin immediate transaction;");
    try {
      const due = db.prepare(`
        select *
        from ${q(this.tables.runs)}
        where (
          status = 'pending' and scheduled_for_utc <= ?
        ) or (
          status = 'leased' and (lease_until_utc is null or lease_until_utc <= ?)
        )
        order by scheduled_for_utc asc
        limit ?
      `).all(now, now, Math.max(1, Math.floor(batchSize))) as Array<Record<string, unknown>>;

      const claimed: JobRunRecord[] = [];
      for (const row of due) {
        const id = String(row.id);
        const attempt = Number(row.attempt);
        const maxAttempts = Number(row.max_attempts);
        const nextAttempt = attempt + 1;

        if (nextAttempt > maxAttempts) {
          db.prepare(`
            update ${q(this.tables.runs)}
            set attempt = ?,
                status = 'failed',
                started_at_utc = coalesce(started_at_utc, ?),
                completed_at_utc = ?,
                error_message = coalesce(error_message, 'Run exceeded max attempts before claim'),
                lease_owner = null,
                lease_until_utc = null
            where id = ?
          `).run(nextAttempt, now, now, id);
          continue;
        }

        db.prepare(`
          update ${q(this.tables.runs)}
          set attempt = ?,
              status = 'leased',
              started_at_utc = coalesce(started_at_utc, ?),
              lease_owner = ?,
              lease_until_utc = ?
          where id = ?
        `).run(nextAttempt, now, workerName, leaseUntil, id);

        const claimedRow = db.prepare(`select * from ${q(this.tables.runs)} where id = ?`).get(id) as Record<string, unknown> | undefined;
        if (claimedRow && String(claimedRow.status) === RUN_STATUS.LEASED) {
          claimed.push(rowToRun(claimedRow));
        }
      }

      db.exec("commit;");
      return claimed;
    } catch (error) {
      db.exec("rollback;");
      throw error;
    }
  }

  public async markSucceeded(runId: string, workerName: string): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runs)}
      set status = 'succeeded', completed_at_utc = ?, lease_owner = null, lease_until_utc = null, error_message = null
      where id = ? and status = 'leased' and lease_owner = ?
    `).run(nowIso(), runId, workerName);
    return changes(result) > 0;
  }

  public async cancelRun(runId: string): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runs)}
      set status = 'failed', completed_at_utc = ?, error_message = 'Run cancelled', lease_owner = null, lease_until_utc = null
      where id = ? and status in ('pending', 'leased')
    `).run(nowIso(), runId);
    return changes(result) > 0;
  }

  public async markFailed(
    runId: string,
    workerName: string,
    errorMessage: string,
    retry: boolean,
    retryAtUtc: string | undefined
  ): Promise<boolean> {
    const db = this.getDb();
    if (retry && retryAtUtc) {
      const retried = db.prepare(`
        update ${q(this.tables.runs)}
        set status = 'pending',
            scheduled_for_utc = ?,
            lease_owner = null,
            lease_until_utc = null,
            completed_at_utc = null,
            error_message = ?
        where id = ? and status = 'leased' and lease_owner = ?
      `).run(retryAtUtc, errorMessage, runId, workerName);
      return changes(retried) > 0;
    }

    const failed = db.prepare(`
      update ${q(this.tables.runs)}
      set status = 'failed', completed_at_utc = ?, lease_owner = null, lease_until_utc = null, error_message = ?
      where id = ? and status = 'leased' and lease_owner = ?
    `).run(nowIso(), errorMessage, runId, workerName);
    return changes(failed) > 0;
  }

  public async getRun(runId: string): Promise<JobRunRecord | undefined> {
    const db = this.getDb();
    const row = db.prepare(`select * from ${q(this.tables.runs)} where id = ?`).get(runId) as Record<string, unknown> | undefined;
    return row ? rowToRun(row) : undefined;
  }

  public async getRecentRuns(take: number): Promise<JobRunRecord[]> {
    const db = this.getDb();
    const rows = db.prepare(`
      select *
      from ${q(this.tables.runs)}
      order by scheduled_for_utc desc
      limit ?
    `).all(Math.max(1, Math.floor(take))) as Array<Record<string, unknown>>;
    return rows.map((row) => rowToRun(row));
  }

  public async getRuns(): Promise<JobRunRecord[]> {
    const db = this.getDb();
    const rows = db.prepare(`select * from ${q(this.tables.runs)} order by scheduled_for_utc desc`).all() as Array<Record<string, unknown>>;
    return rows.map((row) => rowToRun(row));
  }

  public async getRunsByJobName(jobName: string, take: number): Promise<JobRunRecord[]> {
    const db = this.getDb();
    const rows = db.prepare(`
      select *
      from ${q(this.tables.runs)}
      where job_name = ?
      order by scheduled_for_utc desc
      limit ?
    `).all(jobName, Math.max(1, Math.floor(take))) as Array<Record<string, unknown>>;
    return rows.map((row) => rowToRun(row));
  }

  public async getRunsByStatus(status: RunStatus, take: number): Promise<JobRunRecord[]> {
    const db = this.getDb();
    const rows = db.prepare(`
      select *
      from ${q(this.tables.runs)}
      where status = ?
      order by scheduled_for_utc desc
      limit ?
    `).all(status, Math.max(1, Math.floor(take))) as Array<Record<string, unknown>>;
    return rows.map((row) => rowToRun(row));
  }

  public async getEnqueuedRuns(take: number): Promise<JobRunRecord[]> {
    const db = this.getDb();
    const rows = db.prepare(`
      select *
      from ${q(this.tables.runs)}
      where status = 'pending' and schedule_slot_utc is null
      order by scheduled_for_utc desc
      limit ?
    `).all(Math.max(1, Math.floor(take))) as Array<Record<string, unknown>>;
    return rows.map((row) => rowToRun(row));
  }

  public async tryEnqueueIfNoActiveRun(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string | undefined> {
    const db = this.getDb();
    const id = generateId();
    const result = db.prepare(`
      insert into ${q(this.tables.runs)}
      (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
      select ?, ?, ?, 'pending', ?, null, null, null, 0, ?, null, null, ?, null, ?
      where not exists (
        select 1
        from ${q(this.tables.runs)}
        where job_name = ? and status in ('pending', 'leased')
      )
    `).run(
      id,
      jobName,
      jobType,
      scheduledForUtc,
      Math.max(1, Math.floor(maxAttempts)),
      payloadJson ?? null,
      nowIso(),
      jobName
    );
    return changes(result) > 0 ? id : undefined;
  }

  public async getRecurringJobs(includeDisabled: boolean): Promise<RecurringJobState[]> {
    const db = this.getDb();
    const rows = includeDisabled
      ? db.prepare(`select * from ${q(this.tables.jobs)} order by job_name asc`).all()
      : db.prepare(`select * from ${q(this.tables.jobs)} where enabled = 1 order by job_name asc`).all();
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRecurring(row));
  }

  public async setRecurringJobEnabled(
    jobName: string,
    enabled: boolean,
    nextRunAtUtc: string | undefined
  ): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.jobs)}
      set enabled = ?,
          next_run_at_utc = coalesce(?, next_run_at_utc),
          updated_at_utc = ?
      where job_name = ?
    `).run(enabled ? 1 : 0, nextRunAtUtc ?? null, nowIso(), jobName);
    return changes(result) > 0;
  }

  public async updateRecurringJobSchedule(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.jobs)}
      set cron_expression = ?,
          time_zone = ?,
          next_run_at_utc = ?,
          updated_at_utc = ?
      where job_name = ?
    `).run(cronExpression, timeZone, nextRunAtUtc, nowIso(), jobName);
    return changes(result) > 0;
  }

  public async pruneHistoricalRuns(completedBeforeUtc: string, batchSize: number): Promise<number> {
    const db = this.getDb();
    const result = db.prepare(`
      delete from ${q(this.tables.runs)}
      where id in (
        select id
        from ${q(this.tables.runs)}
        where status in ('succeeded', 'failed')
          and completed_at_utc is not null
          and completed_at_utc < ?
        order by completed_at_utc asc
        limit ?
      )
    `).run(completedBeforeUtc, Math.max(1, Math.floor(batchSize)));
    return changes(result);
  }

  public async upsertRecurringJob(registration: DurableJobRegistration, nextRunAtUtc: string): Promise<void> {
    if (!registration.recurring) {
      return;
    }
    const db = this.getDb();
    db.prepare(`
      insert into ${q(this.tables.jobs)}
      (job_name, job_type, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs, retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      on conflict(job_name) do update set
        job_type = excluded.job_type,
        cron_expression = excluded.cron_expression,
        time_zone = excluded.time_zone,
        max_attempts = excluded.max_attempts,
        enabled = excluded.enabled,
        allow_concurrent_runs = excluded.allow_concurrent_runs,
        retry_behavior = excluded.retry_behavior,
        retry_initial_delay_seconds = excluded.retry_initial_delay_seconds,
        updated_at_utc = excluded.updated_at_utc
    `).run(
      registration.jobName,
      registration.jobType,
      registration.recurring.cronExpression,
      registration.recurring.timeZone,
      Math.max(1, Math.floor(registration.maxAttempts)),
      registration.recurring.enabled ?? true ? 1 : 0,
      registration.recurring.allowConcurrentRuns ?? false ? 1 : 0,
      registration.recurring.retryBehavior ?? null,
      registration.recurring.retryInitialDelaySeconds ?? null,
      nextRunAtUtc,
      nowIso()
    );
  }

  public async getDueRecurringJobs(nowUtc: string, batchSize: number): Promise<RecurringJobState[]> {
    const db = this.getDb();
    const rows = db.prepare(`
      select *
      from ${q(this.tables.jobs)}
      where enabled = 1
        and next_run_at_utc <= ?
      order by next_run_at_utc asc
      limit ?
    `).all(nowUtc, Math.max(1, Math.floor(batchSize))) as Array<Record<string, unknown>>;
    return rows.map((row) => rowToRecurring(row));
  }

  public async updateRecurringNextRun(jobName: string, nextRunAtUtc: string): Promise<void> {
    const db = this.getDb();
    db.prepare(`
      update ${q(this.tables.jobs)}
      set next_run_at_utc = ?,
          updated_at_utc = ?
      where job_name = ?
    `).run(nextRunAtUtc, nowIso(), jobName);
  }

  public async tryMaterializeRecurringRun(
    recurring: RecurringJobState,
    registration: DurableJobRegistration,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const db = this.getDb();
    db.exec("begin immediate transaction;");
    try {
      if (!recurring.allowConcurrentRuns) {
        const active = db.prepare(`
          select 1
          from ${q(this.tables.runs)}
          where job_name = ?
            and status in ('pending', 'leased')
          limit 1
        `).get(recurring.jobName) as Record<string, unknown> | undefined;
        if (active) {
          db.exec("rollback;");
          return false;
        }
      }

      const lockRow = db.prepare(`
        select next_run_at_utc, enabled
        from ${q(this.tables.jobs)}
        where job_name = ?
      `).get(recurring.jobName) as Record<string, unknown> | undefined;

      if (!lockRow || !toBool(lockRow.enabled)) {
        db.exec("rollback;");
        return false;
      }

      const currentNext = Date.parse(toDateIso(lockRow.next_run_at_utc));
      const expectedNext = Date.parse(recurring.nextRunAtUtc);
      if (Math.abs(currentNext - expectedNext) > 1) {
        db.exec("rollback;");
        return false;
      }

      db.prepare(`
        update ${q(this.tables.jobs)}
        set next_run_at_utc = ?,
            updated_at_utc = ?
        where job_name = ?
      `).run(nextRunAtUtc, nowIso(), recurring.jobName);

      db.prepare(`
        insert into ${q(this.tables.runs)}
        (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
        values (?, ?, ?, 'pending', ?, ?, null, null, 0, ?, null, null, null, null, ?)
      `).run(
        generateId(),
        recurring.jobName,
        registration.jobType,
        recurring.nextRunAtUtc,
        recurring.nextRunAtUtc,
        recurring.maxAttempts,
        nowIso()
      );

      db.exec("commit;");
      return true;
    } catch (error) {
      db.exec("rollback;");
      if (isBusyOrConstraint(error)) {
        return false;
      }
      throw error;
    }
  }

  public async extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runs)}
      set lease_until_utc = ?
      where id = ?
        and status = 'leased'
        and lease_owner = ?
    `).run(addSeconds(nowIso(), Math.max(1, Math.floor(leaseDurationSeconds))), runId, workerName);
    return changes(result) > 0;
  }

  public async tryLeaseRuntimeCommandReceipt(
    commandId: string,
    workerName: string,
    leaseDurationSeconds: number
  ): Promise<boolean> {
    const db = this.getDb();
    const now = nowIso();
    const leaseUntil = addSeconds(now, Math.max(1, Math.floor(leaseDurationSeconds)));

    db.exec("begin immediate transaction;");
    try {
      const existing = db.prepare(`
        select lease_owner, lease_until_utc
        from ${q(this.tables.runtimeCommandReceipts)}
        where command_id = ?
      `).get(commandId) as Record<string, unknown> | undefined;

      if (!existing) {
        db.prepare(`
          insert into ${q(this.tables.runtimeCommandReceipts)}
          (command_id, status, error_code, error_message, run_id, recorded_at_utc, completed_at_utc, uploaded_at_utc, lease_owner, lease_until_utc)
          values (?, ?, null, null, null, ?, null, null, ?, ?)
        `).run(commandId, RUNTIME_COMMAND_RECEIPT_STATUS.LEASED, now, workerName, leaseUntil);
        db.exec("commit;");
        return true;
      }

      const owner = existing.lease_owner ? String(existing.lease_owner) : undefined;
      const leaseUntilIso = toIsoUtc(existing.lease_until_utc);
      const expired = !leaseUntilIso || Date.parse(leaseUntilIso) <= Date.now();
      const sameOwner = owner === workerName;

      if (!expired && !sameOwner) {
        db.exec("rollback;");
        return false;
      }

      db.prepare(`
        update ${q(this.tables.runtimeCommandReceipts)}
        set status = ?,
            recorded_at_utc = ?,
            lease_owner = ?,
            lease_until_utc = ?
        where command_id = ?
      `).run(RUNTIME_COMMAND_RECEIPT_STATUS.LEASED, now, workerName, leaseUntil, commandId);

      db.exec("commit;");
      return true;
    } catch (error) {
      db.exec("rollback;");
      if (isBusyOrConstraint(error)) {
        return false;
      }
      throw error;
    }
  }

  public async markRuntimeCommandAcknowledged(
    commandId: string,
    workerName: string,
    recordedAtUtc: string
  ): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runtimeCommandReceipts)}
      set status = ?,
          recorded_at_utc = ?
      where command_id = ?
        and lease_owner = ?
    `).run(RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED, recordedAtUtc, commandId, workerName);
    return changes(result) > 0;
  }

  public async markRuntimeCommandSucceeded(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    runId: string | undefined
  ): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runtimeCommandReceipts)}
      set status = ?,
          recorded_at_utc = ?,
          completed_at_utc = ?,
          run_id = ?,
          lease_owner = null,
          lease_until_utc = null
      where command_id = ?
        and lease_owner = ?
    `).run(RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED, recordedAtUtc, completedAtUtc, runId ?? null, commandId, workerName);
    return changes(result) > 0;
  }

  public async markRuntimeCommandFailed(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    errorCode: string | undefined,
    errorMessage: string | undefined
  ): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runtimeCommandReceipts)}
      set status = ?,
          recorded_at_utc = ?,
          completed_at_utc = ?,
          error_code = ?,
          error_message = ?,
          lease_owner = null,
          lease_until_utc = null
      where command_id = ?
        and lease_owner = ?
    `).run(
      RUNTIME_COMMAND_RECEIPT_STATUS.FAILED,
      recordedAtUtc,
      completedAtUtc,
      errorCode ?? null,
      errorMessage ?? null,
      commandId,
      workerName
    );
    return changes(result) > 0;
  }

  public async getRuntimeCommandReceipts(take: number): Promise<RuntimeCommandReceiptRecord[]> {
    const db = this.getDb();
    const rows = db.prepare(`
      select *
      from ${q(this.tables.runtimeCommandReceipts)}
      where uploaded_at_utc is null
        and status in (?, ?, ?)
      order by recorded_at_utc asc
      limit ?
    `).all(
      RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED,
      RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED,
      RUNTIME_COMMAND_RECEIPT_STATUS.FAILED,
      Math.max(1, Math.floor(take))
    ) as Array<Record<string, unknown>>;
    return rows.map((row) => rowToReceipt(row));
  }

  public async markRuntimeCommandReceiptUploaded(commandId: string): Promise<boolean> {
    const db = this.getDb();
    const result = db.prepare(`
      update ${q(this.tables.runtimeCommandReceipts)}
      set uploaded_at_utc = ?
      where command_id = ?
    `).run(nowIso(), commandId);
    return changes(result) > 0;
  }
}
