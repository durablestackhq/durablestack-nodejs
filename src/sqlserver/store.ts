import sql from "mssql";
import { randomUUID } from "node:crypto";
import type {
  DurableJobRegistration,
  DurableJobStore,
  JobRunRecord,
  RecurringJobState,
  RuntimeCommandReceiptRecord
} from "../types.js";
import { RUNTIME_COMMAND_RECEIPT_STATUS, type RunStatus } from "../constants.js";
import type { SqlServerDurableStackOptions } from "./types.js";
import { resolveSqlServerTableNames } from "./table-names.js";

function q(name: string): string {
  return `[${name.replace(/]/g, "]]")}]`;
}

function toIsoUtc(value: unknown): string | undefined {
  if (value === null || typeof value === "undefined") {
    return undefined;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  const text = String(value).trim();
  if (!text) {
    return undefined;
  }
  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const withZone = /z$|[+-]\d\d:?\d\d$/i.test(normalized) ? normalized : `${normalized}Z`;
  return new Date(withZone).toISOString();
}

function toDate(value: unknown): Date {
  const iso = toIsoUtc(value);
  if (!iso) {
    throw new Error("Expected datetime value.");
  }
  return new Date(iso);
}

function toNVarCharJson(payload: unknown): string | undefined {
  if (payload === null || typeof payload === "undefined") {
    return undefined;
  }
  return typeof payload === "string" ? payload : JSON.stringify(payload);
}

function toCanonicalId(value: unknown): string {
  return String(value).toLowerCase();
}

function rowToRun(row: Record<string, unknown>): JobRunRecord {
  return {
    id: toCanonicalId(row.id),
    jobName: String(row.job_name),
    jobType: String(row.job_type),
    status: String(row.status) as RunStatus,
    scheduledForUtc: toDate(row.scheduled_for_utc).toISOString(),
    scheduleSlotUtc: row.schedule_slot_utc ? toDate(row.schedule_slot_utc).toISOString() : undefined,
    startedAtUtc: row.started_at_utc ? toDate(row.started_at_utc).toISOString() : undefined,
    completedAtUtc: row.completed_at_utc ? toDate(row.completed_at_utc).toISOString() : undefined,
    attempt: Number(row.attempt),
    maxAttempts: Number(row.max_attempts),
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: row.lease_until_utc ? toDate(row.lease_until_utc).toISOString() : undefined,
    payloadJson: toNVarCharJson(row.payload_json),
    errorMessage: row.error_message ? String(row.error_message) : undefined
  };
}

function toBool(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  return String(value).toLowerCase() === "true" || String(value) === "1";
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
    nextRunAtUtc: toDate(row.next_run_at_utc).toISOString()
  };
}

function rowToReceipt(row: Record<string, unknown>): RuntimeCommandReceiptRecord {
  return {
    commandId: String(row.command_id),
    status: String(row.status) as RuntimeCommandReceiptRecord["status"],
    errorCode: row.error_code ? String(row.error_code) : undefined,
    errorMessage: row.error_message ? String(row.error_message) : undefined,
    runId: row.run_id ? toCanonicalId(row.run_id) : undefined,
    recordedAtUtc: toDate(row.recorded_at_utc).toISOString(),
    completedAtUtc: row.completed_at_utc ? toDate(row.completed_at_utc).toISOString() : undefined,
    uploadedAtUtc: row.uploaded_at_utc ? toDate(row.uploaded_at_utc).toISOString() : undefined,
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: row.lease_until_utc ? toDate(row.lease_until_utc).toISOString() : undefined
  };
}

function notImplemented(): never {
  throw new Error("SQL Server provider methods are not implemented yet.");
}

export class SqlServerDurableJobStore implements DurableJobStore {
  private readonly pool: sql.ConnectionPool;
  private connectPromise: Promise<sql.ConnectionPool> | undefined;
  private readonly tables: {
    jobs: string;
    runs: string;
    migrations: string;
    runtimeCommandReceipts: string;
  };

  public constructor(options: SqlServerDurableStackOptions) {
    this.pool = new sql.ConnectionPool(options.connectionString);
    this.tables = resolveSqlServerTableNames(options.databaseTablePrefix);
  }

  public async connect(): Promise<void> {
    if (!this.connectPromise) {
      this.connectPromise = this.pool.connect();
    }
    await this.connectPromise;
  }

  public getPool(): sql.ConnectionPool {
    return this.pool;
  }

  public async close(): Promise<void> {
    if (this.connectPromise) {
      await this.connectPromise;
      await this.pool.close();
    }
  }

  public async enqueue(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string> {
    const id = randomUUID();
    await this.pool.request()
      .input("id", sql.UniqueIdentifier, id)
      .input("jobName", sql.NVarChar(256), jobName)
      .input("jobType", sql.NVarChar(256), jobType)
      .input("scheduledForUtc", sql.DateTime2(3), new Date(scheduledForUtc))
      .input("maxAttempts", sql.Int, Math.max(1, Math.floor(maxAttempts)))
      .input("payloadJson", sql.NVarChar(sql.MAX), payloadJson ?? null)
      .query(`
        insert into ${q(this.tables.runs)}
        (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
        values (@id, @jobName, @jobType, 'pending', @scheduledForUtc, null, null, null, 0, @maxAttempts, null, null, @payloadJson, null, sysutcdatetime())
      `);
    return id;
  }

  public async claimDueRuns(
    workerName: string,
    batchSize: number,
    leaseDurationSeconds: number
  ): Promise<JobRunRecord[]> {
    const tx = this.pool.transaction();
    await tx.begin();
    try {
      const candidateResult = await tx.request()
        .input("batchSize", sql.Int, Math.max(1, Math.floor(batchSize)))
        .query(`
          select top (@batchSize) id
          from ${q(this.tables.runs)} with (updlock, rowlock, readpast)
          where
            (status = 'pending' and scheduled_for_utc <= sysutcdatetime())
            or
            (status = 'leased' and (lease_until_utc is null or lease_until_utc <= sysutcdatetime()))
          order by scheduled_for_utc asc
        `);

      const ids = candidateResult.recordset.map((x) => String((x as { id: string }).id));
      if (ids.length === 0) {
        await tx.rollback();
        return [];
      }

      const idsSql = ids.map((x) => `'${x}'`).join(", ");
      await tx.request()
        .input("workerName", sql.NVarChar(256), workerName)
        .input("leaseSeconds", sql.Int, Math.max(1, Math.floor(leaseDurationSeconds)))
        .query(`
          update ${q(this.tables.runs)}
          set
            attempt = attempt + 1,
            status = case when (attempt + 1) > max_attempts then 'failed' else 'leased' end,
            started_at_utc = isnull(started_at_utc, sysutcdatetime()),
            completed_at_utc = case when (attempt + 1) > max_attempts then sysutcdatetime() else completed_at_utc end,
            error_message = case when (attempt + 1) > max_attempts then isnull(error_message, 'Run exceeded max attempts before claim') else error_message end,
            lease_owner = case when (attempt + 1) > max_attempts then null else @workerName end,
            lease_until_utc = case when (attempt + 1) > max_attempts then null else dateadd(second, @leaseSeconds, sysutcdatetime()) end
          where id in (${idsSql})
        `);

      const leasedResult = await tx.request().query(`
        select *
        from ${q(this.tables.runs)}
        where status = 'leased' and id in (${idsSql})
        order by scheduled_for_utc asc
      `);

      await tx.commit();
      return leasedResult.recordset.map((row) => rowToRun(row as Record<string, unknown>));
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  }

  public async markSucceeded(runId: string, workerName: string): Promise<boolean> {
    const result = await this.pool.request()
      .input("runId", sql.UniqueIdentifier, runId)
      .input("workerName", sql.NVarChar(256), workerName)
      .query(`
        update ${q(this.tables.runs)}
        set status = 'succeeded', completed_at_utc = sysutcdatetime(), lease_owner = null, lease_until_utc = null, error_message = null
        where id = @runId and status = 'leased' and lease_owner = @workerName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async cancelRun(runId: string): Promise<boolean> {
    const result = await this.pool.request()
      .input("runId", sql.UniqueIdentifier, runId)
      .query(`
        update ${q(this.tables.runs)}
        set status = 'failed', completed_at_utc = sysutcdatetime(), error_message = 'Run cancelled', lease_owner = null, lease_until_utc = null
        where id = @runId and status in ('pending', 'leased')
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async markFailed(
    runId: string,
    workerName: string,
    errorMessage: string,
    retry: boolean,
    retryAtUtc: string | undefined
  ): Promise<boolean> {
    if (retry && retryAtUtc) {
      const retried = await this.pool.request()
        .input("runId", sql.UniqueIdentifier, runId)
        .input("workerName", sql.NVarChar(256), workerName)
        .input("retryAtUtc", sql.DateTime2(3), new Date(retryAtUtc))
        .input("errorMessage", sql.NVarChar(sql.MAX), errorMessage)
        .query(`
          update ${q(this.tables.runs)}
          set status = 'pending', scheduled_for_utc = @retryAtUtc, lease_owner = null, lease_until_utc = null, completed_at_utc = null, error_message = @errorMessage
          where id = @runId and status = 'leased' and lease_owner = @workerName
        `);
      return (retried.rowsAffected[0] ?? 0) > 0;
    }

    const failed = await this.pool.request()
      .input("runId", sql.UniqueIdentifier, runId)
      .input("workerName", sql.NVarChar(256), workerName)
      .input("errorMessage", sql.NVarChar(sql.MAX), errorMessage)
      .query(`
        update ${q(this.tables.runs)}
        set status = 'failed', completed_at_utc = sysutcdatetime(), lease_owner = null, lease_until_utc = null, error_message = @errorMessage
        where id = @runId and status = 'leased' and lease_owner = @workerName
      `);
    return (failed.rowsAffected[0] ?? 0) > 0;
  }

  public async getRun(runId: string): Promise<JobRunRecord | undefined> {
    const result = await this.pool.request()
      .input("runId", sql.UniqueIdentifier, runId)
      .query(`select * from ${q(this.tables.runs)} where id = @runId`);
    const row = result.recordset[0];
    return row ? rowToRun(row as Record<string, unknown>) : undefined;
  }

  public async getRecentRuns(take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.request()
      .input("take", sql.Int, Math.max(1, Math.floor(take)))
      .query(`select top (@take) * from ${q(this.tables.runs)} order by scheduled_for_utc desc`);
    return result.recordset.map((row) => rowToRun(row as Record<string, unknown>));
  }

  public async getRuns(): Promise<JobRunRecord[]> {
    const result = await this.pool.request().query(`select * from ${q(this.tables.runs)} order by scheduled_for_utc desc`);
    return result.recordset.map((row) => rowToRun(row as Record<string, unknown>));
  }

  public async getRunsByJobName(jobName: string, take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.request()
      .input("jobName", sql.NVarChar(256), jobName)
      .input("take", sql.Int, Math.max(1, Math.floor(take)))
      .query(`select top (@take) * from ${q(this.tables.runs)} where job_name = @jobName order by scheduled_for_utc desc`);
    return result.recordset.map((row) => rowToRun(row as Record<string, unknown>));
  }

  public async getRunsByStatus(status: RunStatus, take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.request()
      .input("status", sql.NVarChar(32), status)
      .input("take", sql.Int, Math.max(1, Math.floor(take)))
      .query(`select top (@take) * from ${q(this.tables.runs)} where status = @status order by scheduled_for_utc desc`);
    return result.recordset.map((row) => rowToRun(row as Record<string, unknown>));
  }

  public async getEnqueuedRuns(take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.request()
      .input("take", sql.Int, Math.max(1, Math.floor(take)))
      .query(`
        select top (@take) *
        from ${q(this.tables.runs)}
        where status = 'pending' and schedule_slot_utc is null
        order by scheduled_for_utc desc
      `);
    return result.recordset.map((row) => rowToRun(row as Record<string, unknown>));
  }

  public async tryEnqueueIfNoActiveRun(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string | undefined> {
    const id = randomUUID();
    const result = await this.pool.request()
      .input("id", sql.UniqueIdentifier, id)
      .input("jobName", sql.NVarChar(256), jobName)
      .input("jobType", sql.NVarChar(256), jobType)
      .input("scheduledForUtc", sql.DateTime2(3), new Date(scheduledForUtc))
      .input("maxAttempts", sql.Int, Math.max(1, Math.floor(maxAttempts)))
      .input("payloadJson", sql.NVarChar(sql.MAX), payloadJson ?? null)
      .query(`
        insert into ${q(this.tables.runs)}
        (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
        select @id, @jobName, @jobType, 'pending', @scheduledForUtc, null, null, null, 0, @maxAttempts, null, null, @payloadJson, null, sysutcdatetime()
        where not exists (
          select 1 from ${q(this.tables.runs)}
          where job_name = @jobName and status in ('pending', 'leased')
        )
      `);
    return (result.rowsAffected[0] ?? 0) > 0 ? id : undefined;
  }

  public async getRecurringJobs(includeDisabled: boolean): Promise<RecurringJobState[]> {
    const result = includeDisabled
      ? await this.pool.request().query(`select * from ${q(this.tables.jobs)} order by job_name asc`)
      : await this.pool.request().query(`select * from ${q(this.tables.jobs)} where enabled = 1 order by job_name asc`);
    return result.recordset.map((row) => rowToRecurring(row as Record<string, unknown>));
  }

  public async setRecurringJobEnabled(
    jobName: string,
    enabled: boolean,
    nextRunAtUtc: string | undefined
  ): Promise<boolean> {
    const result = await this.pool.request()
      .input("jobName", sql.NVarChar(256), jobName)
      .input("enabled", sql.Bit, enabled)
      .input("nextRunAtUtc", sql.DateTime2(3), nextRunAtUtc ? new Date(nextRunAtUtc) : null)
      .query(`
        update ${q(this.tables.jobs)}
        set enabled = @enabled,
            next_run_at_utc = coalesce(@nextRunAtUtc, next_run_at_utc),
            updated_at_utc = sysutcdatetime()
        where job_name = @jobName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async updateRecurringJobSchedule(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const result = await this.pool.request()
      .input("jobName", sql.NVarChar(256), jobName)
      .input("cronExpression", sql.NVarChar(256), cronExpression)
      .input("timeZone", sql.NVarChar(128), timeZone)
      .input("nextRunAtUtc", sql.DateTime2(3), new Date(nextRunAtUtc))
      .query(`
        update ${q(this.tables.jobs)}
        set cron_expression = @cronExpression,
            time_zone = @timeZone,
            next_run_at_utc = @nextRunAtUtc,
            updated_at_utc = sysutcdatetime()
        where job_name = @jobName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async pruneHistoricalRuns(completedBeforeUtc: string, batchSize: number): Promise<number> {
    const result = await this.pool.request()
      .input("completedBeforeUtc", sql.DateTime2(3), new Date(completedBeforeUtc))
      .input("batchSize", sql.Int, Math.max(1, Math.floor(batchSize)))
      .query(`
        ;with doomed as (
          select top (@batchSize) id
          from ${q(this.tables.runs)}
          where status in ('succeeded', 'failed')
            and completed_at_utc is not null
            and completed_at_utc < @completedBeforeUtc
          order by completed_at_utc asc
        )
        delete from ${q(this.tables.runs)}
        where id in (select id from doomed)
      `);
    return result.rowsAffected[0] ?? 0;
  }
  public async upsertRecurringJob(registration: DurableJobRegistration, nextRunAtUtc: string): Promise<void> {
    if (!registration.recurring) {
      return;
    }

    await this.pool.request()
      .input("jobName", sql.NVarChar(256), registration.jobName)
      .input("jobType", sql.NVarChar(256), registration.jobType)
      .input("cronExpression", sql.NVarChar(256), registration.recurring.cronExpression)
      .input("timeZone", sql.NVarChar(128), registration.recurring.timeZone)
      .input("maxAttempts", sql.Int, Math.max(1, Math.floor(registration.maxAttempts)))
      .input("enabled", sql.Bit, registration.recurring.enabled ?? true)
      .input("allowConcurrentRuns", sql.Bit, registration.recurring.allowConcurrentRuns ?? false)
      .input("retryBehavior", sql.NVarChar(32), registration.recurring.retryBehavior ?? null)
      .input("retryInitialDelaySeconds", sql.Int, registration.recurring.retryInitialDelaySeconds ?? null)
      .input("nextRunAtUtc", sql.DateTime2(3), new Date(nextRunAtUtc))
      .query(`
        merge ${q(this.tables.jobs)} as target
        using (select @jobName as job_name) as source
        on target.job_name = source.job_name
        when matched then
          update set
            job_type = @jobType,
            cron_expression = @cronExpression,
            time_zone = @timeZone,
            max_attempts = @maxAttempts,
            enabled = @enabled,
            allow_concurrent_runs = @allowConcurrentRuns,
            retry_behavior = @retryBehavior,
            retry_initial_delay_seconds = @retryInitialDelaySeconds,
            next_run_at_utc = @nextRunAtUtc,
            updated_at_utc = sysutcdatetime()
        when not matched then
          insert (job_name, job_type, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs, retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
          values (@jobName, @jobType, @cronExpression, @timeZone, @maxAttempts, @enabled, @allowConcurrentRuns, @retryBehavior, @retryInitialDelaySeconds, @nextRunAtUtc, sysutcdatetime());
      `);
  }

  public async getDueRecurringJobs(nowUtc: string, batchSize: number): Promise<RecurringJobState[]> {
    const result = await this.pool.request()
      .input("nowUtc", sql.DateTime2(3), new Date(nowUtc))
      .input("batchSize", sql.Int, Math.max(1, Math.floor(batchSize)))
      .query(`
        select top (@batchSize) *
        from ${q(this.tables.jobs)}
        where enabled = 1
          and next_run_at_utc <= @nowUtc
        order by next_run_at_utc asc
      `);
    return result.recordset.map((row) => rowToRecurring(row as Record<string, unknown>));
  }

  public async updateRecurringNextRun(jobName: string, nextRunAtUtc: string): Promise<void> {
    await this.pool.request()
      .input("jobName", sql.NVarChar(256), jobName)
      .input("nextRunAtUtc", sql.DateTime2(3), new Date(nextRunAtUtc))
      .query(`
        update ${q(this.tables.jobs)}
        set next_run_at_utc = @nextRunAtUtc,
            updated_at_utc = sysutcdatetime()
        where job_name = @jobName
      `);
  }

  public async tryMaterializeRecurringRun(
    recurring: RecurringJobState,
    registration: DurableJobRegistration,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const tx = this.pool.transaction();
    await tx.begin();
    try {
      if (!recurring.allowConcurrentRuns) {
        const active = await tx.request()
          .input("jobName", sql.NVarChar(256), recurring.jobName)
          .query(`
            select top 1 1
            from ${q(this.tables.runs)}
            where job_name = @jobName
              and status in ('pending', 'leased')
          `);
        if ((active.recordset?.length ?? 0) > 0) {
          await tx.rollback();
          return false;
        }
      }

      const lockRow = await tx.request()
        .input("jobName", sql.NVarChar(256), recurring.jobName)
        .query(`
          select next_run_at_utc
          from ${q(this.tables.jobs)} with (updlock, rowlock)
          where job_name = @jobName
            and enabled = 1
        `);

      if ((lockRow.recordset?.length ?? 0) === 0) {
        await tx.rollback();
        return false;
      }

      const currentNext = Date.parse(toDate((lockRow.recordset[0] as { next_run_at_utc: unknown }).next_run_at_utc).toISOString());
      const expectedNext = Date.parse(recurring.nextRunAtUtc);
      if (Math.abs(currentNext - expectedNext) > 1) {
        await tx.rollback();
        return false;
      }

      await tx.request()
        .input("jobName", sql.NVarChar(256), recurring.jobName)
        .input("nextRunAtUtc", sql.DateTime2(3), new Date(nextRunAtUtc))
        .query(`
          update ${q(this.tables.jobs)}
          set next_run_at_utc = @nextRunAtUtc,
              updated_at_utc = sysutcdatetime()
          where job_name = @jobName
        `);

      await tx.request()
        .input("id", sql.UniqueIdentifier, randomUUID())
        .input("jobName", sql.NVarChar(256), recurring.jobName)
        .input("jobType", sql.NVarChar(256), registration.jobType)
        .input("scheduledForUtc", sql.DateTime2(3), new Date(recurring.nextRunAtUtc))
        .input("scheduleSlotUtc", sql.DateTime2(3), new Date(recurring.nextRunAtUtc))
        .input("maxAttempts", sql.Int, recurring.maxAttempts)
        .query(`
          insert into ${q(this.tables.runs)}
          (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
          values (@id, @jobName, @jobType, 'pending', @scheduledForUtc, @scheduleSlotUtc, null, null, 0, @maxAttempts, null, null, null, null, sysutcdatetime())
        `);

      await tx.commit();
      return true;
    } catch (error) {
      await tx.rollback();
      const number = Number((error as { number?: number }).number ?? 0);
      if (number === 2601 || number === 2627 || number === 1205) {
        return false;
      }
      throw error;
    }
  }

  public async extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const result = await this.pool.request()
      .input("runId", sql.UniqueIdentifier, runId)
      .input("workerName", sql.NVarChar(256), workerName)
      .input("leaseSeconds", sql.Int, Math.max(1, Math.floor(leaseDurationSeconds)))
      .query(`
        update ${q(this.tables.runs)}
        set lease_until_utc = dateadd(second, @leaseSeconds, sysutcdatetime())
        where id = @runId
          and status = 'leased'
          and lease_owner = @workerName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async tryLeaseRuntimeCommandReceipt(
    commandId: string,
    workerName: string,
    leaseDurationSeconds: number
  ): Promise<boolean> {
    const tx = this.pool.transaction();
    await tx.begin();
    try {
      const existing = await tx.request()
        .input("commandId", sql.NVarChar(256), commandId)
        .query(`
          select status, lease_owner,
                 case when lease_until_utc is null or lease_until_utc <= sysutcdatetime() then 1 else 0 end as lease_expired
          from ${q(this.tables.runtimeCommandReceipts)} with (updlock, rowlock)
          where command_id = @commandId
        `);

      if ((existing.recordset?.length ?? 0) === 0) {
        await tx.request()
          .input("commandId", sql.NVarChar(256), commandId)
          .input("status", sql.NVarChar(32), RUNTIME_COMMAND_RECEIPT_STATUS.LEASED)
          .input("workerName", sql.NVarChar(256), workerName)
          .input("leaseSeconds", sql.Int, Math.max(1, Math.floor(leaseDurationSeconds)))
          .query(`
            insert into ${q(this.tables.runtimeCommandReceipts)}
            (command_id, status, error_code, error_message, run_id, recorded_at_utc, completed_at_utc, uploaded_at_utc, lease_owner, lease_until_utc)
            values (@commandId, @status, null, null, null, sysutcdatetime(), null, null, @workerName, dateadd(second, @leaseSeconds, sysutcdatetime()))
          `);
        await tx.commit();
        return true;
      }

      const row = existing.recordset[0] as { status?: unknown; lease_owner?: unknown; lease_expired?: unknown };
      const status = String(row.status ?? "");
      const terminal =
        status === RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED ||
        status === RUNTIME_COMMAND_RECEIPT_STATUS.FAILED;
      const owner = row.lease_owner ? String(row.lease_owner) : undefined;
      const expired = Number(row.lease_expired ?? 0) === 1;
      const sameOwner = owner === workerName;

      if (terminal || (!expired && !sameOwner)) {
        await tx.rollback();
        return false;
      }

      await tx.request()
        .input("commandId", sql.NVarChar(256), commandId)
        .input("status", sql.NVarChar(32), RUNTIME_COMMAND_RECEIPT_STATUS.LEASED)
        .input("workerName", sql.NVarChar(256), workerName)
        .input("leaseSeconds", sql.Int, Math.max(1, Math.floor(leaseDurationSeconds)))
        .query(`
          update ${q(this.tables.runtimeCommandReceipts)}
          set status = @status,
              recorded_at_utc = sysutcdatetime(),
              lease_owner = @workerName,
              lease_until_utc = dateadd(second, @leaseSeconds, sysutcdatetime())
          where command_id = @commandId
        `);

      await tx.commit();
      return true;
    } catch (error) {
      await tx.rollback();
      const number = Number((error as { number?: number }).number ?? 0);
      if (number === 1205 || number === 2601 || number === 2627) {
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
    const result = await this.pool.request()
      .input("commandId", sql.NVarChar(256), commandId)
      .input("workerName", sql.NVarChar(256), workerName)
      .input("status", sql.NVarChar(32), RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED)
      .input("recordedAtUtc", sql.DateTime2(3), new Date(recordedAtUtc))
      .query(`
        update ${q(this.tables.runtimeCommandReceipts)}
        set status = @status,
            recorded_at_utc = @recordedAtUtc
        where command_id = @commandId and lease_owner = @workerName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async markRuntimeCommandSucceeded(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    runId: string | undefined
  ): Promise<boolean> {
    const result = await this.pool.request()
      .input("commandId", sql.NVarChar(256), commandId)
      .input("workerName", sql.NVarChar(256), workerName)
      .input("status", sql.NVarChar(32), RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED)
      .input("recordedAtUtc", sql.DateTime2(3), new Date(recordedAtUtc))
      .input("completedAtUtc", sql.DateTime2(3), new Date(completedAtUtc))
      .input("runId", sql.UniqueIdentifier, runId ?? null)
      .query(`
        update ${q(this.tables.runtimeCommandReceipts)}
        set status = @status,
            recorded_at_utc = @recordedAtUtc,
            completed_at_utc = @completedAtUtc,
            run_id = @runId,
            lease_owner = null,
            lease_until_utc = null
        where command_id = @commandId and lease_owner = @workerName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async markRuntimeCommandFailed(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    errorCode: string | undefined,
    errorMessage: string | undefined
  ): Promise<boolean> {
    const result = await this.pool.request()
      .input("commandId", sql.NVarChar(256), commandId)
      .input("workerName", sql.NVarChar(256), workerName)
      .input("status", sql.NVarChar(32), RUNTIME_COMMAND_RECEIPT_STATUS.FAILED)
      .input("recordedAtUtc", sql.DateTime2(3), new Date(recordedAtUtc))
      .input("completedAtUtc", sql.DateTime2(3), new Date(completedAtUtc))
      .input("errorCode", sql.NVarChar(128), errorCode ?? null)
      .input("errorMessage", sql.NVarChar(sql.MAX), errorMessage ?? null)
      .query(`
        update ${q(this.tables.runtimeCommandReceipts)}
        set status = @status,
            recorded_at_utc = @recordedAtUtc,
            completed_at_utc = @completedAtUtc,
            error_code = @errorCode,
            error_message = @errorMessage,
            lease_owner = null,
            lease_until_utc = null
        where command_id = @commandId and lease_owner = @workerName
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }

  public async getRuntimeCommandReceipts(take: number): Promise<RuntimeCommandReceiptRecord[]> {
    const result = await this.pool.request()
      .input("take", sql.Int, Math.max(1, Math.floor(take)))
      .query(`
        select top (@take) *
        from ${q(this.tables.runtimeCommandReceipts)}
        where uploaded_at_utc is null
          and status in ('${RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED}', '${RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED}', '${RUNTIME_COMMAND_RECEIPT_STATUS.FAILED}')
        order by recorded_at_utc asc
      `);
    return result.recordset.map((row) => rowToReceipt(row as Record<string, unknown>));
  }

  public async markRuntimeCommandReceiptUploaded(commandId: string): Promise<boolean> {
    const result = await this.pool.request()
      .input("commandId", sql.NVarChar(256), commandId)
      .query(`
        update ${q(this.tables.runtimeCommandReceipts)}
        set uploaded_at_utc = sysutcdatetime()
        where command_id = @commandId
      `);
    return (result.rowsAffected[0] ?? 0) > 0;
  }
}
