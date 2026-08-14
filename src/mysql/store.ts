import type { Pool } from "mysql2/promise";
import {
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
import { createMySqlPool } from "./migrator.js";
import type { MySqlDurableStackOptions } from "./types.js";
import { generateId } from "../utils.js";
import { resolveMySqlTableNames } from "./table-names.js";

function q(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``;
}

function toIsoUtc(value: unknown): string | undefined {
  if (value === null || typeof value === "undefined") {
    return undefined;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  const raw = String(value).trim();
  if (!raw) {
    return undefined;
  }

  const normalized = raw.includes("T") ? raw : raw.replace(" ", "T");
  const hasOffset = /z$|[+-]\d\d:?\d\d$/i.test(normalized);
  return new Date(hasOffset ? normalized : `${normalized}Z`).toISOString();
}

function rowToRun(row: Record<string, unknown>): JobRunRecord {
  const payload = row.payload_json;
  return {
    id: String(row.id),
    jobName: String(row.job_name),
    jobType: String(row.job_type),
    status: String(row.status) as RunStatus,
    scheduledForUtc: toIsoUtc(row.scheduled_for_utc)!,
    scheduleSlotUtc: toIsoUtc(row.schedule_slot_utc),
    startedAtUtc: toIsoUtc(row.started_at_utc),
    completedAtUtc: toIsoUtc(row.completed_at_utc),
    attempt: Number(row.attempt),
    maxAttempts: Number(row.max_attempts),
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: toIsoUtc(row.lease_until_utc),
    // mysql2 auto-parses the json column into a native JS value (including for a
    // stored string payload, which comes back unwrapped rather than as JSON text),
    // so this must always re-stringify rather than pass a string through as-is —
    // and a truthy check would incorrectly treat `false`, `0`, or `""` as absent.
    payloadJson: (payload === null || typeof payload === "undefined") ? undefined : JSON.stringify(payload),
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
    enabled: Boolean(Number(row.enabled)),
    allowConcurrentRuns: Boolean(Number(row.allow_concurrent_runs)),
    retryBehavior: row.retry_behavior ? String(row.retry_behavior) as "fixed" | "backoff" : undefined,
    retryInitialDelaySeconds: row.retry_initial_delay_seconds ? Number(row.retry_initial_delay_seconds) : undefined,
    nextRunAtUtc: toIsoUtc(row.next_run_at_utc)!
  };
}

function rowToReceipt(row: Record<string, unknown>): RuntimeCommandReceiptRecord {
  return {
    commandId: String(row.command_id),
    status: String(row.status) as RuntimeCommandReceiptRecord["status"],
    errorCode: row.error_code ? String(row.error_code) : undefined,
    errorMessage: row.error_message ? String(row.error_message) : undefined,
    runId: row.run_id ? String(row.run_id) : undefined,
    recordedAtUtc: toIsoUtc(row.recorded_at_utc)!,
    completedAtUtc: toIsoUtc(row.completed_at_utc),
    uploadedAtUtc: toIsoUtc(row.uploaded_at_utc),
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: toIsoUtc(row.lease_until_utc)
  };
}

function affectedRows(result: unknown): number {
  return Number((result as { affectedRows?: number }).affectedRows ?? 0);
}

function toMySqlDateTime(isoUtc: string): string {
  return isoUtc.replace("T", " ").replace("Z", "");
}

export class MySqlDurableJobStore implements DurableJobStore {
  private readonly pool: Pool;
  private readonly tables: ReturnType<typeof resolveMySqlTableNames>;

  public constructor(options: MySqlDurableStackOptions) {
    this.pool = createMySqlPool(options.connectionString);
    this.tables = resolveMySqlTableNames(options.databaseTablePrefix);
  }

  public async close(): Promise<void> {
    await this.pool.end();
  }

  public getPool(): Pool {
    return this.pool;
  }

  public async enqueue(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string> {
    const id = generateId();
    const sql = `
      insert into ${q(this.tables.runs)}
      (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
      values (?, ?, ?, 'pending', ?, null, null, null, 0, ?, null, null, ?, null, utc_timestamp(3));
    `;
    await this.pool.query(sql, [
      id,
      jobName,
      jobType,
      toMySqlDateTime(scheduledForUtc),
      Math.max(1, Math.floor(maxAttempts)),
      payloadJson ?? null
    ]);
    return id;
  }

  public async claimDueRuns(
    workerName: string,
    batchSize: number,
    leaseDurationSeconds: number
  ): Promise<JobRunRecord[]> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();

      const [candidateRows] = await conn.query(
        `
          select id
          from ${q(this.tables.runs)}
          where
            (status = 'pending' and scheduled_for_utc <= utc_timestamp(3))
            or
            (status = 'leased' and (lease_until_utc is null or lease_until_utc <= utc_timestamp(3)))
          order by scheduled_for_utc asc
          limit ?
          for update skip locked
        `,
        [Math.max(1, Math.floor(batchSize))]
      );

      const ids = (candidateRows as Array<{ id: string }>).map((x) => x.id);
      if (ids.length === 0) {
        await conn.rollback();
        return [];
      }

      const idParams = ids.map(() => "?").join(", ");
      await conn.query(
        `
          update ${q(this.tables.runs)}
          set
            attempt = attempt + 1,
            status = if((attempt + 1) > max_attempts, 'failed', 'leased'),
            started_at_utc = ifnull(started_at_utc, utc_timestamp(3)),
            completed_at_utc = if((attempt + 1) > max_attempts, utc_timestamp(3), completed_at_utc),
            error_message = if((attempt + 1) > max_attempts, ifnull(error_message, 'Run exceeded max attempts before claim'), error_message),
            lease_owner = if((attempt + 1) > max_attempts, null, ?),
            lease_until_utc = if((attempt + 1) > max_attempts, null, date_add(utc_timestamp(3), interval ? second))
          where id in (${idParams})
        `,
        [workerName, Math.max(1, Math.floor(leaseDurationSeconds)), ...ids]
      );

      const [rows] = await conn.query(
        `
          select *
          from ${q(this.tables.runs)}
          where status = 'leased'
            and id in (${idParams})
          order by scheduled_for_utc asc
        `,
        ids
      );

      await conn.commit();
      return (rows as Array<Record<string, unknown>>).map((row) => rowToRun(row));
    } catch (error) {
      await conn.rollback();
      throw error;
    } finally {
      conn.release();
    }
  }

  public async markSucceeded(runId: string, workerName: string): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runs)}
        set status = 'succeeded', completed_at_utc = utc_timestamp(3), lease_owner = null, lease_until_utc = null, error_message = null
        where id = ? and status = 'leased' and lease_owner = ?
      `,
      [runId, workerName]
    );
    return affectedRows(result) > 0;
  }

  public async cancelRun(runId: string): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runs)}
        set status = 'failed', completed_at_utc = utc_timestamp(3), error_message = 'Run cancelled', lease_owner = null, lease_until_utc = null
        where id = ? and status in ('pending', 'leased')
      `,
      [runId]
    );
    return affectedRows(result) > 0;
  }

  public async markFailed(
    runId: string,
    workerName: string,
    errorMessage: string,
    retry: boolean,
    retryAtUtc: string | undefined
  ): Promise<boolean> {
    if (retry && retryAtUtc) {
      const [result] = await this.pool.query(
        `
          update ${q(this.tables.runs)}
          set status = 'pending',
              scheduled_for_utc = ?,
              lease_owner = null,
              lease_until_utc = null,
              completed_at_utc = null,
              error_message = ?
          where id = ? and status = 'leased' and lease_owner = ?
        `,
        [toMySqlDateTime(retryAtUtc), errorMessage, runId, workerName]
      );
      return affectedRows(result) > 0;
    }

    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runs)}
        set status = 'failed', completed_at_utc = utc_timestamp(3), lease_owner = null, lease_until_utc = null, error_message = ?
        where id = ? and status = 'leased' and lease_owner = ?
      `,
      [errorMessage, runId, workerName]
    );
    return affectedRows(result) > 0;
  }

  public async getRun(runId: string): Promise<JobRunRecord | undefined> {
    const [rows] = await this.pool.query(`select * from ${q(this.tables.runs)} where id = ?`, [runId]);
    const row = (rows as Array<Record<string, unknown>>)[0];
    return row ? rowToRun(row) : undefined;
  }

  public async getRecentRuns(take: number): Promise<JobRunRecord[]> {
    const [rows] = await this.pool.query(
      `select * from ${q(this.tables.runs)} order by scheduled_for_utc desc limit ?`,
      [Math.max(1, Math.floor(take))]
    );
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRun(row));
  }

  public async getRuns(): Promise<JobRunRecord[]> {
    const [rows] = await this.pool.query(`select * from ${q(this.tables.runs)} order by scheduled_for_utc desc`);
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRun(row));
  }

  public async getRunsByJobName(jobName: string, take: number): Promise<JobRunRecord[]> {
    const [rows] = await this.pool.query(
      `select * from ${q(this.tables.runs)} where job_name = ? order by scheduled_for_utc desc limit ?`,
      [jobName, Math.max(1, Math.floor(take))]
    );
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRun(row));
  }

  public async getRunsByStatus(status: RunStatus, take: number): Promise<JobRunRecord[]> {
    const [rows] = await this.pool.query(
      `select * from ${q(this.tables.runs)} where status = ? order by scheduled_for_utc desc limit ?`,
      [status, Math.max(1, Math.floor(take))]
    );
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRun(row));
  }

  public async getEnqueuedRuns(take: number): Promise<JobRunRecord[]> {
    const [rows] = await this.pool.query(
      `select * from ${q(this.tables.runs)} where status = 'pending' and schedule_slot_utc is null order by scheduled_for_utc desc limit ?`,
      [Math.max(1, Math.floor(take))]
    );
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRun(row));
  }

  public async tryEnqueueIfNoActiveRun(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string | undefined> {
    const id = generateId();
    const [result] = await this.pool.query(
      `
        insert into ${q(this.tables.runs)}
        (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
        select ?, ?, ?, 'pending', ?, null, null, null, 0, ?, null, null, ?, null, utc_timestamp(3)
        from dual
        where not exists (
          select 1 from ${q(this.tables.runs)}
          where job_name = ? and status in ('pending', 'leased')
        )
      `,
      [id, jobName, jobType, toMySqlDateTime(scheduledForUtc), Math.max(1, Math.floor(maxAttempts)), payloadJson ?? null, jobName]
    );
    return affectedRows(result) > 0 ? id : undefined;
  }

  public async getRecurringJobs(includeDisabled: boolean): Promise<RecurringJobState[]> {
    const sql = includeDisabled
      ? `select * from ${q(this.tables.jobs)} order by job_name asc`
      : `select * from ${q(this.tables.jobs)} where enabled = 1 order by job_name asc`;
    const [rows] = await this.pool.query(sql);
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRecurring(row));
  }

  public async setRecurringJobEnabled(
    jobName: string,
    enabled: boolean,
    nextRunAtUtc: string | undefined
  ): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.jobs)}
        set enabled = ?,
            next_run_at_utc = coalesce(?, next_run_at_utc),
            updated_at_utc = utc_timestamp(3)
        where job_name = ?
      `,
      [enabled ? 1 : 0, nextRunAtUtc ? toMySqlDateTime(nextRunAtUtc) : null, jobName]
    );
    return affectedRows(result) > 0;
  }

  public async updateRecurringJobSchedule(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.jobs)}
        set cron_expression = ?,
            time_zone = ?,
            next_run_at_utc = ?,
            updated_at_utc = utc_timestamp(3)
        where job_name = ?
      `,
      [cronExpression, timeZone, toMySqlDateTime(nextRunAtUtc), jobName]
    );
    return affectedRows(result) > 0;
  }

  public async pruneHistoricalRuns(completedBeforeUtc: string, batchSize: number): Promise<number> {
    const [result] = await this.pool.query(
      `
        delete from ${q(this.tables.runs)}
        where status in ('succeeded', 'failed')
          and completed_at_utc is not null
          and completed_at_utc < ?
        order by completed_at_utc asc
        limit ?
      `,
      [toMySqlDateTime(completedBeforeUtc), Math.max(1, Math.floor(batchSize))]
    );

    return affectedRows(result);
  }
  public async upsertRecurringJob(registration: DurableJobRegistration, nextRunAtUtc: string): Promise<void> {
    if (!registration.recurring) {
      return;
    }

    await this.pool.query(
      `
        insert into ${q(this.tables.jobs)}
        (job_name, job_type, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs, retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, utc_timestamp(3))
        on duplicate key update
          job_type = values(job_type),
          cron_expression = values(cron_expression),
          time_zone = values(time_zone),
          max_attempts = values(max_attempts),
          enabled = values(enabled),
          allow_concurrent_runs = values(allow_concurrent_runs),
          retry_behavior = values(retry_behavior),
          retry_initial_delay_seconds = values(retry_initial_delay_seconds),
          next_run_at_utc = values(next_run_at_utc),
          updated_at_utc = utc_timestamp(3)
      `,
      [
        registration.jobName,
        registration.jobType,
        registration.recurring.cronExpression,
        registration.recurring.timeZone,
        Math.max(1, Math.floor(registration.maxAttempts)),
        registration.recurring.enabled ?? true,
        registration.recurring.allowConcurrentRuns ?? false,
        registration.recurring.retryBehavior ?? null,
        registration.recurring.retryInitialDelaySeconds ?? null,
        toMySqlDateTime(nextRunAtUtc)
      ]
    );
  }

  public async getDueRecurringJobs(nowUtc: string, batchSize: number): Promise<RecurringJobState[]> {
    const [rows] = await this.pool.query(
      `
        select *
        from ${q(this.tables.jobs)}
        where enabled = 1
          and next_run_at_utc <= ?
        order by next_run_at_utc asc
        limit ?
      `,
      [toMySqlDateTime(nowUtc), Math.max(1, Math.floor(batchSize))]
    );
    return (rows as Array<Record<string, unknown>>).map((row) => rowToRecurring(row));
  }

  public async updateRecurringNextRun(jobName: string, nextRunAtUtc: string): Promise<void> {
    await this.pool.query(
      `
        update ${q(this.tables.jobs)}
        set next_run_at_utc = ?,
            updated_at_utc = utc_timestamp(3)
        where job_name = ?
      `,
      [toMySqlDateTime(nextRunAtUtc), jobName]
    );
  }

  public async tryMaterializeRecurringRun(
    recurring: RecurringJobState,
    registration: DurableJobRegistration,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();

      if (!recurring.allowConcurrentRuns) {
        const [activeRows] = await conn.query(
          `
            select 1
            from ${q(this.tables.runs)}
            where job_name = ?
              and status in ('pending', 'leased')
            limit 1
          `,
          [recurring.jobName]
        );

        if ((activeRows as unknown[]).length > 0) {
          await conn.rollback();
          return false;
        }
      }

      const [lockedRows] = await conn.query(
        `
          select next_run_at_utc
          from ${q(this.tables.jobs)}
          where job_name = ?
            and enabled = 1
          for update
        `,
        [recurring.jobName]
      );

      const locked = lockedRows as Array<{ next_run_at_utc: unknown }>;
      if (locked.length === 0) {
        await conn.rollback();
        return false;
      }

      const currentNextRun = Date.parse(toIsoUtc(locked[0]!.next_run_at_utc)!);
      const expectedNextRun = Date.parse(recurring.nextRunAtUtc);
      if (Math.abs(currentNextRun - expectedNextRun) > 1) {
        await conn.rollback();
        return false;
      }

      await conn.query(
        `
          update ${q(this.tables.jobs)}
          set next_run_at_utc = ?,
              updated_at_utc = utc_timestamp(3)
          where job_name = ?
        `,
        [toMySqlDateTime(nextRunAtUtc), recurring.jobName]
      );

      const runId = generateId();
      await conn.query(
        `
          insert into ${q(this.tables.runs)}
          (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
          values (?, ?, ?, 'pending', ?, ?, null, null, 0, ?, null, null, null, null, utc_timestamp(3))
        `,
        [
          runId,
          recurring.jobName,
          registration.jobType,
          toMySqlDateTime(recurring.nextRunAtUtc),
          toMySqlDateTime(recurring.nextRunAtUtc),
          recurring.maxAttempts
        ]
      );

      await conn.commit();
      return true;
    } catch (error) {
      await conn.rollback();
      const code = String((error as { code?: string; errno?: number }).code ?? "");
      const errno = Number((error as { errno?: number }).errno ?? 0);
      if (code === "ER_DUP_ENTRY" || errno === 1062 || code === "ER_LOCK_DEADLOCK" || errno === 1213 || code === "ER_LOCK_WAIT_TIMEOUT" || errno === 1205) {
        return false;
      }
      throw error;
    } finally {
      conn.release();
    }
  }

  public async extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runs)}
        set lease_until_utc = date_add(utc_timestamp(3), interval ? second)
        where id = ?
          and status = 'leased'
          and lease_owner = ?
      `,
      [Math.max(1, Math.floor(leaseDurationSeconds)), runId, workerName]
    );
    return affectedRows(result) > 0;
  }

  public async tryLeaseRuntimeCommandReceipt(
    commandId: string,
    workerName: string,
    leaseDurationSeconds: number
  ): Promise<boolean> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();

      const [rows] = await conn.query(
        `
          select status, lease_owner,
                 case when lease_until_utc is null or lease_until_utc <= utc_timestamp(3) then 1 else 0 end as lease_expired
          from ${q(this.tables.runtimeCommandReceipts)}
          where command_id = ?
          for update
        `,
        [commandId]
      );

      const existing = rows as Array<{ status: unknown; lease_owner: unknown; lease_expired: unknown }>;
      if (existing.length === 0) {
        await conn.query(
          `
            insert into ${q(this.tables.runtimeCommandReceipts)}
            (command_id, status, error_code, error_message, run_id, recorded_at_utc, completed_at_utc, uploaded_at_utc, lease_owner, lease_until_utc)
            values (?, ?, null, null, null, utc_timestamp(3), null, null, ?, date_add(utc_timestamp(3), interval ? second))
          `,
          [commandId, RUNTIME_COMMAND_RECEIPT_STATUS.LEASED, workerName, Math.max(1, Math.floor(leaseDurationSeconds))]
        );
        await conn.commit();
        return true;
      }

      const status = String(existing[0]!.status ?? "");
      const terminal =
        status === RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED ||
        status === RUNTIME_COMMAND_RECEIPT_STATUS.FAILED;
      const leaseOwner = existing[0]!.lease_owner ? String(existing[0]!.lease_owner) : undefined;
      const expired = Number(existing[0]!.lease_expired ?? 0) === 1;
      const sameOwner = leaseOwner === workerName;

      if (terminal || (!expired && !sameOwner)) {
        await conn.rollback();
        return false;
      }

      await conn.query(
        `
          update ${q(this.tables.runtimeCommandReceipts)}
          set status = ?,
              recorded_at_utc = utc_timestamp(3),
              lease_owner = ?,
              lease_until_utc = date_add(utc_timestamp(3), interval ? second)
          where command_id = ?
        `,
        [RUNTIME_COMMAND_RECEIPT_STATUS.LEASED, workerName, Math.max(1, Math.floor(leaseDurationSeconds)), commandId]
      );
      await conn.commit();
      return true;
    } catch (error) {
      await conn.rollback();
      const code = String((error as { code?: string; errno?: number }).code ?? "");
      const errno = Number((error as { errno?: number }).errno ?? 0);
      if (code === "ER_LOCK_DEADLOCK" || errno === 1213 || code === "ER_LOCK_WAIT_TIMEOUT" || errno === 1205) {
        return false;
      }
      throw error;
    } finally {
      conn.release();
    }
  }

  public async markRuntimeCommandAcknowledged(
    commandId: string,
    workerName: string,
    recordedAtUtc: string
  ): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runtimeCommandReceipts)}
        set status = ?,
            recorded_at_utc = ?
        where command_id = ?
          and lease_owner = ?
      `,
      [RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED, toMySqlDateTime(recordedAtUtc), commandId, workerName]
    );
    return affectedRows(result) > 0;
  }

  public async markRuntimeCommandSucceeded(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    runId: string | undefined
  ): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runtimeCommandReceipts)}
        set status = ?,
            recorded_at_utc = ?,
            completed_at_utc = ?,
            run_id = ?,
            lease_owner = null,
            lease_until_utc = null
        where command_id = ?
          and lease_owner = ?
      `,
      [
        RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED,
        toMySqlDateTime(recordedAtUtc),
        toMySqlDateTime(completedAtUtc),
        runId ?? null,
        commandId,
        workerName
      ]
    );
    return affectedRows(result) > 0;
  }

  public async markRuntimeCommandFailed(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    errorCode: string | undefined,
    errorMessage: string | undefined
  ): Promise<boolean> {
    const [result] = await this.pool.query(
      `
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
      `,
      [
        RUNTIME_COMMAND_RECEIPT_STATUS.FAILED,
        toMySqlDateTime(recordedAtUtc),
        toMySqlDateTime(completedAtUtc),
        errorCode ?? null,
        errorMessage ?? null,
        commandId,
        workerName
      ]
    );
    return affectedRows(result) > 0;
  }

  public async getRuntimeCommandReceipts(take: number): Promise<RuntimeCommandReceiptRecord[]> {
    const [rows] = await this.pool.query(
      `
        select *
        from ${q(this.tables.runtimeCommandReceipts)}
        where uploaded_at_utc is null
          and status in (?, ?, ?)
        order by recorded_at_utc asc
        limit ?
      `,
      [
        RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED,
        RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED,
        RUNTIME_COMMAND_RECEIPT_STATUS.FAILED,
        Math.max(1, Math.floor(take))
      ]
    );
    return (rows as Array<Record<string, unknown>>).map((row) => rowToReceipt(row));
  }

  public async markRuntimeCommandReceiptUploaded(commandId: string): Promise<boolean> {
    const [result] = await this.pool.query(
      `
        update ${q(this.tables.runtimeCommandReceipts)}
        set uploaded_at_utc = utc_timestamp(3)
        where command_id = ?
      `,
      [commandId]
    );
    return affectedRows(result) > 0;
  }
}
