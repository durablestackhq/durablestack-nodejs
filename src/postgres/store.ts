import { Pool } from "pg";
import { RUN_STATUS, RUNTIME_COMMAND_RECEIPT_STATUS, type RunStatus } from "../constants.js";
import type {
  DurableJobRegistration,
  DurableJobStore,
  JobRunRecord,
  RecurringJobState,
  RuntimeCommandReceiptRecord
} from "../types.js";
import { generateId, nowIso } from "../utils.js";
import { resolvePostgresTableNames } from "./table-names.js";
import type { PostgresDurableStackOptions } from "./types.js";

function q(name: string): string {
  return `"${name.replace(/"/g, "\"\"")}"`;
}

function intervalExpr(seconds: number): string {
  return `${Math.max(1, Math.floor(seconds))} seconds`;
}

function rowToRun(row: Record<string, unknown>): JobRunRecord {
  return {
    id: String(row.id),
    jobName: String(row.job_name),
    jobType: String(row.job_type),
    status: String(row.status) as RunStatus,
    scheduledForUtc: new Date(String(row.scheduled_for_utc)).toISOString(),
    scheduleSlotUtc: row.schedule_slot_utc ? new Date(String(row.schedule_slot_utc)).toISOString() : undefined,
    startedAtUtc: row.started_at_utc ? new Date(String(row.started_at_utc)).toISOString() : undefined,
    completedAtUtc: row.completed_at_utc ? new Date(String(row.completed_at_utc)).toISOString() : undefined,
    attempt: Number(row.attempt),
    maxAttempts: Number(row.max_attempts),
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: row.lease_until_utc ? new Date(String(row.lease_until_utc)).toISOString() : undefined,
    payloadJson: row.payload_json ? JSON.stringify(row.payload_json) : undefined,
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
    enabled: Boolean(row.enabled),
    allowConcurrentRuns: Boolean(row.allow_concurrent_runs),
    retryBehavior: row.retry_behavior ? String(row.retry_behavior) as "fixed" | "backoff" : undefined,
    retryInitialDelaySeconds: row.retry_initial_delay_seconds ? Number(row.retry_initial_delay_seconds) : undefined,
    nextRunAtUtc: new Date(String(row.next_run_at_utc)).toISOString()
  };
}

function rowToReceipt(row: Record<string, unknown>): RuntimeCommandReceiptRecord {
  return {
    commandId: String(row.command_id),
    status: String(row.status) as RuntimeCommandReceiptRecord["status"],
    errorCode: row.error_code ? String(row.error_code) : undefined,
    errorMessage: row.error_message ? String(row.error_message) : undefined,
    runId: row.run_id ? String(row.run_id) : undefined,
    recordedAtUtc: new Date(String(row.recorded_at_utc)).toISOString(),
    completedAtUtc: row.completed_at_utc ? new Date(String(row.completed_at_utc)).toISOString() : undefined,
    uploadedAtUtc: row.uploaded_at_utc ? new Date(String(row.uploaded_at_utc)).toISOString() : undefined,
    leaseOwner: row.lease_owner ? String(row.lease_owner) : undefined,
    leaseUntilUtc: row.lease_until_utc ? new Date(String(row.lease_until_utc)).toISOString() : undefined
  };
}

export class PostgresDurableJobStore implements DurableJobStore {
  private readonly pool: Pool;
  private readonly tables: ReturnType<typeof resolvePostgresTableNames>;

  public constructor(options: PostgresDurableStackOptions) {
    this.pool = new Pool({ connectionString: options.connectionString });
    this.tables = resolvePostgresTableNames(options.databaseTablePrefix);
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
      values ($1::uuid, $2, $3, 'pending', $4::timestamptz, null, null, null, 0, $5, null, null, $6::jsonb, null, now())
      returning id::text;
    `;
    const result = await this.pool.query(sql, [id, jobName, jobType, scheduledForUtc, Math.max(1, Math.floor(maxAttempts)), payloadJson ?? null]);
    return String(result.rows[0]?.id);
  }

  public async claimDueRuns(workerName: string, batchSize: number, leaseDurationSeconds: number): Promise<JobRunRecord[]> {
    const sql = `
      with candidates as (
        select id
        from ${q(this.tables.runs)}
        where
          (status = 'pending' and scheduled_for_utc <= now())
          or
          (status = 'leased' and (lease_until_utc is null or lease_until_utc <= now()))
        order by scheduled_for_utc asc
        for update skip locked
        limit $1
      ),
      updated as (
        update ${q(this.tables.runs)} r
        set
          attempt = r.attempt + 1,
          status = case when (r.attempt + 1) > r.max_attempts then 'failed' else 'leased' end,
          started_at_utc = coalesce(r.started_at_utc, now()),
          completed_at_utc = case when (r.attempt + 1) > r.max_attempts then now() else r.completed_at_utc end,
          error_message = case when (r.attempt + 1) > r.max_attempts then coalesce(r.error_message, 'Run exceeded max attempts before claim') else r.error_message end,
          lease_owner = case when (r.attempt + 1) > r.max_attempts then null else $2 end,
          lease_until_utc = case when (r.attempt + 1) > r.max_attempts then null else now() + $3::interval end
        from candidates c
        where r.id = c.id
        returning r.*
      )
      select * from updated where status = 'leased' order by scheduled_for_utc asc;
    `;

    const result = await this.pool.query(sql, [Math.max(1, Math.floor(batchSize)), workerName, intervalExpr(leaseDurationSeconds)]);
    return result.rows.map((row) => rowToRun(row));
  }

  public async markSucceeded(runId: string, workerName: string): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runs)}
      set status = 'succeeded', completed_at_utc = now(), lease_owner = null, lease_until_utc = null, error_message = null
      where id = $1::uuid and status = 'leased' and lease_owner = $2;
    `;
    const result = await this.pool.query(sql, [runId, workerName]);
    return (result.rowCount ?? 0) > 0;
  }

  public async cancelRun(runId: string): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runs)}
      set status = 'failed', completed_at_utc = now(), error_message = 'Run cancelled', lease_owner = null, lease_until_utc = null
      where id = $1::uuid and status in ('pending', 'leased');
    `;
    const result = await this.pool.query(sql, [runId]);
    return (result.rowCount ?? 0) > 0;
  }

  public async markFailed(
    runId: string,
    workerName: string,
    errorMessage: string,
    retry: boolean,
    retryAtUtc: string | undefined
  ): Promise<boolean> {
    const sqlRetry = `
      update ${q(this.tables.runs)}
      set status = 'pending', scheduled_for_utc = $3::timestamptz, lease_owner = null, lease_until_utc = null, error_message = $4
      where id = $1::uuid and status = 'leased' and lease_owner = $2;
    `;
    const sqlFail = `
      update ${q(this.tables.runs)}
      set status = 'failed', completed_at_utc = now(), lease_owner = null, lease_until_utc = null, error_message = $3
      where id = $1::uuid and status = 'leased' and lease_owner = $2;
    `;

    const result = retry && retryAtUtc
      ? await this.pool.query(sqlRetry, [runId, workerName, retryAtUtc, errorMessage])
      : await this.pool.query(sqlFail, [runId, workerName, errorMessage]);

    return (result.rowCount ?? 0) > 0;
  }

  public async getRun(runId: string): Promise<JobRunRecord | undefined> {
    const result = await this.pool.query(`select * from ${q(this.tables.runs)} where id = $1::uuid`, [runId]);
    const row = result.rows[0];
    return row ? rowToRun(row) : undefined;
  }

  public async getRecentRuns(take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.query(
      `select * from ${q(this.tables.runs)} order by scheduled_for_utc desc limit $1`,
      [Math.max(1, Math.floor(take))]
    );
    return result.rows.map((row) => rowToRun(row));
  }

  public async getRuns(): Promise<JobRunRecord[]> {
    const result = await this.pool.query(`select * from ${q(this.tables.runs)} order by scheduled_for_utc desc`);
    return result.rows.map((row) => rowToRun(row));
  }

  public async getRunsByJobName(jobName: string, take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.query(
      `select * from ${q(this.tables.runs)} where job_name = $1 order by scheduled_for_utc desc limit $2`,
      [jobName, Math.max(1, Math.floor(take))]
    );
    return result.rows.map((row) => rowToRun(row));
  }

  public async getRunsByStatus(status: RunStatus, take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.query(
      `select * from ${q(this.tables.runs)} where status = $1 order by scheduled_for_utc desc limit $2`,
      [status, Math.max(1, Math.floor(take))]
    );
    return result.rows.map((row) => rowToRun(row));
  }

  public async getEnqueuedRuns(take: number): Promise<JobRunRecord[]> {
    const result = await this.pool.query(
      `select * from ${q(this.tables.runs)} where status = 'pending' and schedule_slot_utc is null order by scheduled_for_utc desc limit $1`,
      [Math.max(1, Math.floor(take))]
    );
    return result.rows.map((row) => rowToRun(row));
  }

  public async tryEnqueueIfNoActiveRun(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string | undefined> {
    const id = generateId();
    const sql = `
      with can_enqueue as (
        select 1
        where not exists (
          select 1 from ${q(this.tables.runs)}
          where job_name = $1
            and status in ('pending', 'leased')
        )
      )
      insert into ${q(this.tables.runs)}
      (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
      select $2::uuid, $1, $3, 'pending', $4::timestamptz, null, null, null, 0, $5, null, null, $6::jsonb, null, now()
      from can_enqueue
      returning id::text;
    `;

    const result = await this.pool.query(sql, [jobName, id, jobType, scheduledForUtc, Math.max(1, Math.floor(maxAttempts)), payloadJson ?? null]);
    const row = result.rows[0];
    return row ? String(row.id) : undefined;
  }

  public async getRecurringJobs(includeDisabled: boolean): Promise<RecurringJobState[]> {
    const sql = includeDisabled
      ? `select * from ${q(this.tables.jobs)} order by job_name asc`
      : `select * from ${q(this.tables.jobs)} where enabled = true order by job_name asc`;
    const result = await this.pool.query(sql);
    return result.rows.map((row) => rowToRecurring(row));
  }

  public async setRecurringJobEnabled(jobName: string, enabled: boolean, nextRunAtUtc: string | undefined): Promise<boolean> {
    const sql = `
      update ${q(this.tables.jobs)}
      set enabled = $2,
          next_run_at_utc = coalesce($3::timestamptz, next_run_at_utc),
          updated_at_utc = now()
      where job_name = $1;
    `;
    const result = await this.pool.query(sql, [jobName, enabled, nextRunAtUtc ?? null]);
    return (result.rowCount ?? 0) > 0;
  }

  public async updateRecurringJobSchedule(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const sql = `
      update ${q(this.tables.jobs)}
      set cron_expression = $2,
          time_zone = $3,
          next_run_at_utc = $4::timestamptz,
          updated_at_utc = now()
      where job_name = $1;
    `;
    const result = await this.pool.query(sql, [jobName, cronExpression, timeZone, nextRunAtUtc]);
    return (result.rowCount ?? 0) > 0;
  }

  public async pruneHistoricalRuns(completedBeforeUtc: string, batchSize: number): Promise<number> {
    const sql = `
      with doomed as (
        select id
        from ${q(this.tables.runs)}
        where status in ('succeeded', 'failed')
          and completed_at_utc is not null
          and completed_at_utc < $1::timestamptz
        order by completed_at_utc asc
        limit $2
      )
      delete from ${q(this.tables.runs)} r
      using doomed d
      where r.id = d.id;
    `;

    const result = await this.pool.query(sql, [completedBeforeUtc, Math.max(1, Math.floor(batchSize))]);
    return result.rowCount ?? 0;
  }

  public async upsertRecurringJob(registration: DurableJobRegistration, nextRunAtUtc: string): Promise<void> {
    if (!registration.recurring) {
      return;
    }

    const sql = `
      insert into ${q(this.tables.jobs)}
      (job_name, job_type, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs, retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz, now())
      on conflict (job_name)
      do update set
        job_type = excluded.job_type,
        cron_expression = excluded.cron_expression,
        time_zone = excluded.time_zone,
        max_attempts = excluded.max_attempts,
        enabled = excluded.enabled,
        allow_concurrent_runs = excluded.allow_concurrent_runs,
        retry_behavior = excluded.retry_behavior,
        retry_initial_delay_seconds = excluded.retry_initial_delay_seconds,
        updated_at_utc = now();
    `;

    await this.pool.query(sql, [
      registration.jobName,
      registration.jobType,
      registration.recurring.cronExpression,
      registration.recurring.timeZone,
      Math.max(1, Math.floor(registration.maxAttempts)),
      registration.recurring.enabled ?? true,
      registration.recurring.allowConcurrentRuns ?? false,
      registration.recurring.retryBehavior ?? null,
      registration.recurring.retryInitialDelaySeconds ?? null,
      nextRunAtUtc
    ]);
  }

  public async getDueRecurringJobs(nowUtc: string, batchSize: number): Promise<RecurringJobState[]> {
    const sql = `
      select *
      from ${q(this.tables.jobs)}
      where enabled = true
        and next_run_at_utc <= $1::timestamptz
      order by next_run_at_utc asc
      limit $2;
    `;
    const result = await this.pool.query(sql, [nowUtc, Math.max(1, Math.floor(batchSize))]);
    return result.rows.map((row) => rowToRecurring(row));
  }

  public async updateRecurringNextRun(jobName: string, nextRunAtUtc: string): Promise<void> {
    const sql = `
      update ${q(this.tables.jobs)}
      set next_run_at_utc = $2::timestamptz,
          updated_at_utc = now()
      where job_name = $1;
    `;
    await this.pool.query(sql, [jobName, nextRunAtUtc]);
  }

  public async tryMaterializeRecurringRun(
    recurring: RecurringJobState,
    registration: DurableJobRegistration,
    nextRunAtUtc: string
  ): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("begin");

      if (!recurring.allowConcurrentRuns) {
        const active = await client.query(
          `select 1 from ${q(this.tables.runs)} where job_name = $1 and status in ('pending', 'leased') limit 1`,
          [recurring.jobName]
        );
        if ((active.rowCount ?? 0) > 0) {
          await client.query("rollback");
          return false;
        }
      }

      const locked = await client.query(
        `select next_run_at_utc
         from ${q(this.tables.jobs)}
         where job_name = $1
           and enabled = true
         for update`,
        [recurring.jobName]
      );

      if ((locked.rowCount ?? 0) === 0) {
        await client.query("rollback");
        return false;
      }

      const currentNextRun = new Date(String(locked.rows[0]?.next_run_at_utc)).getTime();
      const expectedNextRun = new Date(recurring.nextRunAtUtc).getTime();
      if (Math.abs(currentNextRun - expectedNextRun) > 1) {
        await client.query("rollback");
        return false;
      }

      await client.query(
        `update ${q(this.tables.jobs)}
         set next_run_at_utc = $2::timestamptz,
             updated_at_utc = now()
         where job_name = $1`,
        [recurring.jobName, nextRunAtUtc]
      );

      const runId = generateId();
      await client.query(
        `insert into ${q(this.tables.runs)}
         (id, job_name, job_type, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc, attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
         values ($1::uuid, $2, $3, 'pending', $4::timestamptz, $5::timestamptz, null, null, 0, $6, null, null, null, null, now())`,
        [runId, recurring.jobName, registration.jobType, recurring.nextRunAtUtc, recurring.nextRunAtUtc, recurring.maxAttempts]
      );

      await client.query("commit");
      return true;
    } catch (error) {
      await client.query("rollback");
      const code = (error as { code?: string }).code;
      if (code === "23505" || code === "40001" || code === "40P01") {
        return false;
      }
      throw error;
    } finally {
      client.release();
    }
  }

  public async extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runs)}
      set lease_until_utc = now() + $3::interval
      where id = $1::uuid
        and status = 'leased'
        and lease_owner = $2;
    `;
    const result = await this.pool.query(sql, [runId, workerName, intervalExpr(leaseDurationSeconds)]);
    return (result.rowCount ?? 0) > 0;
  }

  public async tryLeaseRuntimeCommandReceipt(commandId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean> {
    const sql = `
      insert into ${q(this.tables.runtimeCommandReceipts)}
      (command_id, status, error_code, error_message, run_id, recorded_at_utc, completed_at_utc, uploaded_at_utc, lease_owner, lease_until_utc)
      values ($1, $2, null, null, null, now(), null, null, $3, now() + $4::interval)
      on conflict (command_id)
      do update set
        status = excluded.status,
        recorded_at_utc = now(),
        lease_owner = excluded.lease_owner,
        lease_until_utc = excluded.lease_until_utc
      where ${q(this.tables.runtimeCommandReceipts)}.status not in ('succeeded', 'failed')
        and (${q(this.tables.runtimeCommandReceipts)}.lease_until_utc is null
          or ${q(this.tables.runtimeCommandReceipts)}.lease_until_utc <= now()
          or ${q(this.tables.runtimeCommandReceipts)}.lease_owner = excluded.lease_owner)
      returning command_id;
    `;

    const result = await this.pool.query(sql, [
      commandId,
      RUNTIME_COMMAND_RECEIPT_STATUS.LEASED,
      workerName,
      intervalExpr(leaseDurationSeconds)
    ]);
    return (result.rowCount ?? 0) > 0;
  }

  public async markRuntimeCommandAcknowledged(commandId: string, workerName: string, recordedAtUtc: string): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runtimeCommandReceipts)}
      set status = $3,
          recorded_at_utc = $4::timestamptz
      where command_id = $1 and lease_owner = $2;
    `;
    const result = await this.pool.query(sql, [commandId, workerName, RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED, recordedAtUtc]);
    return (result.rowCount ?? 0) > 0;
  }

  public async markRuntimeCommandSucceeded(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    runId: string | undefined
  ): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runtimeCommandReceipts)}
      set status = $3,
          recorded_at_utc = $4::timestamptz,
          completed_at_utc = $5::timestamptz,
          run_id = $6::uuid,
          lease_owner = null,
          lease_until_utc = null
      where command_id = $1 and lease_owner = $2;
    `;
    const result = await this.pool.query(sql, [
      commandId,
      workerName,
      RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED,
      recordedAtUtc,
      completedAtUtc,
      runId ?? null
    ]);
    return (result.rowCount ?? 0) > 0;
  }

  public async markRuntimeCommandFailed(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    errorCode: string | undefined,
    errorMessage: string | undefined
  ): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runtimeCommandReceipts)}
      set status = $3,
          recorded_at_utc = $4::timestamptz,
          completed_at_utc = $5::timestamptz,
          error_code = $6,
          error_message = $7,
          lease_owner = null,
          lease_until_utc = null
      where command_id = $1 and lease_owner = $2;
    `;
    const result = await this.pool.query(sql, [
      commandId,
      workerName,
      RUNTIME_COMMAND_RECEIPT_STATUS.FAILED,
      recordedAtUtc,
      completedAtUtc,
      errorCode ?? null,
      errorMessage ?? null
    ]);
    return (result.rowCount ?? 0) > 0;
  }

  public async getRuntimeCommandReceipts(take: number): Promise<RuntimeCommandReceiptRecord[]> {
    const sql = `
      select *
      from ${q(this.tables.runtimeCommandReceipts)}
      where uploaded_at_utc is null
        and status in ($2, $3, $4)
      order by recorded_at_utc asc
      limit $1;
    `;
    const result = await this.pool.query(sql, [
      Math.max(1, Math.floor(take)),
      RUNTIME_COMMAND_RECEIPT_STATUS.ACKNOWLEDGED,
      RUNTIME_COMMAND_RECEIPT_STATUS.SUCCEEDED,
      RUNTIME_COMMAND_RECEIPT_STATUS.FAILED
    ]);
    return result.rows.map((row) => rowToReceipt(row));
  }

  public async markRuntimeCommandReceiptUploaded(commandId: string): Promise<boolean> {
    const sql = `
      update ${q(this.tables.runtimeCommandReceipts)}
      set uploaded_at_utc = $2::timestamptz
      where command_id = $1;
    `;
    const result = await this.pool.query(sql, [commandId, nowIso()]);
    return (result.rowCount ?? 0) > 0;
  }
}
