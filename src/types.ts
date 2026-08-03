import {
  CURRENT_EVENT_VERSION,
  type DurableEventType,
  type RunStatus,
  type RuntimeCommandReceiptStatus,
  type RuntimeCommandType
} from "./constants.js";

export type RetryBehavior = "fixed" | "backoff";

export interface DurableStackEvent {
  eventId: string;
  eventType: DurableEventType;
  eventVersion: number;
  occurredAtUtc: string;
  runId?: string;
  jobName?: string;
  attempt?: number;
  maxAttempts?: number;
  workerName?: string;
  tenantId?: string;
  serviceName?: string;
  traceId?: string;
  spanId?: string;
  durationMs?: number;
  retryAtUtc?: string;
  errorType?: string;
  errorMessage?: string;
  errorDetail?: string;
  message?: string;
}

export interface JobRunRecord {
  id: string;
  jobName: string;
  jobType: string;
  status: RunStatus;
  scheduledForUtc: string;
  scheduleSlotUtc?: string;
  startedAtUtc?: string;
  completedAtUtc?: string;
  attempt: number;
  maxAttempts: number;
  leaseOwner?: string;
  leaseUntilUtc?: string;
  payloadJson?: string;
  errorMessage?: string;
}

export interface RecurringJobState {
  jobName: string;
  jobType: string;
  cronExpression: string;
  timeZone: string;
  maxAttempts: number;
  enabled: boolean;
  allowConcurrentRuns: boolean;
  retryBehavior?: RetryBehavior;
  retryInitialDelaySeconds?: number;
  nextRunAtUtc: string;
}

export interface RuntimeCommandReceiptRecord {
  commandId: string;
  status: RuntimeCommandReceiptStatus;
  errorCode?: string;
  errorMessage?: string;
  runId?: string;
  recordedAtUtc: string;
  completedAtUtc?: string;
  uploadedAtUtc?: string;
  leaseOwner?: string;
  leaseUntilUtc?: string;
}

export interface RuntimeJobSnapshotItemDto {
  jobName: string;
  jobType?: string;
  cronExpression: string;
  timeZone: string;
  enabled: boolean;
  nextRunAtUtc: string;
  maxAttempts: number;
  allowConcurrentRuns: boolean;
  lastSeenAtUtc: string;
}

export interface RuntimeCommandReceiptDto {
  commandId: string;
  status: RuntimeCommandReceiptStatus;
  recordedAtUtc: string;
  completedAtUtc?: string;
  runId?: string;
  errorCode?: string;
  errorMessage?: string;
}

export interface RuntimeCommandEnvelopeDto {
  commandId: string;
  commandType: RuntimeCommandType;
  payloadJson: string;
  issuedAtUtc: string;
  expiresAtUtc?: string;
}

export interface RuntimeControlSyncRequest {
  tenantId: string;
  workerName?: string;
  runtime?: string;
  runtimeVersion?: string;
  sentAtUtc: string;
  snapshotItems: RuntimeJobSnapshotItemDto[];
  receipts: RuntimeCommandReceiptDto[];
}

export interface RuntimeControlSyncResponse {
  serverTimeUtc: string;
  commands: RuntimeCommandEnvelopeDto[];
}

export interface TelemetryEventDto {
  eventType: DurableEventType;
  eventVersion: number;
  occurredAtUtc: string;
  runId?: string;
  jobName?: string;
  attempt?: number;
  maxAttempts?: number;
  workerName?: string;
  runtime?: string;
  runtimeVersion?: string;
  durationMs?: number;
  errorType?: string;
  errorMessage?: string;
  payloadJson?: string;
}

export interface TelemetryBatchRequest {
  tenantId?: string;
  idempotencyKey: string;
  serviceName?: string;
  events: TelemetryEventDto[];
}

export interface TelemetryBatchResponse {
  acceptedCount: number;
  rejectedCount: number;
  serverTimeUtc: string;
  idempotencyKey?: string;
  isDuplicate: boolean;
  correlationId?: string;
}

export interface DurableStackEventSink {
  publish(event: DurableStackEvent): Promise<void>;
}

export interface JobContext {
  runId: string;
  jobName: string;
  attempt: number;
  workerName: string;
}

export type DurableJobHandler<TPayload = unknown> =
  (payload: TPayload | undefined, context: JobContext, signal: AbortSignal) => Promise<void>;

export interface DurableJobRegistration {
  jobName: string;
  jobType: string;
  maxAttempts: number;
  retryBehavior?: RetryBehavior;
  retryInitialDelaySeconds?: number;
  recurring?: {
    cronExpression: string;
    timeZone: string;
    enabled?: boolean;
    allowConcurrentRuns?: boolean;
    retryBehavior?: RetryBehavior;
    retryInitialDelaySeconds?: number;
  };
  handler: DurableJobHandler;
}

export interface DurableStackRetentionOptions {
  enabled?: boolean;
  runRetentionSeconds?: number;
  sweepIntervalSeconds?: number;
  deleteBatchSize?: number;
}

export interface DurableStackRecurringRegistrationSyncOptions {
  existingJobBehavior?: "KeepDatabase" | "UpdateFromCode";
  orphanedJobBehavior?: "Disable" | "Ignore";
}

export interface DurableStackRecurringOptions {
  catchUpPolicy?: "SkipMissed" | "CatchUp";
  registrationSync?: DurableStackRecurringRegistrationSyncOptions;
}

export interface DurableStackEventingOptions {
  tenantId?: string;
  clientSecret?: string;
  serviceName?: string;
  ingestionApiBaseUrl?: string;
  ingestionPath?: string;
  ingestionMaxBatchSize?: number;
  ingestionMaxRequestBodyBytes?: number;
  ingestionMaxRetryAttempts?: number;
  ingestionFlushIntervalSeconds?: number;
  includeErrorDetail?: boolean;
  maxErrorDetailLength?: number;
  runtimeControlEnabled?: boolean;
  runtimeControlSyncPath?: string;
  runtimeControlSyncIntervalSeconds?: number;
  runtimeControlMaxReceiptUpload?: number;
  runtimeControlCommandLeaseDurationSeconds?: number;
}

export interface DurableStackAutodiscoveryOptions {
  enabled?: boolean;
  includeGlobs?: string[];
  excludeGlobs?: string[];
  failOnError?: boolean;
  maxModules?: number;
  baseDir?: string;
  exportName?: string;
}

export interface DurableStackOptions {
  workerName?: string;
  databaseTablePrefix?: string;
  pollIntervalSeconds?: number;
  pollJitterEnabled?: boolean;
  pollJitterRatio?: number;
  claimBatchSize?: number;
  maxConcurrentRuns?: number;
  leaseDurationSeconds?: number;
  shutdownDrainTimeoutSeconds?: number;
  retryDelaySeconds?: number;
  retryMaxDelaySeconds?: number;
  retryJitterEnabled?: boolean;
  retryJitterRatio?: number;
  recurring?: DurableStackRecurringOptions;
  retention?: DurableStackRetentionOptions;
  eventing?: DurableStackEventingOptions;
  autodiscovery?: DurableStackAutodiscoveryOptions;
}

export interface NormalizedDurableStackOptions {
  workerName: string;
  databaseTablePrefix?: string;
  pollIntervalSeconds: number;
  pollJitterEnabled: boolean;
  pollJitterRatio: number;
  claimBatchSize: number;
  maxConcurrentRuns: number;
  leaseDurationSeconds: number;
  shutdownDrainTimeoutSeconds: number;
  retryDelaySeconds: number;
  retryMaxDelaySeconds: number;
  retryJitterEnabled: boolean;
  retryJitterRatio: number;
  recurring: {
    catchUpPolicy: "SkipMissed" | "CatchUp";
    registrationSync: {
      existingJobBehavior: "KeepDatabase" | "UpdateFromCode";
      orphanedJobBehavior: "Disable" | "Ignore";
    };
  };
  retention: {
    enabled: boolean;
    runRetentionSeconds?: number;
    sweepIntervalSeconds: number;
    deleteBatchSize: number;
  };
  eventing: Required<DurableStackEventingOptions>;
  autodiscovery: {
    enabled: boolean;
    includeGlobs: string[];
    excludeGlobs: string[];
    failOnError: boolean;
    maxModules: number;
    baseDir: string;
    exportName: string;
  };
}

export interface DurableJobStore {
  enqueue(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string>;

  claimDueRuns(workerName: string, batchSize: number, leaseDurationSeconds: number): Promise<JobRunRecord[]>;

  markSucceeded(runId: string, workerName: string): Promise<boolean>;
  cancelRun(runId: string): Promise<boolean>;
  markFailed(
    runId: string,
    workerName: string,
    errorMessage: string,
    retry: boolean,
    retryAtUtc: string | undefined
  ): Promise<boolean>;

  getRun(runId: string): Promise<JobRunRecord | undefined>;
  getRecentRuns(take: number): Promise<JobRunRecord[]>;
  getRuns(): Promise<JobRunRecord[]>;
  getRunsByJobName(jobName: string, take: number): Promise<JobRunRecord[]>;
  getRunsByStatus(status: RunStatus, take: number): Promise<JobRunRecord[]>;
  getEnqueuedRuns(take: number): Promise<JobRunRecord[]>;

  tryEnqueueIfNoActiveRun(
    jobName: string,
    jobType: string,
    payloadJson: string | undefined,
    scheduledForUtc: string,
    maxAttempts: number
  ): Promise<string | undefined>;

  getRecurringJobs(includeDisabled: boolean): Promise<RecurringJobState[]>;
  setRecurringJobEnabled(jobName: string, enabled: boolean, nextRunAtUtc: string | undefined): Promise<boolean>;
  updateRecurringJobSchedule(
    jobName: string,
    cronExpression: string,
    timeZone: string,
    nextRunAtUtc: string
  ): Promise<boolean>;

  pruneHistoricalRuns(completedBeforeUtc: string, batchSize: number): Promise<number>;

  upsertRecurringJob(registration: DurableJobRegistration, nextRunAtUtc: string): Promise<void>;
  getDueRecurringJobs(nowUtc: string, batchSize: number): Promise<RecurringJobState[]>;
  updateRecurringNextRun(jobName: string, nextRunAtUtc: string): Promise<void>;
  tryMaterializeRecurringRun(
    recurring: RecurringJobState,
    registration: DurableJobRegistration,
    nextRunAtUtc: string
  ): Promise<boolean>;

  extendLease(runId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean>;

  tryLeaseRuntimeCommandReceipt(commandId: string, workerName: string, leaseDurationSeconds: number): Promise<boolean>;
  markRuntimeCommandAcknowledged(commandId: string, workerName: string, recordedAtUtc: string): Promise<boolean>;
  markRuntimeCommandSucceeded(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    runId: string | undefined
  ): Promise<boolean>;
  markRuntimeCommandFailed(
    commandId: string,
    workerName: string,
    recordedAtUtc: string,
    completedAtUtc: string,
    errorCode: string | undefined,
    errorMessage: string | undefined
  ): Promise<boolean>;
  getRuntimeCommandReceipts(take: number): Promise<RuntimeCommandReceiptRecord[]>;
  markRuntimeCommandReceiptUploaded(commandId: string): Promise<boolean>;
}

export function createEventBase(partial: Omit<DurableStackEvent, "eventVersion" | "occurredAtUtc">): DurableStackEvent {
  return {
    eventVersion: CURRENT_EVENT_VERSION,
    occurredAtUtc: new Date().toISOString(),
    ...partial
  };
}
