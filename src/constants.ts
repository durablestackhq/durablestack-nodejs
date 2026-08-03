export const RUN_STATUS = {
  PENDING: "pending",
  LEASED: "leased",
  SUCCEEDED: "succeeded",
  FAILED: "failed"
} as const;

export type RunStatus = (typeof RUN_STATUS)[keyof typeof RUN_STATUS];

export const EVENT_TYPES = {
  JOB_CLAIMED: "job_claimed",
  JOB_STARTED: "job_started",
  JOB_SUCCEEDED: "job_succeeded",
  JOB_FAILED: "job_failed",
  JOB_RETRIED: "job_retried",
  RETRY_SCHEDULED: "retry_scheduled",
  WORKER_HEARTBEAT: "worker_heartbeat"
} as const;

export type DurableEventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

export const CURRENT_EVENT_VERSION = 2;

export const RUNTIME_COMMAND_TYPES = {
  SET_SCHEDULE_ENABLED: "set_schedule_enabled",
  RUN_SCHEDULE_NOW: "run_schedule_now",
  UPDATE_SCHEDULE_CRON: "update_schedule_cron"
} as const;

export type RuntimeCommandType = (typeof RUNTIME_COMMAND_TYPES)[keyof typeof RUNTIME_COMMAND_TYPES];

export const RUNTIME_COMMAND_RECEIPT_STATUS = {
  LEASED: "leased",
  ACKNOWLEDGED: "acknowledged",
  SUCCEEDED: "succeeded",
  FAILED: "failed"
} as const;

export type RuntimeCommandReceiptStatus =
  (typeof RUNTIME_COMMAND_RECEIPT_STATUS)[keyof typeof RUNTIME_COMMAND_RECEIPT_STATUS];
