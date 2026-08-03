import type { DurableStackEventingOptions, DurableStackOptions, NormalizedDurableStackOptions } from "./types.js";
import { ensurePositive, normalizePrefix } from "./utils.js";

const DEFAULT_EVENTING: Required<DurableStackEventingOptions> = {
  tenantId: "",
  clientSecret: "",
  serviceName: "",
  ingestionApiBaseUrl: "https://api.durablestack.com",
  ingestionPath: "/v1/events/batch",
  ingestionMaxBatchSize: 100,
  ingestionMaxRequestBodyBytes: 1_000_000,
  ingestionMaxRetryAttempts: 5,
  ingestionFlushIntervalSeconds: 5,
  includeErrorDetail: false,
  maxErrorDetailLength: 4096,
  runtimeControlEnabled: true,
  runtimeControlSyncPath: "/v1/runtime/control/sync",
  runtimeControlSyncIntervalSeconds: 5,
  runtimeControlMaxReceiptUpload: 200,
  runtimeControlCommandLeaseDurationSeconds: 30
};

export function createDefaultWorkerName(): string {
  const host = process.env.HOSTNAME ?? "node-worker";
  return `${host}-${process.pid}`;
}

export function normalizeOptions(input: DurableStackOptions | undefined): NormalizedDurableStackOptions {
  const eventing: Required<DurableStackEventingOptions> = {
    ...DEFAULT_EVENTING,
    ...(input?.eventing ?? {})
  };

  return {
    workerName: (input?.workerName?.trim() || createDefaultWorkerName()),
    databaseTablePrefix: normalizePrefix(input?.databaseTablePrefix),
    pollIntervalSeconds: ensurePositive(input?.pollIntervalSeconds, 5),
    pollJitterEnabled: input?.pollJitterEnabled ?? true,
    pollJitterRatio: typeof input?.pollJitterRatio === "number" ? input.pollJitterRatio : 0.2,
    claimBatchSize: Math.max(1, Math.floor(ensurePositive(input?.claimBatchSize, 5))),
    maxConcurrentRuns: Math.max(1, Math.floor(ensurePositive(input?.maxConcurrentRuns, 5))),
    leaseDurationSeconds: ensurePositive(input?.leaseDurationSeconds, 30),
    shutdownDrainTimeoutSeconds: Math.max(0, ensurePositive(input?.shutdownDrainTimeoutSeconds, 10)),
    retryDelaySeconds: ensurePositive(input?.retryDelaySeconds, 5),
    retryMaxDelaySeconds: ensurePositive(input?.retryMaxDelaySeconds, 3600),
    retryJitterEnabled: input?.retryJitterEnabled ?? false,
    retryJitterRatio: typeof input?.retryJitterRatio === "number" ? input.retryJitterRatio : 0.2,
    recurring: {
      catchUpPolicy: input?.recurring?.catchUpPolicy ?? "SkipMissed",
      registrationSync: {
        existingJobBehavior: input?.recurring?.registrationSync?.existingJobBehavior ?? "KeepDatabase",
        orphanedJobBehavior: input?.recurring?.registrationSync?.orphanedJobBehavior ?? "Disable"
      }
    },
    retention: {
      enabled: input?.retention?.enabled ?? true,
      runRetentionSeconds: input?.retention?.runRetentionSeconds,
      sweepIntervalSeconds: ensurePositive(input?.retention?.sweepIntervalSeconds, 300),
      deleteBatchSize: Math.max(1, Math.floor(ensurePositive(input?.retention?.deleteBatchSize, 1000)))
    },
    eventing
  };
}

export function getEffectiveRunRetentionSeconds(options: NormalizedDurableStackOptions): number {
  return options.retention.runRetentionSeconds ?? 3600;
}
