import { fileURLToPath } from "node:url";
import { dirname } from "node:path";
import type {
  DurableStackAutodiscoveryOptions,
  DurableStackEventingOptions,
  DurableStackOptions,
  NormalizedDurableStackOptions
} from "./types.js";
import { ensurePositive, normalizePrefix } from "./utils.js";

const DEFAULT_AUTODISCOVERY: Required<Omit<DurableStackAutodiscoveryOptions, "baseDir">> = {
  enabled: false,
  includeGlobs: [],
  excludeGlobs: ["**/node_modules/**", "**/dist/**", "**/*.d.ts"],
  failOnError: true,
  maxModules: 500,
  exportName: "durableStackJobs"
};

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
  ingestionSyncJitterEnabled: true,
  ingestionSyncJitterRatio: 0.2,
  includeErrorDetail: false,
  maxErrorDetailLength: 4096,
  runtimeControlEnabled: true,
  runtimeControlSyncPath: "/v1/runtime/control/sync",
  runtimeControlSyncIntervalSeconds: 5,
  runtimeControlSyncJitterEnabled: true,
  runtimeControlSyncJitterRatio: 0.2,
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

  const defaultBaseDir = dirname(fileURLToPath(new URL("../", import.meta.url)));
  const autodiscoveryInput = input?.autodiscovery;
  const autodiscovery = {
    ...DEFAULT_AUTODISCOVERY,
    ...(autodiscoveryInput ?? {}),
    includeGlobs: (autodiscoveryInput?.includeGlobs ?? []).map((x) => x.trim()).filter((x) => x.length > 0),
    excludeGlobs: (autodiscoveryInput?.excludeGlobs ?? DEFAULT_AUTODISCOVERY.excludeGlobs).map((x) => x.trim()).filter((x) => x.length > 0),
    maxModules: Math.max(1, Math.floor(ensurePositive(autodiscoveryInput?.maxModules, DEFAULT_AUTODISCOVERY.maxModules))),
    exportName: (autodiscoveryInput?.exportName?.trim() || DEFAULT_AUTODISCOVERY.exportName),
    baseDir: (autodiscoveryInput?.baseDir?.trim() || defaultBaseDir)
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
    eventing,
    autodiscovery
  };
}

export function getEffectiveRunRetentionSeconds(options: NormalizedDurableStackOptions): number {
  return options.retention.runRetentionSeconds ?? 86_400;
}
