import type {
  NormalizedDurableStackOptions,
  RuntimeCommandEnvelopeDto,
  RuntimeControlSyncRequest,
  RuntimeControlSyncResponse,
  RuntimeJobSnapshotItemDto,
  RuntimeCommandReceiptDto,
  DurableJobStore,
  RecurringJobState
} from "../types.js";
import { defaultHttpPost, isTransientStatus, type HttpPost } from "./http.js";
import { generateId, nowIso, randomJittered, sleep } from "../utils.js";
import { validateRuntimeCommandEnvelopeDto, validateRuntimeControlSyncRequest } from "../validators.js";

function computeBackoffDelayMs(attempt: number): number {
  const bounded = Math.max(1, attempt);
  const base = Math.min(30_000, 500 * Math.pow(2, Math.max(0, bounded - 1)));
  const jitter = Math.floor(Math.random() * 250);
  return base + jitter;
}

function normalizeRuntimeVersion(value: string): string {
  const trimmed = (value || "").trim();
  if (!trimmed) {
    return "unknown";
  }

  return trimmed.replace(/^v/i, "");
}

function defaultRuntimeName(runtimeVersion: string): string {
  return `Node.js ${normalizeRuntimeVersion(runtimeVersion)}`;
}

function summarizeBody(bodyText: string | undefined): string {
  if (!bodyText) {
    return "";
  }
  const compact = bodyText.replace(/\s+/g, " ").trim();
  if (!compact) {
    return "";
  }
  const truncated = compact.length > 600 ? `${compact.slice(0, 600)}...` : compact;
  return ` Body=${truncated}`;
}

function createCorrelationId(): string {
  return generateId().replace(/-/g, "");
}

export interface RuntimeControlAdmin {
  listScheduledJobs(includeDisabled: boolean): Promise<RecurringJobState[]>;
  setScheduledJobEnabled(jobName: string, enabled: boolean): Promise<boolean>;
  updateScheduledJobCron(jobName: string, cronExpression: string, timeZone: string): Promise<boolean>;
  runScheduledJobNow(jobName: string): Promise<string | undefined>;
}

function buildSnapshotItems(schedules: RecurringJobState[], lastSeenAtUtc: string): RuntimeJobSnapshotItemDto[] {
  return schedules.map((schedule) => ({
    jobName: schedule.jobName,
    jobType: schedule.jobType,
    cronExpression: schedule.cronExpression,
    timeZone: schedule.timeZone,
    enabled: schedule.enabled,
    nextRunAtUtc: schedule.nextRunAtUtc,
    maxAttempts: schedule.maxAttempts,
    allowConcurrentRuns: schedule.allowConcurrentRuns,
    lastSeenAtUtc
  }));
}

function toReceiptDto(receipt: Awaited<ReturnType<DurableJobStore["getRuntimeCommandReceipts"]>>[number]): RuntimeCommandReceiptDto {
  return {
    commandId: receipt.commandId,
    status: receipt.status,
    recordedAtUtc: receipt.recordedAtUtc,
    completedAtUtc: receipt.completedAtUtc,
    runId: receipt.runId,
    errorCode: receipt.errorCode,
    errorMessage: receipt.errorMessage
  };
}

export class RuntimeControlSyncService {
  private running = false;
  private loopPromise: Promise<void> | undefined;
  private abortController: AbortController | undefined;
  private readonly runtimeVersion: string;

  public constructor(
    private readonly store: DurableJobStore,
    private readonly admin: RuntimeControlAdmin,
    private readonly options: NormalizedDurableStackOptions,
    private readonly httpPost: HttpPost = defaultHttpPost,
    runtimeName: string | undefined = undefined,
    runtimeVersion = process.version
  ) {
    this.runtimeName = (runtimeName && runtimeName.trim()) ? runtimeName.trim() : defaultRuntimeName(runtimeVersion);
    this.runtimeVersion = normalizeRuntimeVersion(runtimeVersion);
  }

  private readonly runtimeName: string;

  public start(): void {
    if (this.running) {
      return;
    }

    if (!this.options.eventing.runtimeControlEnabled) {
      return;
    }

    if (!this.options.eventing.tenantId || !this.options.eventing.clientSecret) {
      return;
    }

    this.running = true;
    this.abortController = new AbortController();

    this.loopPromise = (async () => {
      const initialDelayMs = Math.max(0, Math.floor(randomJittered(
        Math.max(0.1, this.options.eventing.runtimeControlSyncIntervalSeconds),
        this.options.eventing.runtimeControlSyncJitterEnabled,
        this.options.eventing.runtimeControlSyncJitterRatio
      ) * 1000));
      if (initialDelayMs > 0) {
        await sleep(initialDelayMs);
      }

      while (this.running && this.abortController && !this.abortController.signal.aborted) {
        try {
          await this.syncOnce(this.abortController.signal);
        } catch (error) {
          const message = error instanceof Error ? error.stack ?? error.message : String(error);
          console.warn(`DurableStack runtime control sync cycle failed. Worker=${this.options.workerName}. ${message}`);
          await sleep(2_000);
        }
        const loopDelayMs = Math.max(0, Math.floor(randomJittered(
          Math.max(0.1, this.options.eventing.runtimeControlSyncIntervalSeconds),
          this.options.eventing.runtimeControlSyncJitterEnabled,
          this.options.eventing.runtimeControlSyncJitterRatio
        ) * 1000));
        await sleep(loopDelayMs);
      }
    })();
  }

  public async stop(): Promise<void> {
    if (!this.running) {
      return;
    }
    this.running = false;
    this.abortController?.abort();
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  public async syncOnce(signal: AbortSignal): Promise<void> {
    if (!this.options.eventing.tenantId || !this.options.eventing.clientSecret) {
      return;
    }

    const sentAtUtc = nowIso();
    const schedules = await this.admin.listScheduledJobs(true);
    const receipts = await this.store.getRuntimeCommandReceipts(this.options.eventing.runtimeControlMaxReceiptUpload);

    const request: RuntimeControlSyncRequest = {
      tenantId: this.options.eventing.tenantId,
      workerName: this.options.workerName,
      runtime: this.runtimeName,
      runtimeVersion: this.runtimeVersion,
      sentAtUtc,
      snapshotItems: buildSnapshotItems(schedules, sentAtUtc),
      receipts: receipts.map((x) => toReceiptDto(x))
    };

    validateRuntimeControlSyncRequest(request);

    const endpoint = new URL(this.options.eventing.runtimeControlSyncPath, this.options.eventing.ingestionApiBaseUrl).toString();
    const payload = JSON.stringify(request);
    if (process.env.DURABLESTACK_RUNTIME_CONTROL_DEBUG_PAYLOAD === "1") {
      console.warn(`DurableStack runtime control request payload: ${payload}`);
    }
    const maxAttempts = Math.max(1, Math.min(10, this.options.eventing.ingestionMaxRetryAttempts));

    let responseBody: string | undefined;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      let response;
      try {
        response = await this.httpPost({
          url: endpoint,
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            "X-DurableStack-TenantId": this.options.eventing.tenantId,
            "X-DurableStack-ClientSecret": this.options.eventing.clientSecret,
            "X-Correlation-Id": createCorrelationId()
          },
          body: payload
        }, signal);
      } catch {
        if (attempt >= maxAttempts) {
          console.warn("DurableStack runtime control sync failed after retries due to request error.");
          return;
        }
        await sleep(computeBackoffDelayMs(attempt));
        continue;
      }

      if (response.status >= 200 && response.status < 300) {
        responseBody = response.bodyText;
        break;
      }

      if (response.status === 401 || response.status === 403) {
        console.warn(
          `DurableStack runtime control sync authorization failed with status ${response.status}.${summarizeBody(response.bodyText)}`
        );
        return;
      }

      if (!isTransientStatus(response.status) || attempt >= maxAttempts) {
        if (!isTransientStatus(response.status)) {
          console.warn(
            `DurableStack runtime control sync rejected without retry. Status=${response.status}. Worker=${request.workerName ?? "unknown"} Runtime=${request.runtime ?? "unknown"} RuntimeVersion=${request.runtimeVersion ?? "unknown"} SnapshotItems=${request.snapshotItems.length} Receipts=${request.receipts.length}.${summarizeBody(response.bodyText)}`
          );
        } else {
          console.warn(
            `DurableStack runtime control sync failed after retries. Status=${response.status}. Worker=${request.workerName ?? "unknown"} Runtime=${request.runtime ?? "unknown"} RuntimeVersion=${request.runtimeVersion ?? "unknown"} SnapshotItems=${request.snapshotItems.length} Receipts=${request.receipts.length}.${summarizeBody(response.bodyText)}`
          );
        }
        return;
      }

      await sleep(computeBackoffDelayMs(attempt));
    }

    if (!responseBody) {
      return;
    }

    const parsed = RuntimeControlSyncService.parseResponse(responseBody);
    if (!parsed) {
      return;
    }

    for (const receipt of receipts) {
      await this.store.markRuntimeCommandReceiptUploaded(receipt.commandId);
    }

    await this.processCommands(parsed.commands, signal);
  }

  public async processCommands(commands: RuntimeCommandEnvelopeDto[], _signal: AbortSignal): Promise<void> {
    for (const command of commands) {
      try {
        validateRuntimeCommandEnvelopeDto(command);
      } catch {
        continue;
      }

      const commandId = command.commandId.trim();
      if (!commandId) {
        continue;
      }

      if (command.expiresAtUtc && Date.parse(command.expiresAtUtc) <= Date.now()) {
        continue;
      }

      const leased = await this.store.tryLeaseRuntimeCommandReceipt(
        commandId,
        this.options.workerName,
        this.options.eventing.runtimeControlCommandLeaseDurationSeconds
      );
      if (!leased) {
        continue;
      }

      const acknowledged = await this.store.markRuntimeCommandAcknowledged(commandId, this.options.workerName, nowIso());
      if (!acknowledged) {
        continue;
      }

      try {
        const result = await this.executeCommand(command);
        if (result.success) {
          await this.store.markRuntimeCommandSucceeded(
            commandId,
            this.options.workerName,
            nowIso(),
            nowIso(),
            result.runId
          );
        } else {
          await this.store.markRuntimeCommandFailed(
            commandId,
            this.options.workerName,
            nowIso(),
            nowIso(),
            result.errorCode,
            result.errorMessage
          );
        }
      } catch (error) {
        await this.store.markRuntimeCommandFailed(
          commandId,
          this.options.workerName,
          nowIso(),
          nowIso(),
          "runtime_exception",
          error instanceof Error ? error.message : "Runtime command execution failed"
        );
      }
    }
  }

  private async executeCommand(command: RuntimeCommandEnvelopeDto): Promise<
    | { success: true; runId?: string }
    | { success: false; errorCode: string; errorMessage: string }
  > {
    const commandType = command.commandType.trim();
    if (!commandType) {
      return { success: false, errorCode: "invalid_command_type", errorMessage: "CommandType is required." };
    }

    let payload: Record<string, unknown>;
    try {
      const parsed = JSON.parse(command.payloadJson || "{}");
      payload = typeof parsed === "object" && parsed !== null ? parsed as Record<string, unknown> : {};
    } catch {
      payload = {};
    }

    const payloadString = (camelKey: string, pascalKey: string): string | undefined => {
      const camelValue = payload[camelKey];
      if (typeof camelValue === "string") {
        return camelValue;
      }
      const pascalValue = payload[pascalKey];
      if (typeof pascalValue === "string") {
        return pascalValue;
      }
      return undefined;
    };

    const payloadBoolean = (camelKey: string, pascalKey: string): boolean | undefined => {
      const camelValue = payload[camelKey];
      if (typeof camelValue === "boolean") {
        return camelValue;
      }
      const pascalValue = payload[pascalKey];
      if (typeof pascalValue === "boolean") {
        return pascalValue;
      }
      return undefined;
    };

    switch (commandType) {
      case "set_schedule_enabled": {
        const jobName = payloadString("jobName", "JobName");
        const enabled = payloadBoolean("enabled", "Enabled");
        if (!jobName || typeof enabled !== "boolean") {
          return { success: false, errorCode: "invalid_payload", errorMessage: "Payload must include jobName and enabled." };
        }

        const normalizedJobName = jobName.trim();
        const updated = await this.admin.setScheduledJobEnabled(normalizedJobName, enabled);
        return updated
          ? { success: true }
          : { success: false, errorCode: "schedule_not_found", errorMessage: `Scheduled job '${normalizedJobName}' was not found.` };
      }

      case "run_schedule_now": {
        const jobName = payloadString("jobName", "JobName");
        if (!jobName) {
          return { success: false, errorCode: "invalid_payload", errorMessage: "Payload must include jobName." };
        }
        const normalizedJobName = jobName.trim();
        const runId = await this.admin.runScheduledJobNow(normalizedJobName);
        return runId
          ? { success: true, runId }
          : { success: false, errorCode: "schedule_not_found", errorMessage: `Scheduled job '${normalizedJobName}' was not found.` };
      }

      case "update_schedule_cron": {
        const jobName = payloadString("jobName", "JobName");
        const cronExpression = payloadString("cronExpression", "CronExpression");
        const timeZone = payloadString("timeZone", "TimeZone");
        if (!jobName || !cronExpression || !timeZone) {
          return {
            success: false,
            errorCode: "invalid_payload",
            errorMessage: "Payload must include jobName, cronExpression, and timeZone."
          };
        }

        const updated = await this.admin.updateScheduledJobCron(
          jobName.trim(),
          cronExpression.trim(),
          timeZone.trim()
        );
        return updated
          ? { success: true }
          : { success: false, errorCode: "schedule_not_found", errorMessage: `Scheduled job '${jobName.trim()}' was not found.` };
      }

      default:
        return {
          success: false,
          errorCode: "unsupported_command_type",
          errorMessage: `Unsupported command type '${commandType}'.`
        };
    }
  }

  public static parseResponse(json: string): RuntimeControlSyncResponse | undefined {
    try {
      const parsed = JSON.parse(json) as Record<string, unknown>;
      const commandsCandidate = parsed.commands ?? parsed.Commands;
      const commands = Array.isArray(commandsCandidate) ? commandsCandidate.map((item) => {
        if (!item || typeof item !== "object") {
          return item;
        }
        const command = item as Record<string, unknown>;
        return {
          commandId: String(command.commandId ?? command.CommandId ?? ""),
          commandType: String(command.commandType ?? command.CommandType ?? ""),
          payloadJson: String(command.payloadJson ?? command.PayloadJson ?? "{}"),
          issuedAtUtc: String(command.issuedAtUtc ?? command.IssuedAtUtc ?? nowIso()),
          expiresAtUtc: typeof (command.expiresAtUtc ?? command.ExpiresAtUtc) === "string"
            ? String(command.expiresAtUtc ?? command.ExpiresAtUtc)
            : undefined
        } satisfies RuntimeCommandEnvelopeDto;
      }) : [];
      const serverTimeUtc = parsed.serverTimeUtc ?? parsed.ServerTimeUtc;
      const errorCode = parsed.errorCode ?? parsed.ErrorCode;
      const errorMessage = parsed.errorMessage ?? parsed.ErrorMessage;
      return {
        serverTimeUtc: typeof serverTimeUtc === "string" ? serverTimeUtc : nowIso(),
        commands,
        errorCode: typeof errorCode === "string" ? errorCode : undefined,
        errorMessage: typeof errorMessage === "string" ? errorMessage : undefined
      };
    } catch {
      return undefined;
    }
  }
}
