import type {
  DurableStackEvent,
  DurableStackEventSink,
  NormalizedDurableStackOptions,
  TelemetryBatchRequest,
  TelemetryBatchResponse,
  TelemetryEventDto
} from "../types.js";
import { EVENT_TYPES } from "../constants.js";
import { defaultHttpPost, isTransientStatus, type HttpPost } from "./http.js";
import { generateId, nowIso, randomJittered, sleep } from "../utils.js";

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

function buildPayloadJson(event: DurableStackEvent): string {
  return JSON.stringify({
    message: event.message,
    errorType: event.errorType,
    errorMessage: event.errorMessage,
    errorDetail: event.errorDetail,
    durationMs: event.durationMs,
    retryAtUtc: event.retryAtUtc,
    traceId: event.traceId,
    spanId: event.spanId
  });
}

function toTelemetryEventDto(event: DurableStackEvent): TelemetryEventDto {
  return {
    eventType: event.eventType,
    eventVersion: event.eventVersion,
    occurredAtUtc: event.occurredAtUtc,
    runId: event.runId,
    jobName: event.jobName,
    attempt: event.attempt,
    maxAttempts: event.maxAttempts,
    workerName: event.workerName,
    durationMs: event.durationMs,
    errorType: event.errorType,
    errorMessage: event.errorMessage,
    payloadJson: buildPayloadJson(event)
  };
}

function getLatestEvent(events: DurableStackEvent[]): DurableStackEvent {
  return events.reduce((latest, current) =>
    Date.parse(current.occurredAtUtc) > Date.parse(latest.occurredAtUtc) ? current : latest
  );
}

function getEarliestEvent(events: DurableStackEvent[]): DurableStackEvent {
  return events.reduce((earliest, current) =>
    Date.parse(current.occurredAtUtc) < Date.parse(earliest.occurredAtUtc) ? current : earliest
  );
}

function buildTelemetryEvents(
  events: DurableStackEvent[],
  runtimeName: string,
  runtimeVersion: string
): TelemetryEventDto[] {
  const list: TelemetryEventDto[] = [];
  const heartbeatEvents: DurableStackEvent[] = [];

  for (const event of events) {
    if (event.eventType === EVENT_TYPES.WORKER_HEARTBEAT) {
      heartbeatEvents.push(event);
      continue;
    }

    list.push({
      ...toTelemetryEventDto(event),
      runtime: runtimeName,
      runtimeVersion
    });
  }

  if (heartbeatEvents.length > 0) {
    const latest = getLatestEvent(heartbeatEvents);
    const earliest = getEarliestEvent(heartbeatEvents);
    const heartbeatPayload = JSON.stringify({
      heartbeatCount: heartbeatEvents.length,
      firstHeartbeatAtUtc: earliest.occurredAtUtc,
      lastHeartbeatAtUtc: latest.occurredAtUtc
    });

    list.push({
      eventType: "worker_heartbeat_batch",
      eventVersion: latest.eventVersion,
      occurredAtUtc: latest.occurredAtUtc,
      workerName: latest.workerName,
      runtime: runtimeName,
      runtimeVersion,
      payloadJson: heartbeatPayload
    });
  }

  return list;
}

function buildIdempotencyKey(workerName: string, sequence: number): string {
  return `${workerName}:${Date.now()}:${sequence}`;
}

export class IngestionDurableStackEventSink implements DurableStackEventSink {
  private readonly queue: DurableStackEvent[] = [];

  public constructor(private readonly maxQueueSize = 10_000) {}

  public async publish(event: DurableStackEvent): Promise<void> {
    if (this.queue.length >= this.maxQueueSize) {
      this.queue.shift();
    }
    this.queue.push(event);
  }

  public drain(maxItems: number): DurableStackEvent[] {
    const count = Math.max(1, Math.floor(maxItems));
    return this.queue.splice(0, count);
  }

  public get size(): number {
    return this.queue.length;
  }
}

export class IngestionEventSyncService {
  private running = false;
  private sequence = 0;
  private loopPromise: Promise<void> | undefined;
  private abortController: AbortController | undefined;
  private readonly runtimeVersion: string;

  public constructor(
    private readonly sink: IngestionDurableStackEventSink,
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

    if (!this.options.eventing.tenantId || !this.options.eventing.clientSecret) {
      return;
    }

    this.resolveEndpoint();

    this.running = true;
    this.abortController = new AbortController();
    const signal = this.abortController.signal;

    this.loopPromise = (async () => {
      const initialDelayMs = Math.max(0, Math.floor(randomJittered(
        Math.max(0.1, this.options.eventing.ingestionFlushIntervalSeconds),
        this.options.eventing.ingestionSyncJitterEnabled,
        this.options.eventing.ingestionSyncJitterRatio
      ) * 1000));
      if (initialDelayMs > 0) {
        await sleep(initialDelayMs, signal);
      }

      while (this.running && !signal.aborted) {
        try {
          await this.flushOnce(signal);
        } catch (error) {
          const message = error instanceof Error ? error.stack ?? error.message : String(error);
          console.warn(`DurableStack ingestion sync cycle failed. Worker=${this.options.workerName}. ${message}`);
          await sleep(2_000, signal);
        }
        const loopDelayMs = Math.max(0, Math.floor(randomJittered(
          Math.max(0.1, this.options.eventing.ingestionFlushIntervalSeconds),
          this.options.eventing.ingestionSyncJitterEnabled,
          this.options.eventing.ingestionSyncJitterRatio
        ) * 1000));
        await sleep(loopDelayMs, signal);
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

    // Final flush so events buffered at shutdown are not silently dropped.
    const finalFlushSignal = new AbortController().signal;
    for (let i = 0; i < 10 && this.sink.size > 0; i += 1) {
      try {
        await this.flushOnce(finalFlushSignal);
      } catch {
        break;
      }
    }
  }

  private resolveEndpoint(): string {
    try {
      return new URL(this.options.eventing.ingestionPath, this.options.eventing.ingestionApiBaseUrl).toString();
    } catch {
      throw new Error(
        `Invalid ingestion endpoint configuration: cannot combine ingestionApiBaseUrl '${this.options.eventing.ingestionApiBaseUrl}' with ingestionPath '${this.options.eventing.ingestionPath}'. The base URL must be absolute, including its scheme (e.g. https://).`
      );
    }
  }

  public async flushOnce(signal: AbortSignal): Promise<void> {
    if (!this.options.eventing.tenantId || !this.options.eventing.clientSecret) {
      return;
    }

    const batch = this.sink.drain(this.options.eventing.ingestionMaxBatchSize);
    if (batch.length === 0) {
      return;
    }

    const idempotencyKey = buildIdempotencyKey(this.options.workerName, ++this.sequence);
    const payload: TelemetryBatchRequest = {
      tenantId: this.options.eventing.tenantId,
      idempotencyKey,
      serviceName: this.options.eventing.serviceName,
      events: buildTelemetryEvents(batch, this.runtimeName, this.runtimeVersion)
    };

    const json = JSON.stringify(payload);
    if (Buffer.byteLength(json, "utf8") > this.options.eventing.ingestionMaxRequestBodyBytes) {
      return;
    }

    const endpoint = this.resolveEndpoint();
    const maxAttempts = Math.max(1, Math.min(10, this.options.eventing.ingestionMaxRetryAttempts));

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
          body: json
        }, signal);
      } catch {
        if (attempt >= maxAttempts || signal.aborted) {
          return;
        }
        await sleep(Math.min(10, attempt) * 200, signal);
        continue;
      }

      if (response.status >= 200 && response.status < 300) {
        return;
      }

      if (response.status === 401 || response.status === 403) {
        console.warn(
          `DurableStack ingestion sync authorization failed with status ${response.status}.${summarizeBody(response.bodyText)}`
        );
        return;
      }

      if (!isTransientStatus(response.status) || attempt >= maxAttempts) {
        if (!isTransientStatus(response.status)) {
          console.warn(
            `DurableStack ingestion sync rejected without retry. Status=${response.status}.${summarizeBody(response.bodyText)}`
          );
        } else {
          console.warn(
            `DurableStack ingestion sync failed after retries. Status=${response.status}.${summarizeBody(response.bodyText)}`
          );
        }
        return;
      }

      await sleep(Math.min(10, attempt) * 200, signal);
    }
  }

  public static parseBatchResponse(json: string): TelemetryBatchResponse | undefined {
    try {
      return JSON.parse(json) as TelemetryBatchResponse;
    } catch {
      return undefined;
    }
  }
}

export function createIngestionEventing(
  options: NormalizedDurableStackOptions,
  httpPost?: HttpPost
): {
  sink: IngestionDurableStackEventSink;
  service: IngestionEventSyncService;
} {
  const sink = new IngestionDurableStackEventSink();
  const service = new IngestionEventSyncService(sink, options, httpPost);
  return { sink, service };
}
