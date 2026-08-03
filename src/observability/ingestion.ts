import type {
  DurableStackEvent,
  DurableStackEventSink,
  NormalizedDurableStackOptions,
  TelemetryBatchRequest,
  TelemetryBatchResponse,
  TelemetryEventDto
} from "../types.js";
import { defaultHttpPost, isTransientStatus, type HttpPost } from "./http.js";
import { nowIso, sleep } from "../utils.js";

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
    payloadJson: event.message
  };
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

  public constructor(
    private readonly sink: IngestionDurableStackEventSink,
    private readonly options: NormalizedDurableStackOptions,
    private readonly httpPost: HttpPost = defaultHttpPost,
    private readonly runtimeName = "Node.js",
    private readonly runtimeVersion = process.version
  ) {}

  public start(): void {
    if (this.running) {
      return;
    }

    if (!this.options.eventing.tenantId || !this.options.eventing.clientSecret) {
      return;
    }

    this.running = true;
    this.abortController = new AbortController();

    this.loopPromise = (async () => {
      while (this.running && this.abortController && !this.abortController.signal.aborted) {
        await this.flushOnce(this.abortController.signal);
        await sleep(Math.max(1, this.options.eventing.ingestionFlushIntervalSeconds) * 1000);
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
      events: batch.map((event) => ({
        ...toTelemetryEventDto(event),
        runtime: this.runtimeName,
        runtimeVersion: this.runtimeVersion
      }))
    };

    const json = JSON.stringify(payload);
    if (Buffer.byteLength(json, "utf8") > this.options.eventing.ingestionMaxRequestBodyBytes) {
      return;
    }

    const endpoint = new URL(this.options.eventing.ingestionPath, this.options.eventing.ingestionApiBaseUrl).toString();
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
            "X-Correlation-Id": `${this.options.workerName}:${Date.now()}`
          },
          body: json
        }, signal);
      } catch {
        if (attempt >= maxAttempts) {
          return;
        }
        await sleep(Math.min(10, attempt) * 200);
        continue;
      }

      if (response.status >= 200 && response.status < 300) {
        return;
      }

      if (response.status === 401 || response.status === 403) {
        return;
      }

      if (!isTransientStatus(response.status) || attempt >= maxAttempts) {
        return;
      }

      await sleep(Math.min(10, attempt) * 200);
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
