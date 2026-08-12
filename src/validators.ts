import {
  CURRENT_EVENT_VERSION,
  EVENT_TYPES,
  RUNTIME_COMMAND_RECEIPT_STATUS
} from "./constants.js";
import type {
  RuntimeCommandEnvelopeDto,
  RuntimeCommandReceiptDto,
  RuntimeControlSyncRequest,
  TelemetryBatchRequest,
  TelemetryEventDto
} from "./types.js";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

function isIsoDate(value: unknown): value is string {
  if (typeof value !== "string") {
    return false;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function validateTelemetryEventDto(value: unknown): asserts value is TelemetryEventDto {
  assert(isObject(value), "telemetry event must be an object");
  assert(typeof value.eventType === "string", "eventType is required");
  assert(Object.values(EVENT_TYPES).includes(value.eventType as (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES]), "eventType is invalid");
  assert(typeof value.eventVersion === "number", "eventVersion is required");
  assert(value.eventVersion === CURRENT_EVENT_VERSION, "eventVersion is not supported");
  assert(isIsoDate(value.occurredAtUtc), "occurredAtUtc must be an ISO date");
}

export function validateTelemetryBatchRequest(value: unknown): asserts value is TelemetryBatchRequest {
  assert(isObject(value), "telemetry batch request must be an object");
  assert(typeof value.idempotencyKey === "string" && value.idempotencyKey.length > 0, "idempotencyKey is required");
  assert(Array.isArray(value.events), "events must be an array");

  for (const event of value.events) {
    validateTelemetryEventDto(event);
  }
}

export function validateRuntimeCommandEnvelopeDto(value: unknown): asserts value is RuntimeCommandEnvelopeDto {
  assert(isObject(value), "runtime command envelope must be an object");
  assert(typeof value.commandId === "string" && value.commandId.length > 0, "commandId is required");
  assert(typeof value.commandType === "string", "commandType is required");
  assert(typeof value.payloadJson === "string", "payloadJson is required");
  assert(isIsoDate(value.issuedAtUtc), "issuedAtUtc must be an ISO date");
  if (typeof value.expiresAtUtc !== "undefined") {
    assert(isIsoDate(value.expiresAtUtc), "expiresAtUtc must be an ISO date when provided");
  }
}

export function validateRuntimeCommandReceiptDto(value: unknown): asserts value is RuntimeCommandReceiptDto {
  assert(isObject(value), "runtime command receipt must be an object");
  assert(typeof value.commandId === "string" && value.commandId.length > 0, "commandId is required");
  assert(typeof value.status === "string", "status is required");
  assert(
    Object.values(RUNTIME_COMMAND_RECEIPT_STATUS).includes(
      value.status as (typeof RUNTIME_COMMAND_RECEIPT_STATUS)[keyof typeof RUNTIME_COMMAND_RECEIPT_STATUS]
    ),
    "receipt status is invalid"
  );
  assert(isIsoDate(value.recordedAtUtc), "recordedAtUtc must be an ISO date");
  if (typeof value.completedAtUtc !== "undefined") {
    assert(isIsoDate(value.completedAtUtc), "completedAtUtc must be an ISO date when provided");
  }
}

export function validateRuntimeControlSyncRequest(value: unknown): asserts value is RuntimeControlSyncRequest {
  assert(isObject(value), "runtime control sync request must be an object");
  assert(typeof value.tenantId === "string" && value.tenantId.length > 0, "tenantId is required");
  assert(isIsoDate(value.sentAtUtc), "sentAtUtc must be an ISO date");
  assert(Array.isArray(value.snapshotItems), "snapshotItems must be an array");
  assert(Array.isArray(value.receipts), "receipts must be an array");

  for (const receipt of value.receipts) {
    validateRuntimeCommandReceiptDto(receipt);
  }
}
