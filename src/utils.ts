import { randomUUID } from "node:crypto";

export function nowIso(): string {
  return new Date().toISOString();
}

export function toDate(value: string): Date {
  return new Date(value);
}

export function addSeconds(iso: string, seconds: number): string {
  const date = toDate(iso);
  date.setTime(date.getTime() + Math.max(0, seconds) * 1000);
  return date.toISOString();
}

export function generateId(): string {
  return randomUUID();
}

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function ensurePositive(value: number | undefined, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return value;
}

export function safeJsonParse<T>(json: string | undefined): T | undefined {
  if (!json) {
    return undefined;
  }

  try {
    return JSON.parse(json) as T;
  } catch {
    return undefined;
  }
}

export function safeJsonStringify(value: unknown): string | undefined {
  if (typeof value === "undefined") {
    return undefined;
  }
  return JSON.stringify(value);
}

export function normalizePrefix(prefix: string | undefined): string | undefined {
  if (!prefix) {
    return undefined;
  }
  const trimmed = prefix.trim();
  if (!trimmed) {
    return undefined;
  }
  if (!/^[A-Za-z0-9_]+$/.test(trimmed)) {
    throw new Error("databaseTablePrefix supports only letters, digits, and underscores.");
  }
  return trimmed;
}

export function randomJittered(baseSeconds: number, enabled: boolean, ratio: number): number {
  if (!enabled) {
    return baseSeconds;
  }

  const boundedRatio = clamp(ratio, 0, 1);
  const delta = baseSeconds * boundedRatio;
  const jitter = (Math.random() * (2 * delta)) - delta;
  return Math.max(0.01, baseSeconds + jitter);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
