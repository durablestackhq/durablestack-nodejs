import { toDate } from "./utils.js";

interface CronFields {
  second: number[];
  minute: number[];
  hour: number[];
  dayOfMonth: number[];
  month: number[];
  dayOfWeek: number[];
}

function parsePart(part: string, min: number, max: number): number[] {
  const trimmed = part.trim();
  if (trimmed === "*") {
    const values: number[] = [];
    for (let i = min; i <= max; i += 1) {
      values.push(i);
    }
    return values;
  }

  if (trimmed.startsWith("*/")) {
    const step = Number.parseInt(trimmed.slice(2), 10);
    if (!Number.isFinite(step) || step <= 0) {
      throw new Error(`Invalid cron step: ${trimmed}`);
    }
    const values: number[] = [];
    for (let i = min; i <= max; i += step) {
      values.push(i);
    }
    return values;
  }

  const values = trimmed.split(",").map((v) => Number.parseInt(v, 10));
  if (values.some((v) => !Number.isFinite(v) || v < min || v > max)) {
    throw new Error(`Invalid cron field '${trimmed}'`);
  }

  return Array.from(new Set(values)).sort((a, b) => a - b);
}

function parseCron(cronExpression: string): CronFields {
  const fields = cronExpression.trim().split(/\s+/);
  if (fields.length !== 5 && fields.length !== 6) {
    throw new Error("Cron expression must have 5 or 6 fields");
  }

  const withSeconds = fields.length === 6;
  const second = withSeconds ? parsePart(fields[0]!, 0, 59) : [0];
  const minute = parsePart(fields[withSeconds ? 1 : 0]!, 0, 59);
  const hour = parsePart(fields[withSeconds ? 2 : 1]!, 0, 23);
  const dayOfMonth = parsePart(fields[withSeconds ? 3 : 2]!, 1, 31);
  const month = parsePart(fields[withSeconds ? 4 : 3]!, 1, 12);
  const dayOfWeek = parsePart(fields[withSeconds ? 5 : 4]!, 0, 6);

  return { second, minute, hour, dayOfMonth, month, dayOfWeek };
}

function toTimeZoneParts(date: Date, timeZone: string): Record<string, number> {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    weekday: "short"
  });

  const parts = formatter.formatToParts(date);
  const map: Record<string, number> = {};

  for (const part of parts) {
    if (part.type === "literal") {
      continue;
    }

    if (part.type === "weekday") {
      const weekdayMap: Record<string, number> = {
        Sun: 0,
        Mon: 1,
        Tue: 2,
        Wed: 3,
        Thu: 4,
        Fri: 5,
        Sat: 6
      };
      map.weekday = weekdayMap[part.value] ?? 0;
      continue;
    }

    map[part.type] = Number.parseInt(part.value, 10);
  }

  return map;
}

function matches(date: Date, cron: CronFields, timeZone: string): boolean {
  const parts = toTimeZoneParts(date, timeZone);
  return (
    cron.second.includes(parts.second ?? 0) &&
    cron.minute.includes(parts.minute ?? 0) &&
    cron.hour.includes(parts.hour ?? 0) &&
    cron.dayOfMonth.includes(parts.day ?? 0) &&
    cron.month.includes(parts.month ?? 0) &&
    cron.dayOfWeek.includes(parts.weekday ?? 0)
  );
}

export function validateIanaTimeZone(timeZone: string): void {
  const trimmed = timeZone.trim();
  try {
    const formatter = new Intl.DateTimeFormat("en-US", { timeZone: trimmed });
    formatter.format(new Date());
  } catch {
    throw new Error(`Invalid IANA time zone: ${timeZone}`);
  }
}

export function getNextOccurrenceUtc(cronExpression: string, timeZone: string, afterUtcIso: string): string {
  validateIanaTimeZone(timeZone);
  const cron = parseCron(cronExpression);
  let cursor = toDate(afterUtcIso);
  cursor = new Date(cursor.getTime() + 1000);

  for (let i = 0; i < 370 * 24 * 60 * 60; i += 1) {
    if (matches(cursor, cron, timeZone)) {
      return cursor.toISOString();
    }
    cursor = new Date(cursor.getTime() + 1000);
  }

  throw new Error("No cron occurrence found within search window");
}
