import { Cron } from "croner";
import { toDate } from "./utils.js";

export function validateIanaTimeZone(timeZone: string): void {
  const trimmed = timeZone.trim();
  try {
    const formatter = new Intl.DateTimeFormat("en-US", { timeZone: trimmed });
    formatter.format(new Date());
  } catch {
    throw new Error(`Invalid IANA time zone: ${timeZone}`);
  }
}

function parsePattern(cronExpression: string, timeZone: string): Cron {
  try {
    return new Cron(cronExpression, {
      timezone: timeZone.trim(),
      mode: "5-or-6-parts"
    });
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`Invalid cron expression '${cronExpression}': ${detail}`);
  }
}

export function getNextOccurrenceUtc(cronExpression: string, timeZone: string, afterUtcIso: string): string {
  validateIanaTimeZone(timeZone);
  const pattern = parsePattern(cronExpression, timeZone);
  const next = pattern.nextRun(toDate(afterUtcIso));
  if (!next) {
    throw new Error("No cron occurrence found within search window");
  }
  return next.toISOString();
}
