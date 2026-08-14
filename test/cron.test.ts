import assert from "node:assert/strict";
import test from "node:test";

import { getNextOccurrenceUtc, validateIanaTimeZone } from "../src/cron.js";

test("cron computes next minute occurrence", () => {
  const next = getNextOccurrenceUtc("*/5 * * * *", "UTC", "2026-01-01T00:02:00Z");
  assert.equal(next, "2026-01-01T00:05:00.000Z");
});

test("cron next occurrence is strictly after the given instant", () => {
  const next = getNextOccurrenceUtc("*/5 * * * *", "UTC", "2026-01-01T00:05:00Z");
  assert.equal(next, "2026-01-01T00:10:00.000Z");
});

test("cron handles sub-second timestamps without repeating the same slot", () => {
  const next = getNextOccurrenceUtc("*/5 * * * *", "UTC", "2026-01-01T00:05:00.500Z");
  assert.equal(next, "2026-01-01T00:10:00.000Z");
});

test("cron supports ranges in hour and day-of-week fields", () => {
  // Saturday 2026-08-15 → next business-hours slot is Monday 09:00.
  const next = getNextOccurrenceUtc("0 9-17 * * 1-5", "UTC", "2026-08-15T12:00:00Z");
  assert.equal(next, "2026-08-17T09:00:00.000Z");
});

test("cron supports day and month names", () => {
  const next = getNextOccurrenceUtc("0 8 * * MON-FRI", "UTC", "2026-08-15T12:00:00Z");
  assert.equal(next, "2026-08-17T08:00:00.000Z");
});

test("cron supports step on range", () => {
  const next = getNextOccurrenceUtc("0-30/10 12 * * *", "UTC", "2026-01-01T12:11:00Z");
  assert.equal(next, "2026-01-01T12:20:00.000Z");
});

test("cron treats Sunday as both 0 and 7", () => {
  const viaZero = getNextOccurrenceUtc("0 6 * * 0", "UTC", "2026-08-13T00:00:00Z");
  const viaSeven = getNextOccurrenceUtc("0 6 * * 7", "UTC", "2026-08-13T00:00:00Z");
  assert.equal(viaZero, viaSeven);
  assert.equal(viaZero, "2026-08-16T06:00:00.000Z");
});

test("cron uses OR semantics when both day-of-month and day-of-week are restricted", () => {
  // 2026-08-13 is a Thursday; "1st of month or Monday" should fire Monday 08-17, not wait for 09-01.
  const next = getNextOccurrenceUtc("0 0 1 * 1", "UTC", "2026-08-13T00:00:00Z");
  assert.equal(next, "2026-08-17T00:00:00.000Z");
});

test("cron supports six-field expressions with seconds", () => {
  const next = getNextOccurrenceUtc("*/15 * * * * *", "UTC", "2026-01-01T00:00:01Z");
  assert.equal(next, "2026-01-01T00:00:15.000Z");
});

test("cron five-field expressions fire at second zero", () => {
  const next = getNextOccurrenceUtc("30 14 * * *", "UTC", "2026-01-01T00:00:00Z");
  assert.equal(next, "2026-01-01T14:30:00.000Z");
});

test("cron respects IANA time zones across DST offsets", () => {
  // 06:30 America/New_York is 11:30Z under EST and 10:30Z under EDT.
  const winter = getNextOccurrenceUtc("30 6 * * *", "America/New_York", "2026-01-15T00:00:00Z");
  assert.equal(winter, "2026-01-15T11:30:00.000Z");
  const summer = getNextOccurrenceUtc("30 6 * * *", "America/New_York", "2026-07-15T00:00:00Z");
  assert.equal(summer, "2026-07-15T10:30:00.000Z");
});

test("cron sparse schedules compute quickly", () => {
  const started = Date.now();
  const monthly = getNextOccurrenceUtc("0 0 1 * *", "UTC", "2026-08-02T00:00:00Z");
  const yearly = getNextOccurrenceUtc("0 0 1 1 *", "UTC", "2026-01-02T00:00:00Z");
  const elapsed = Date.now() - started;
  assert.equal(monthly, "2026-09-01T00:00:00.000Z");
  assert.equal(yearly, "2027-01-01T00:00:00.000Z");
  assert.ok(elapsed < 1000, `sparse cron computation took ${elapsed}ms`);
});

test("cron rejects invalid expressions", () => {
  assert.throws(() => getNextOccurrenceUtc("not a cron", "UTC", "2026-01-01T00:00:00Z"), /Invalid cron expression/);
  assert.throws(() => getNextOccurrenceUtc("* * *", "UTC", "2026-01-01T00:00:00Z"), /Invalid cron expression/);
  assert.throws(() => getNextOccurrenceUtc("61 * * * *", "UTC", "2026-01-01T00:00:00Z"), /Invalid cron expression/);
  assert.throws(() => getNextOccurrenceUtc("* * * * 8", "UTC", "2026-01-01T00:00:00Z"), /Invalid cron expression/);
});

test("cron rejects seven-field expressions", () => {
  assert.throws(
    () => getNextOccurrenceUtc("0 0 0 1 1 * 2027", "UTC", "2026-01-01T00:00:00Z"),
    /Invalid cron expression/
  );
});

test("cron throws when no occurrence exists", () => {
  assert.throws(
    () => getNextOccurrenceUtc("0 0 30 2 *", "UTC", "2026-01-01T00:00:00Z"),
    /No cron occurrence found/
  );
});

test("validateIanaTimeZone rejects unknown zones", () => {
  assert.throws(() => validateIanaTimeZone("Not/AZone"), /Invalid IANA time zone/);
  validateIanaTimeZone("Europe/Stockholm");
  validateIanaTimeZone("UTC");
});
