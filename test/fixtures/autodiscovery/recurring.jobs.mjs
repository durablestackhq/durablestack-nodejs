export const durableStackJobs = [
  {
    jobName: "auto-recurring",
    maxAttempts: 3,
    recurring: {
      cronExpression: "*/1 * * * * *",
      timeZone: "UTC",
      enabled: true,
      allowConcurrentRuns: false
    },
    handler: async () => {
      globalThis.__autodiscoveryRecurringHit = (globalThis.__autodiscoveryRecurringHit || 0) + 1;
    }
  }
];
