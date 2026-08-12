export const durableStackJobs = [
  {
    jobName: "auto-excluded",
    maxAttempts: 3,
    handler: async () => {
      globalThis.__autodiscoveryExcludedHit = (globalThis.__autodiscoveryExcludedHit || 0) + 1;
    }
  }
];
