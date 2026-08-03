export const durableStackJobs = [
  {
    jobName: "auto-valid",
    maxAttempts: 3,
    handler: async () => {
      globalThis.__autodiscoveryHit = (globalThis.__autodiscoveryHit || 0) + 1;
    }
  }
];
