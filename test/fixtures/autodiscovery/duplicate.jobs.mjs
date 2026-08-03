export const durableStackJobs = [
  {
    jobName: "duplicate-job",
    maxAttempts: 3,
    handler: async () => {
      globalThis.__autodiscoveryDuplicateHit = (globalThis.__autodiscoveryDuplicateHit || 0) + 1;
    }
  }
];
