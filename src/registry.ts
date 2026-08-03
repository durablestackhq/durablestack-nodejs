import type { DurableJobRegistration } from "./types.js";

export class DurableJobRegistry {
  private readonly byName = new Map<string, DurableJobRegistration>();

  register(registration: DurableJobRegistration): void {
    const jobName = registration.jobName.trim();
    if (!jobName) {
      throw new Error("jobName is required");
    }
    if (this.byName.has(jobName)) {
      throw new Error(`A job with name '${jobName}' is already registered.`);
    }

    this.byName.set(jobName, {
      ...registration,
      jobName,
      maxAttempts: Math.max(1, Math.floor(registration.maxAttempts || 3))
    });
  }

  get(jobName: string): DurableJobRegistration | undefined {
    return this.byName.get(jobName);
  }

  getAll(): DurableJobRegistration[] {
    return Array.from(this.byName.values());
  }
}
