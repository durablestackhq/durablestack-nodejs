import { readdir } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { minimatch } from "minimatch";
import type { DurableJobHandler, RetryBehavior } from "./types.js";

export interface DiscoveredJobDefinition {
  jobName: string;
  jobType?: string;
  maxAttempts?: number;
  recurring?: {
    cronExpression: string;
    timeZone: string;
    enabled?: boolean;
    allowConcurrentRuns?: boolean;
    retryBehavior?: RetryBehavior;
    retryInitialDelaySeconds?: number;
  };
  handler: DurableJobHandler;
  sourcePath?: string;
}

interface LoadAutodiscoveredJobsInput {
  baseDir: string;
  includeGlobs: string[];
  excludeGlobs: string[];
  exportName: string;
  maxModules: number;
  failOnError: boolean;
}

function toPosixPath(value: string): string {
  return value.split(sep).join("/");
}

async function walkFiles(root: string, out: string[]): Promise<void> {
  const entries = await readdir(root, { withFileTypes: true });
  for (const entry of entries) {
    const full = join(root, entry.name);
    if (entry.isDirectory()) {
      await walkFiles(full, out);
      continue;
    }
    if (entry.isFile()) {
      out.push(full);
    }
  }
}

function parseJobsExport(value: unknown, sourcePath: string): DiscoveredJobDefinition[] {
  if (!Array.isArray(value)) {
    throw new Error(`Autodiscovery export '${sourcePath}' must be an array of job definitions.`);
  }

  const definitions: DiscoveredJobDefinition[] = [];
  for (let i = 0; i < value.length; i += 1) {
    const item = value[i];
    if (!item || typeof item !== "object") {
      throw new Error(`Autodiscovery job at index ${i} in '${sourcePath}' must be an object.`);
    }
    const candidate = item as Partial<DiscoveredJobDefinition>;
    if (typeof candidate.jobName !== "string" || candidate.jobName.trim().length === 0) {
      throw new Error(`Autodiscovery job at index ${i} in '${sourcePath}' must include non-empty jobName.`);
    }
    if (typeof candidate.handler !== "function") {
      throw new Error(`Autodiscovery job '${candidate.jobName}' in '${sourcePath}' must include handler function.`);
    }

    if (candidate.recurring) {
      if (typeof candidate.recurring.cronExpression !== "string" || candidate.recurring.cronExpression.trim().length === 0) {
        throw new Error(`Autodiscovery recurring job '${candidate.jobName}' in '${sourcePath}' must include cronExpression.`);
      }
      if (typeof candidate.recurring.timeZone !== "string" || candidate.recurring.timeZone.trim().length === 0) {
        throw new Error(`Autodiscovery recurring job '${candidate.jobName}' in '${sourcePath}' must include timeZone.`);
      }
    }

    definitions.push({
      ...candidate,
      jobName: candidate.jobName.trim(),
      handler: candidate.handler
    });
  }

  return definitions;
}

export async function loadAutodiscoveredJobs(input: LoadAutodiscoveredJobsInput): Promise<DiscoveredJobDefinition[]> {
  const root = isAbsolute(input.baseDir) ? input.baseDir : resolve(process.cwd(), input.baseDir);
  const files: string[] = [];
  await walkFiles(root, files);

  const matched: string[] = [];
  for (const fullPath of files) {
    const relPosix = toPosixPath(relative(root, fullPath));
    const included = input.includeGlobs.some((pattern) => minimatch(relPosix, pattern, { dot: false }));
    if (!included) {
      continue;
    }

    const excluded = input.excludeGlobs.some((pattern) => minimatch(relPosix, pattern, { dot: false }));
    if (excluded) {
      continue;
    }

    matched.push(fullPath);
  }

  matched.sort((a, b) => a.localeCompare(b));
  if (matched.length > input.maxModules) {
    throw new Error(`Autodiscovery matched ${matched.length} modules which exceeds maxModules=${input.maxModules}.`);
  }

  const definitions: DiscoveredJobDefinition[] = [];
  for (const filePath of matched) {
    try {
      const module = await import(pathToFileURL(filePath).href);
      const exported = module[input.exportName] ?? module.default;
      if (typeof exported === "undefined") {
        continue;
      }
      const jobs = parseJobsExport(exported, filePath).map((x) => ({ ...x, sourcePath: filePath }));
      definitions.push(...jobs);
    } catch (error) {
      if (input.failOnError) {
        throw error;
      }
    }
  }

  return definitions;
}
