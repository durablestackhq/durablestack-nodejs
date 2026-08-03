# Phase 4 Plan - Job Autodiscovery

## Status

Completed for runtime discovery mode; build-time manifest mode deferred.

## Scope

Implement opt-in job autodiscovery for Node.js runtime users who want convention-based registration in addition to explicit `registerJob` and `registerRecurring` calls.

## Goals

- Preserve the current explicit-registration model as the default behavior.
- Add an opt-in autodiscovery mechanism with predictable startup cost.
- Keep discovered job metadata and runtime behavior aligned with existing lifecycle, lease, retry, and recurring semantics.

## Non-Goals

- No cross-runtime worker interoperability.
- No implicit filesystem scanning by default.
- No framework-specific magic wiring in this phase.
- No replacement of explicit APIs; autodiscovery supplements them.

## Proposed Modes

1. Runtime discovery mode (primary for Phase 4)
   - User provides one or more include globs plus optional exclude globs.
   - Runtime imports matching modules at startup and registers discovered jobs.

2. Build-time manifest mode (deferred within Phase 4 unless needed)
   - Optional generated manifest file consumed at runtime to avoid broad scans in production.
   - Consider this if runtime-scan startup cost is too high in real projects.

## Discovery Contract

- A discoverable module exports one or more job definitions in a documented shape.
- Each discovered job maps to the same internal registration structures used by explicit registration.
- Duplicate job names fail fast with a clear startup error.
- Invalid module exports are surfaced with actionable error messages including module path.

## Runtime Options (Draft)

- `autodiscovery.enabled: boolean` (default `false`)
- `autodiscovery.includeGlobs: string[]`
- `autodiscovery.excludeGlobs?: string[]`
- `autodiscovery.failOnError?: boolean` (default `true`)
- `autodiscovery.maxModules?: number` (safety guard)

## Acceptance Criteria

1. Feature behavior
   - With autodiscovery disabled, runtime behavior is unchanged.
   - With autodiscovery enabled, matching modules are discovered and registered deterministically.
   - Duplicate job names are rejected before worker loop starts.

2. Reliability and safety
   - Discovery errors include module path and reason.
   - Exclude patterns are honored.
   - Startup does not proceed into processing when `failOnError=true` and discovery fails.

3. Contract compatibility
   - Discovered jobs execute with existing lifecycle/lease/retry semantics.
   - Recurring discovered jobs preserve existing slot uniqueness and schedule admin semantics.

4. Testing
   - Unit tests for module shape validation and duplicate detection.
   - Integration tests for include/exclude behavior and startup failure modes.
   - Existing explicit-registration tests remain green.

## Implementation Work Breakdown

1. Add autodiscovery option model and normalization.
2. Add module loader/discovery component with include/exclude filtering.
3. Add discovered-job schema validation and mapping.
4. Wire discovery into runtime startup before processor loop begins.
5. Add tests (unit + integration) and docs examples.

## Risks and Mitigations

- Startup latency from broad globs.
  - Mitigation: require explicit include globs, add `maxModules` guard, document production guidance.
- Ambiguous export conventions.
  - Mitigation: keep a single canonical export shape for Phase 4 and validate strictly.
- Duplicate names across files.
  - Mitigation: fail-fast duplicate detection with file-origin reporting.

## Definition of Done

- Autodiscovery is opt-in, documented, and covered by tests.
- Explicit registration remains fully supported and unchanged.
- CI passes with autodiscovery tests enabled.
