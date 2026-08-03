# Phase 4 Progress - Job Autodiscovery

## Status

In progress.

## Completed So Far

1. Option model and normalization
   - Added `autodiscovery` options on runtime config:
     - `enabled`, `includeGlobs`, `excludeGlobs`, `failOnError`, `maxModules`, `baseDir`, `exportName`.

2. Runtime discovery loader
   - Added module discovery/loader with recursive file scan under `baseDir`.
   - Added include/exclude glob filtering with deterministic ordering.
   - Added strict export validation for discoverable job definitions.
   - Added safety guard for `maxModules`.

3. Runtime startup wiring
   - Autodiscovery runs before recurring initialization and worker loop start.
   - Fail-fast behavior when `failOnError=true`.
   - Continue-on-error behavior when `failOnError=false`.

4. Test coverage added
   - discovered one-off job registration and execution,
   - include/exclude filtering behavior,
   - fail-on-error startup rejection,
   - recurring autodiscovered job materialization/execution,
   - duplicate-name conflict (explicit + discovered),
   - continue-on-error behavior with mixed valid/invalid modules,
   - `maxModules` startup guard.

## Remaining For Phase 4 Completion

1. Add focused docs guidance for production-safe patterns
   - narrow include globs,
   - recommended exclude patterns,
   - deterministic naming conventions.

2. Add CI confirmation pass and monitor Postgres env-gated stability with new test load.

3. Decide whether build-time manifest mode is required for this phase or explicitly deferred.
