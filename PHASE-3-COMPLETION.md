# Phase 3 Completion - Observability and Runtime-Control

## Status

Phase 3 is complete.

## Completed Scope

1. Hosted observability building blocks
   - ingestion event sink queue,
   - ingestion sync service with contract-compatible payload mapping,
   - runtime-control sync service with command processing.

2. Runtime lifecycle integration
   - hosted ingestion/runtime-control services auto-start and stop with runtime lifecycle when tenant credentials are configured.

3. Transport hardening
   - bounded retries for transient transport failures (`408`, `409`, `425`, `429`, `5xx`),
   - no retry for unauthorized responses (`401`, `403`),
   - error-safe behavior for transport exceptions to avoid unhandled runtime failures.

4. Contract and behavior tests
   - ingestion request headers/payload/runtime metadata checks,
   - runtime-control command execution and receipt lifecycle checks,
   - transient retry and unauthorized no-retry checks for ingestion and runtime-control,
   - command expiration handling and receipt upload progression checks.

## Acceptance Criteria Check

- Payload contract tests pass against local stubs: complete.
- End-to-end runtime-control command flow validated in tests: complete.
- Runtime-host lifecycle wiring for hosted services validated: complete.

## Release Readiness Notes

- CI now captures and uploads Postgres-enabled test output artifacts on failure for faster diagnostics.
- Postgres recurring race contract assertions are hardened around slot uniqueness invariants.
- Local checks pass (`npm test`, `npm run typecheck`), with Postgres tests env-gated by `DURABLESTACK_TEST_POSTGRES`.

## Next Actions For Phase 4 Prep

1. Implement autodiscovery option schema and normalization.
2. Build module discovery loader with include/exclude filters and duplicate-name detection.
3. Add autodiscovery unit/integration tests using the scoped plan in `PHASE-4-AUTODISCOVERY-PLAN.md`.
