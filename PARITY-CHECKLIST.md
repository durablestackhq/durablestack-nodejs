# Node.js Runtime Parity Checklist

## Purpose

Track parity status between DurableStack .NET semantics and the Node.js runtime implementation for externally visible contracts and durable behavior.

## Canonical Areas

1. Run lifecycle semantics (`pending` -> `leased` -> terminal)
   - Status: aligned.
   - Coverage: in-memory and Postgres contract tests validate claim, stale completion fencing, retry transition, reclaim after lease expiry, and terminal transitions.

2. Lease ownership and fencing
   - Status: aligned.
   - Coverage: stale owner completion writes are rejected; lease reclaim increments attempt and changes owner.

3. Retry behavior and max-attempt boundaries
   - Status: aligned for baseline behavior.
   - Coverage: retry path transitions leased run back to pending and reclaims as next attempt.

4. Recurring schedule materialization and slot uniqueness
   - Status: aligned on invariant semantics.
   - Coverage: duplicate slot materialization is blocked under race; contention-safe test assertions used for Postgres CI.

5. Retention pruning guarantees
   - Status: aligned.
   - Coverage: pruning removes terminal runs only and preserves active states.

6. Runtime-control command receipt lifecycle
   - Status: aligned for lease/ack/success/failure/upload flow.
   - Coverage: lease contention single-winner checks; sync flow tests validate command processing and receipt transitions.

7. Event ingestion contract and runtime-control transport behavior
   - Status: aligned for payload shape and bounded retry behavior.
   - Coverage: tests validate headers/payload/runtime fields, transient retry, and no-retry for unauthorized responses.

## Intentional Differences / Current Constraints

- Cross-runtime worker interoperability is intentionally out of scope (Node workers run Node handlers).
- Job autodiscovery remains deferred and opt-in for Phase 4; explicit registration is the current default.
- Provider parity beyond Postgres (MySQL/SQL Server/SQLite) is deferred to the provider parity phase.

## Follow-up Test Focus Added

- Guard rails for deduplicated active-run enqueue behavior (`tryEnqueueIfNoActiveRun`).
- Runtime-control sync behavior for expired commands and receipt upload progression across sync cycles.
