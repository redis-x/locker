# Remaining issues in @redis-x/locker

Reviewed October 10, 2026, against the current working tree. This file contains unresolved findings only. Delete an entry after its fix and regression checks pass; do not mark it completed or renumber the remaining entries. Keep IDs stable and do not reuse removed IDs. Next ID: **L11**.

**P1** means a locking correctness problem that should be fixed first. **P2** means a reliability, compatibility, or test infrastructure problem.

Current validation: lint, TypeScript, build, and publint checks pass. All 58 tests pass under Bun and Vitest in a temporary copy whose fixture points to an isolated Redis 7.4.5 instance. The built ESM and CommonJS entry points pass acquisition, extension, and release smoke tests on Node.js 16.0.0, and reject Redis 7.2 without writing lock data. The existing multi-lock test file covers atomic acquisition, owner checks, fresh TTLs after retries and request delays, client clock changes, additive extension, and rollback after expiration errors.

## L03 — P1: Acquisition can return a lock whose lease already expired

Location: `src/locker/single.ts:60–66`, `src/locker/multi.ts:79–113`; review the extension success paths too.

Both implementations return success solely from the Redis reply, without accounting for elapsed time. A delayed response can arrive after the lease has expired, so the caller begins its critical section without a live lock.

Reproduction for both implementations: execute the real acquisition with a 40 ms TTL and delay delivery of its successful reply by 100 ms. `lock()` still resolves, and a competing acquisition immediately succeeds.

Track a conservative lease deadline using a monotonic clock, reject acknowledgements that arrive after it, and perform ownership-safe cleanup when needed. Apply equivalent validity checks to extension and document how callers must keep work within the remaining lease. Add delayed-response regression tests. The [Redis locking guidance](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/) describes deducting acquisition time from lock validity.

## L08 — P2: Integration tests depend on order and share destructive cleanup

Location: `src/locker/single.test.ts`, `src/locker/multi.test.ts`, `test/redis.ts:3–10`, `package.json:40–42`.

Single-lock tests inherit locks and handles from earlier tests. Running `bun test src/locker/single.test.ts --test-name-pattern '^failed lock$'` fails because its prerequisite lock was created by another test. Delayed successful replies still lack regression coverage. Multi-lock tests now establish and clean up their own namespaces, but the single-lock suite still depends on test order.

The fixture also hardcodes port 16379 and calls `FLUSHDB` on import; the single-lock extension test flushes the database again. `redis:up` reuses a globally named container, so concurrent runs or sibling packages using that endpoint can erase one another's locks. The fixture does not close its client.

Make every test establish and clean up its own state, provide a configurable isolated Redis endpoint, avoid flushing a reused database, and close connections in teardown. Add the regression cases from the other entries and verify filtered tests and concurrent independent suite runs.
