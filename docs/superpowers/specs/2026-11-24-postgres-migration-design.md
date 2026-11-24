# Postgres migration — design

**Date:** 2026-11-24
**Supersedes:** the "Why SQLite, and not Valkey or Postgres" section of `docs/plan.md`

## Intent

Replace SQLite with Postgres, using `jackc/pgx/v5`. Blobs stay on local disk.

**Why (my reading, stated so it can be corrected):** the audit measured two
problems that both trace to SQLite's single global write lock.

- A hot key sheds ~1% of requests as 503 at only 64 concurrent clients. The
  cause is not saturation — the lock is held ~4 ms — but SQLite's busy
  handler, which retries with backoff and has no queue, so unlucky writers
  starve.
- A sweeper draining a backlog collapses throughput ~18× (1837 → 99 PUTs in
  6 s) and pushes p99 from 117 ms to 3.37 s, because the reclaim loop
  re-acquires the one write lock 7,454 times a second.

Postgres removes both structurally: row-level locks mean a sweeper deleting
hash A and a client writing hash B never contend at all.

## Goals

- Writes to different keys do not block each other.
- A sweeper drain leaves request latency essentially unchanged.
- Every correctness property the SQLite version had is preserved, and proven
  by a test that fails without the fix.

## Non-goals

- **Multi-instance.** Blobs are on local disk, so a second instance on
  another machine still cannot see them. This migration does not deliver
  horizontal scale and must not be described as if it does.
- Shared blob storage (S3/NFS). That is the larger, separate change.
- Keeping SQLite as an option.

## Anti-goals

- A database abstraction layer supporting both engines. It would mean the
  test suite no longer exercises the real locking semantics, which is
  precisely where the risk of this port lives.
- Any behaviour change visible to an HTTP client. Same status codes, same
  ETags, same precondition semantics.

## Constraints

- `CGO_ENABLED=0` still holds — pgx is pure Go.
- Tests require a live Postgres. There is no honest substitute.
- The data directory, blob layout, sweeper design, trash and orphan walk
  are unchanged. Only the metadata store moves.

## The part that is not a transcription

`docs/plan.md` claims "the port is a DSN and a driver." That is wrong in one
place, and it is the dangerous one.

**Today's PUT relies on a global lock for atomicity.** It does:

```
SELECT hash FROM objects WHERE bucket=? AND object_id=?   -- precondition + 201/200
...
INSERT INTO objects ... ON CONFLICT DO UPDATE SET hash=excluded.hash
```

`_txlock=immediate` makes that whole transaction mutually exclusive with
every other writer, so the read and the write cannot be separated. Postgres
has no such lock. Under READ COMMITTED (the default, confirmed on the target
server) those two statements see different snapshots, so:

- **`If-Match` regains the lost-update race.** Two writers both read hash
  `h`, both pass the check, both write. One silently overwrites the other.
- **`If-None-Match: *` stops being create-only.** Two writers both see the
  row absent, both pass, and `ON CONFLICT DO UPDATE` lets the second
  overwrite the first — both get 201.

`SELECT ... FOR UPDATE` fixes the first but not the second: there is no row
to lock when the object does not exist yet.

**Decision: `pg_advisory_xact_lock` keyed on (bucket, object_id), taken as
the first statement of every write transaction.** It restores exactly the
invariant SQLite gave — writers on one key serialize — but per key instead
of globally, which is the entire point of the move. It is uniform across
every precondition case, including create-only, and needs no conditional-SQL
cleverness.

`docs/plan.md` rejected advisory locks for the *blob* race on the grounds
that "nothing enforces it — a new code path that forgets the lock compiles
fine." That objection stands and is accepted: it is mitigated, not
dismissed, by there being exactly two write paths (`Put`, `Delete`) and by a
concurrency test that fails if the lock is dropped. The blob race keeps the
foreign key, as originally designed — the schema still enforces that one.

## Verified against the target server

Postgres 17.12, `default_transaction_isolation = read committed`:

- `DELETE FROM blobs ... AND NOT EXISTS (SELECT 1 FROM objects ...)` affects
  **0 rows** while a child row exists. The sweeper's guard works unchanged.
- An unguarded delete of a referenced parent raises **SQLSTATE 23503**. So
  if `reclaimOne`'s `NOT EXISTS` ever reads a stale snapshot, the result is
  a loud error, not a silently orphaned row. **23503 must be handled as a
  normal "adopted by a concurrent PUT, skip it" outcome**, alongside the
  existing zero-rows-affected case.

## Approaches considered

**A. `database/sql` + the `pgx/v5/stdlib` adapter.** Smallest diff: swap the
driver, rewrite placeholders, done. Rejected because the user asked to
reorganize, and the adapter hides pgx's error codes (`*pgconn.PgError`)
behind `database/sql`, which is exactly what `statusFor` needs to classify
503-worthy failures.

**B. Native `pgx/v5` with `pgxpool`. Chosen.** Idiomatic for a Postgres-only
service, gives direct access to SQLSTATE codes for 23503 and the retryable
classes, and `pgxpool` replaces the hand-set `SetMaxOpenConns(10)` with a
real pool. Costs a wider diff: every call site changes shape
(`QueryRowContext(ctx, …)` → `QueryRow(ctx, …)`, `sql.ErrNoRows` →
`pgx.ErrNoRows`, `RowsAffected()` loses its error return).

**C. Keep SQLite, add Postgres behind an interface.** Rejected as an
anti-goal above.

## What changes

| Area | Change |
|---|---|
| Driver | `modernc.org/sqlite` → `jackc/pgx/v5` + `pgxpool`. One dependency swapped. |
| Schema | Drop `STRICT`. Tables, keys, FK and index otherwise identical. |
| Placeholders | 23 × `?` → `$1..$N`. |
| Transactions | `_txlock=immediate` → `pg_advisory_xact_lock` per object key in `Put` and `Delete`. |
| Sweeper | `reclaimOne` additionally treats SQLSTATE 23503 as "skip". |
| Errors | `sqlite3.SQLITE_BUSY/LOCKED` → `*pgconn.PgError` 40001, 55P03, 57014 and connection failures → 503. |
| Config | `-data` keeps blobs; new `-database-url`. `meta.db` and its pragmas disappear. |
| Pool | `SetMaxOpenConns(10)` → `pgxpool` `MaxConns`. |
| Deployment | Needs a Postgres. Still one replica, because of the blobs. |

**Unchanged:** blob layout and fanout, content addressing, dedup, crash-safety
ordering, `renameDurable`/`mkdirAllSync`, trash and retention, the size cap,
the orphan walk and its empty-metadata guard, validation, all HTTP behaviour,
every metric name.

## Tests

Each test gets its own Postgres **schema** (`CREATE SCHEMA`, set
`search_path`), so the suite stays parallel-safe and fast. Connection comes
from `OBJECTSTORE_TEST_DATABASE_URL`; when unset the suite **fails** with a
clear message rather than skipping, because a silently-skipped concurrency
suite is how this port would ship broken.

Three new tests carry the risk of this change, each required to fail when its
fix is removed:

1. **Lost update under concurrency.** N writers read one ETag and all PUT
   with `If-Match`. Exactly one 2xx. Fails without the advisory lock.
2. **Create-only under concurrency.** N writers PUT the same new path with
   `If-None-Match: *`. Exactly one 201, the rest 412. Fails without the
   advisory lock.
3. **The sweeper no longer starves writers.** The audit's measurement, rerun:
   PUT throughput and p99 during a large drain must stay close to baseline.
   This is the test that proves the migration was worth doing.

Everything else is the existing suite, ported. `TestTxLockIsImmediate` is
deleted (no equivalent); `TestForeignKeysOnEveryPooledConnection` becomes a
plain FK-liveness check, since Postgres cannot turn foreign keys off.

## Left to the builder

- Exact pool sizing and timeouts.
- Whether schema creation runs on every boot (as today) or once; today's
  `CREATE TABLE IF NOT EXISTS` on startup is fine to keep.
- A `compose.yaml` for local development, if it helps.
