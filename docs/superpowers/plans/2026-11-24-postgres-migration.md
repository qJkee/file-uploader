# Postgres Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SQLite with Postgres via `jackc/pgx/v5`, so writes to different keys stop blocking each other and a sweeper drain stops starving the request path.

**Architecture:** Unchanged except the metadata store. Blobs stay content-addressed on local disk; `pgxpool` replaces `database/sql`+`modernc.org/sqlite`; the global-write-lock guarantee SQLite gave is replaced by `pg_advisory_xact_lock` per object key, and the blob race keeps the foreign key.

**Tech Stack:** Go, `github.com/jackc/pgx/v5` (+ `pgxpool`, `pgconn`), `github.com/prometheus/client_golang`.

**Spec:** `docs/superpowers/specs/2026-11-24-postgres-migration-design.md`

## Global Constraints

- Flat `package main`. Dependencies become exactly `jackc/pgx/v5` and `prometheus/client_golang`; `modernc.org/sqlite` is removed from `go.mod`.
- `CGO_ENABLED=0` must still build.
- **No HTTP-visible behaviour changes.** Same status codes, same ETag format, same precondition semantics, same metric names.
- No database abstraction layer. Postgres only.
- Tests connect via `OBJECTSTORE_TEST_DATABASE_URL`. When it is unset the suite **fails with a clear message** — never skips.
- Each test gets its own Postgres schema and drops it on cleanup.
- Blob layout, dedup, crash-safety ordering, trash, orphan walk and validation are untouched.
- Every task ends green under `go test -race ./...`.

## Review Focus

1. **A `?` placeholder left behind** — pgx rejects them, but only on the path that runs, so a rarely-hit query can break in production while the suite is green. Grep for `?` in SQL strings at the end. *(Task 1)*
2. **A write path that forgets the advisory lock** — the design accepts this risk explicitly. Both `Put` and `Delete` must take it, and the concurrency tests must fail if either is removed. *(Task 2)*
3. **`pgx.ErrNoRows` vs `sql.ErrNoRows`** — a missed conversion turns "object not found" into a 500 instead of a 404. *(Task 1)*
4. **`RowsAffected()` loses its error return** — a mechanical conversion that silently changes control flow in `reclaimOne` if mishandled. *(Task 3)*
5. **Connection loss mid-request** — pgx returns `*pgconn.PgError` or a network error; neither must become a generic 500 when it is retryable. *(Task 4)*

---

### Task 1: The mechanical port — driver, schema, test harness

The package does not compile part-ported, so this task moves everything at once and is gated on the existing suite passing. Correctness gaps Postgres introduces are Tasks 2–4.

**Files:**
- Modify: `go.mod`, `store.go`, `sweeper.go`, `handler.go`, `main.go`, and every `*_test.go`
- Create: `compose.yaml`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```go
  type Config struct {
      DataDir        string
      DatabaseURL    string
      MaxObjectSize  int64
      SweepInterval  time.Duration
      SweepBudget    time.Duration
      TmpMaxAge      time.Duration
      TrashRetention time.Duration
      MaxTrashBytes  int64
  }
  func Open(ctx context.Context, cfg Config) (*Store, error)
  func (s *Store) Close()                  // pgxpool.Close returns nothing
  func (s *Store) Pool() *pgxpool.Pool     // replaces DB()
  ```
  `Store.db *sql.DB` becomes `Store.pool *pgxpool.Pool`. All three test hooks survive unchanged.

- [ ] **Step 1: Add `compose.yaml` and confirm the dev database**

A single `postgres:17-alpine` service on port 15432, user/password/db all `objectstore`.

Run: `podman ps --filter name=objstore-pg --format '{{.Status}}'`
Expected: a running container (one is already up from the design phase).

- [ ] **Step 2: Swap the dependency**

```bash
go get github.com/jackc/pgx/v5
go mod edit -droprequire modernc.org/sqlite
```

Run: `go mod tidy && grep -c sqlite go.mod`
Expected: `0`.

- [ ] **Step 3: Write the test harness in `store_test.go`**

```go
// testDatabaseURL fails the run when OBJECTSTORE_TEST_DATABASE_URL is unset:
// a silently skipped concurrency suite is how this port ships broken.
func testDatabaseURL(t *testing.T) string

// newTestStore creates a private schema, points search_path at it, opens a
// Store on a fresh temp dir, and drops the schema on cleanup.
func newTestStore(t *testing.T) *Store
```

Schema names are `test_<random>`. pgx puts unrecognised URL query parameters into `RuntimeParams`, so `?search_path=test_x` scopes every connection in the pool — assert this works before relying on it.

- [ ] **Step 4: Write the failing harness test**

```go
func TestSchemaIsolation(t *testing.T)
// two stores from newTestStore do not see each other's objects rows
func TestSearchPathAppliesToEveryPooledConnection(t *testing.T)
// 16 concurrent queries all report the same current_schema()
```

- [ ] **Step 5: Run to verify they fail**

Run: `OBJECTSTORE_TEST_DATABASE_URL='postgres://objectstore:objectstore@localhost:15432/objectstore' go test ./... -run 'TestSchemaIsolation|TestSearchPath' -v`
Expected: FAIL — build errors, `undefined: pgxpool`.

- [ ] **Step 6: Port `store.go`**

`Open` parses the URL with `pgxpool.ParseConfig`, sets `MaxConns` to 10, calls `pgxpool.NewWithConfig`, `Ping`s, then execs the schema. Schema is the current DDL with `STRICT` removed — tables, primary keys, the composite foreign key and `objects_by_hash` are otherwise identical.

Mechanical conversions throughout: `QueryRowContext(ctx, …)` → `QueryRow(ctx, …)`; `ExecContext` → `Exec`; `BeginTx(ctx, nil)` → `Begin(ctx)`; `tx.Rollback()` → `tx.Rollback(ctx)`; `tx.Commit()` → `tx.Commit(ctx)`; `sql.ErrNoRows` → `pgx.ErrNoRows`; `result.RowsAffected()` drops its error return; all `?` become `$1..$N`.

Delete `openDB`, the pragma DSN, `maxOpenConns` and `checkSameDevice`'s unchanged neighbours stay.

- [ ] **Step 7: Port `sweeper.go`, `handler.go`, `main.go` and the tests**

`handler.go` loses its `modernc.org/sqlite` imports; its busy-error branch is replaced in Task 4, so for now map unknown errors to 500. `main.go` gains `-database-url` (env `OBJECTSTORE_DATABASE_URL`), loses nothing else, and `Open` now takes a context.

Delete `TestTxLockIsImmediate` — it tested a SQLite DSN parameter with no equivalent. Reduce `TestForeignKeysOnEveryPooledConnection` to a plain FK-liveness test, since Postgres cannot disable foreign keys.

- [ ] **Step 8: Run the whole suite**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test -race ./... 2>&1 | tail -30`
Expected: PASS. Concurrency tests may still pass here by luck — Task 2 is what pins them.

- [ ] **Step 9: Grep for leftovers**

Run: `grep -rn 'sqlite\|meta\.db\|busy_timeout\|_txlock\|_pragma' --include='*.go' . | grep -v '^./docs'`
Expected: no output.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor: move metadata from SQLite to Postgres via pgx"
```

---

### Task 2: Restore write atomicity with a per-key advisory lock

**Files:**
- Modify: `store.go`, `regression_test.go`

**Interfaces:**
- Consumes: Task 1's `Store`, `Put`, `Delete`.
- Produces: `func lockObjectKey(ctx context.Context, tx pgx.Tx, bucket, objectID string) error`

- [ ] **Step 1: Write the failing tests**

```go
func TestConcurrentIfMatchPicksOneWinner(t *testing.T)
// 16 writers read one ETag, all PUT with If-Match. Exactly one 2xx, 15 x 412,
// and GET returns the winner's bytes.
func TestConcurrentCreateOnlyPicksOneWinner(t *testing.T)
// 16 writers PUT the same absent path with If-None-Match: *. Exactly one 201,
// 15 x 412, and the stored body is the winner's.
```

Both run 16 writers rather than the existing suite's 2, because two writers under READ COMMITTED frequently serialise by accident and would hide the bug.

- [ ] **Step 2: Run to verify they fail**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test -race ./... -run 'TestConcurrent(IfMatch|CreateOnly)' -count=3 -v`
Expected: FAIL — more than one winner, because the precondition read and the write sit in different snapshots.

- [ ] **Step 3: Implement the lock**

```sql
SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))
```

Taken as the **first statement** of the transaction in both `Put` and `Delete`, before the precondition `SELECT`. The two-integer form keyed on bucket and object ID separately keeps collisions rarer than hashing a concatenation; a collision only costs extra blocking.

Comment it with why it exists: it restores per-key what `_txlock=immediate` gave globally, and `SELECT … FOR UPDATE` cannot replace it because a create-only PUT has no row to lock.

- [ ] **Step 4: Run to verify they pass**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test -race ./... -run 'TestConcurrent(IfMatch|CreateOnly)' -count=3 -v`
Expected: PASS on all three runs.

- [ ] **Step 5: Prove the tests discriminate**

Comment out the lock in `Put`, rerun Step 4, confirm failure, restore it.
Expected: FAIL with multiple winners, then PASS again once restored.

- [ ] **Step 6: Commit**

```bash
git add store.go regression_test.go
git commit -m "fix: serialise writers per object key with an advisory lock"
```

---

### Task 3: Treat a foreign-key violation as a lost race

**Files:**
- Modify: `sweeper.go`, `sweeper_test.go`

**Interfaces:**
- Consumes: Task 1's `reclaimOne`, `reclaimOrphan`.
- Produces: `func isForeignKeyViolation(err error) bool` (SQLSTATE `23503`).

- [ ] **Step 1: Write the failing test**

```go
func TestReclaimSkipsAdoptedBlob(t *testing.T)
// With a blobs row whose objects child is created between the candidate
// query and the delete, reclaimOne returns (false, nil) — not an error, and
// the file stays in blobs/.
```

Drive the race with the existing `hookOrphanWindow` pattern: add the child row from the hook.

- [ ] **Step 2: Run to verify it fails**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test ./... -run TestReclaimSkipsAdoptedBlob -v`
Expected: FAIL with a `23503` foreign-key violation surfacing as a sweep error.

- [ ] **Step 3: Implement**

In `reclaimOne` and `reclaimOrphan`, `errors.As` to `*pgconn.PgError` and treat `23503` exactly like zero rows affected: the blob was adopted, leave the file alone, return `(false, nil)`. Verified on Postgres 17.12 that an unguarded delete of a referenced parent raises this code.

- [ ] **Step 4: Run to verify it passes**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test -race ./... -run 'TestReclaim|TestSweep|TestOrphan' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sweeper.go sweeper_test.go
git commit -m "fix: treat a foreign-key violation in reclaim as an adopted blob"
```

---

### Task 4: Map Postgres failures to the right status codes

**Files:**
- Modify: `handler.go`, `handler_test.go`

**Interfaces:**
- Consumes: Task 1's `statusFor`.
- Produces: `statusFor` handling `*pgconn.PgError`.

- [ ] **Step 1: Write the failing test**

```go
func TestStatusForPostgresErrors(t *testing.T)
// &pgconn.PgError{Code:"40001"} -> 503   (serialization_failure)
// &pgconn.PgError{Code:"55P03"} -> 503   (lock_not_available)
// &pgconn.PgError{Code:"57014"} -> 503   (query_canceled)
// &pgconn.PgError{Code:"23505"} -> 500   (anything else stays 500)
```

Unlike the SQLite driver's error type, `pgconn.PgError` is constructible, so the branch the old code could only document is now testable. Delete the `shortcut:` comment that said otherwise.

- [ ] **Step 2: Run to verify it fails**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test ./... -run TestStatusForPostgresErrors -v`
Expected: FAIL — all four return 500.

- [ ] **Step 3: Implement**

Add a `*pgconn.PgError` branch to `statusFor` returning 503 with `Retry-After` for the three retryable codes. Keep the existing ordering: `ErrInvalidName`, `ErrPrecondition`, `ErrNotFound`, `ErrBlobMissing`, `MaxBytesError`, `ENOSPC`, client-gone, then Postgres, then 500.

- [ ] **Step 4: Run the whole suite**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test -race ./... 2>&1 | tail -20`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add handler.go handler_test.go
git commit -m "feat: map retryable Postgres errors to 503"
```

---

### Task 5: Deployment — config, image, compose

**Files:**
- Modify: `Dockerfile`, `main.go`, `main_test.go`
- Create/modify: `compose.yaml`

- [ ] **Step 1: Write the failing test**

```go
func TestDatabaseURLIsRequired(t *testing.T)
// run([]string{"-data", dir}) returns an error naming -database-url
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./... -run TestDatabaseURLIsRequired -v`
Expected: FAIL

- [ ] **Step 3: Implement and extend compose**

`run` requires both `-data` and `-database-url`. `compose.yaml` gains the object store service alongside Postgres, with `depends_on` and the data volume. The Dockerfile is unchanged — pgx is pure Go, so the scratch image still works.

- [ ] **Step 4: Verify the static build and the image**

Run: `CGO_ENABLED=0 go build -o /tmp/os-pg . && podman build -t objectstore:pg . 2>&1 | tail -3`
Expected: a clean build and `Successfully tagged`.

- [ ] **Step 5: End-to-end against the real binary**

Run the spec's curl sequence from `docs/plan.md` against `/tmp/os-pg` pointed at the dev Postgres.
Expected: 201, 201, one blob file, 200, `hello`, `world`, two blob files, 204, `world`, `dedup_hits_total 1`.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile compose.yaml main.go main_test.go
git commit -m "feat: require a database URL and ship a compose stack"
```

---

### Task 6: Prove the migration was worth doing

**Files:**
- Create: `contention_test.go`

**Interfaces:**
- Consumes: everything.

- [ ] **Step 1: Write the measurement**

```go
func TestSweepDoesNotStarveWriters(t *testing.T)
// Port of the audit harness: 8 workers PUT for 6s, with and without a
// concurrent drain of 60k unreferenced blobs. Assert PUT throughput during
// the drain is at least 50% of baseline, and p99 under 1s.
// On SQLite the measured numbers were 1837 -> 99 PUTs and p99 117ms -> 3.37s.
```

The thresholds are deliberately loose: the test must catch a regression to SQLite-like behaviour without being flaky on a loaded laptop.

- [ ] **Step 2: Run it**

Run: `OBJECTSTORE_TEST_DATABASE_URL=… go test ./... -run TestSweepDoesNotStarveWriters -v`
Expected: PASS, with the logged before/after numbers showing throughput close to baseline.

- [ ] **Step 3: Record the result**

Put the measured before/after table in the design doc under a new *Outcome* section, so the next person can see what the migration actually bought.

- [ ] **Step 4: Commit**

```bash
git add contention_test.go docs/superpowers/specs/2026-11-24-postgres-migration-design.md
git commit -m "test: pin that a sweeper drain no longer starves writers"
```

---

## Deliberately not built

Multi-instance support and shared blob storage — blobs stay on local disk, so the service is still one replica and the deployment stays `strategy: Recreate`. Schema migrations beyond `CREATE TABLE IF NOT EXISTS`, connection retry/backoff at boot, read replicas, and any SQLite fallback.
