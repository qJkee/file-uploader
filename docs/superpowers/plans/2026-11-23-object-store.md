# HTTP Object Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An HTTP service storing opaque byte objects at `/objects/{bucket}/{objectID}` with PUT/GET/DELETE, per-bucket content dedup, and optimistic concurrency via ETags.

**Architecture:** Content-addressed blobs on local disk (`{data}/blobs/{bucket}/{hh}/{hash}`), SQLite as the transactional name→content mapping and the only source of truth. Blobs are never unlinked inline: a periodic sweeper reclaims unreferenced blobs to a trash directory with a retention window. A foreign key from `objects` to `blobs` is the lock that makes the PUT/sweeper race impossible.

**Tech Stack:** Go (flat `package main`), `modernc.org/sqlite` (pure Go, no cgo), `github.com/prometheus/client_golang`.

**Spec:** `docs/plan.md`

## Global Constraints

- Flat `package main` at the repo root. No internal packages.
- Dependencies are exactly `modernc.org/sqlite` and `github.com/prometheus/client_golang`. Nothing else.
- Builds with `CGO_ENABLED=0`.
- The database never stores object content and never stores a blob path. The path is a pure function of `(bucket, hash)`.
- PRAGMAs go in the DSN, never `db.Exec` — `database/sql` pools connections and they are per-connection.
- Bytes are durable (`tmp.Sync()`, rename, fsync parent dir) **before** the row commits. Never the reverse.
- ETags are strong: `"<lowercase hex sha256>"`, no `W/` prefix.
- Metric names are prefixed `objectstore_`. Never label by bucket or object ID.
- Deviation from the spec's file list: entity-tag parsing and precondition evaluation live in `etag.go`, not `handler.go`, because `store.go` also needs them. Same package, no interface change.
- Every task ends green under `go test -race ./...`.

## Review Focus

1. **Percent-encoded separators in path segments** (`%2F`, `%2E%2E`) — `PathValue` returns the decoded segment, so these must reach validation and get 400, never escape the bucket directory. *(Task 6)*
2. **Weak and multi-valued `If-Match`** — `W/"h"` must fail strong comparison and give 412; `If-Match: "a", "b"` must succeed when either matches. *(Task 7)*
3. **Client disconnect / context cancellation mid-PUT** — the temp file is removed, nothing commits, and the client gets no 5xx in the log path. *(Task 10)*
4. **`-trash-retention 0` and a missing trash directory** — retention 0 unlinks at reclaim time instead of staging; a sweep before any trash exists must not error. *(Task 4)*
5. **Name length boundaries** — bucket 63 accepted / 64 rejected, objectID 255 accepted / 256 rejected, and an empty segment rejected. *(Task 1)*

---

### Task 1: Module, name validation, path derivation

**Files:**
- Create: `go.mod`, `store.go`, `store_test.go`, `.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```go
  type Store struct { dataDir string }   // db field added in Task 2
  var ErrInvalidName = errors.New("invalid name")
  func validateBucket(b string) error      // ErrInvalidName on reject
  func validateObjectID(id string) error   // ErrInvalidName on reject
  func (s *Store) blobDir(bucket, hash string) string   // {data}/blobs/{bucket}/{hash[:2]}
  func (s *Store) blobPath(bucket, hash string) string  // blobDir + "/" + hash
  func (s *Store) trashDir(bucket string) string        // {data}/trash/{bucket}
  func (s *Store) trashPath(bucket, hash string) string // trashDir + "/" + hash
  func (s *Store) tmpDir() string                       // {data}/tmp
  ```

- [ ] **Step 1: `go mod init objectstore` and add `.gitignore`**

`.gitignore` contains `/objectstore` (the built binary) and `/.superpowers/`.

- [ ] **Step 2: Write the failing tests in `store_test.go`**

```go
func TestValidateBucket(t *testing.T)
// accept: "b1", "a", "a.b-c_d", strings.Repeat("a", 63)
// reject: "", "A", "Photos", "-a", ".a", ".", "..", "a/b", "a b",
//         "a%2Fb", strings.Repeat("a", 64), "é"
func TestValidateObjectID(t *testing.T)
// accept: "a", "A.b-c_d", strings.Repeat("a", 255)
// reject: "", ".", "..", "a/b", "a b", strings.Repeat("a", 256), "café"
func TestBlobPathFanout(t *testing.T)
// (&Store{dataDir: "/d"}).blobPath("b1", "ab12…") == "/d/blobs/b1/ab/ab12…"
// trashPath("b1", "ab12…")                        == "/d/trash/b1/ab12…"
```

Every reject case asserts `errors.Is(err, ErrInvalidName)`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./... -run 'TestValidate|TestBlobPath' -v`
Expected: FAIL — build error, `undefined: validateBucket`.

- [ ] **Step 4: Implement validation and path helpers in `store.go`**

Two package-level compiled `regexp`: bucket `^[a-z0-9][a-z0-9._-]{0,62}$`, objectID `^[A-Za-z0-9._-]{1,255}$`. The bucket pattern already excludes `.` and `..`; objectID needs them rejected explicitly. One comment on `store.go`'s bucket regex: lowercase-only because the bucket is a directory name and APFS is case-insensitive while SQLite TEXT keys are not.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./... -run 'TestValidate|TestBlobPath' -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add go.mod .gitignore store.go store_test.go
git commit -m "feat: add bucket/object name validation and blob path derivation"
```

---

### Task 2: Store lifecycle — directories, DSN pragmas, schema, device check

**Files:**
- Modify: `store.go`
- Modify: `store_test.go`

**Interfaces:**
- Consumes: Task 1's `Store`, path helpers.
- Produces:
  ```go
  type Config struct {
      DataDir        string
      MaxObjectSize  int64
      SweepInterval  time.Duration
      SweepBudget    time.Duration
      TmpMaxAge      time.Duration
      TrashRetention time.Duration
  }
  type Store struct {
      cfg Config
      db  *sql.DB
      dataDir string
      hookBeforeCommit func() // test seam, nil in production (Task 10)
      hookGetMiss      func() // test seam, nil in production (Task 3)
  }
  func Open(cfg Config) (*Store, error)
  func (s *Store) Close() error
  func (s *Store) DB() *sql.DB            // tests and /healthz
  func checkSameDevice(dirs ...string) error
  func newTestStore(t *testing.T) *Store  // test helper in store_test.go: Open on t.TempDir(), t.Cleanup(Close)
  ```

- [ ] **Step 1: Write the failing tests**

```go
func TestOpenCreatesLayout(t *testing.T)
// blobs/, tmp/, trash/ and meta.db exist under the data dir after Open
func TestForeignKeysAreLive(t *testing.T)
// INSERT INTO objects with no matching blobs row returns an error.
// If it succeeds, foreign_keys is off and the locking design is not there.
func TestForeignKeysOnEveryPooledConnection(t *testing.T)
// 16 goroutines each run the Task-2 FK insert; every one must error.
func TestCheckSameDevice(t *testing.T)
// checkSameDevice(dir, dir) == nil
// checkSameDevice(dir, "/dev") != nil   (t.Skip if Stat_t.Dev matches)
func TestTxLockIsImmediate(t *testing.T)
// openDB(dir, 0); tx1 := db.BeginTx(ctx, nil) and nothing else; a write on a
// second connection must fail immediately with "database is locked".
// A deferred transaction holds no lock and the second write would succeed.
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run 'TestOpen|TestForeignKeys|TestCheckSameDevice' -v`
Expected: FAIL — `undefined: Open`.

- [ ] **Step 3: Implement `Open`, `Close`, `DB`, `checkSameDevice`**

`Open` does, in order: `os.MkdirAll` for `blobs/`, `tmp/`, `trash/`; `checkSameDevice` across those three (`syscall.Stat_t.Dev` via `os.Stat` + `Sys()`); `sql.Open("sqlite", dsn)`; `db.Ping`; `db.Exec` the schema. The DSN is exactly:

```
file:{data}/meta.db?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)
```

`_txlock=immediate` makes every `db.BeginTx` a `BEGIN IMMEDIATE`; SQLite's default is deferred, which upgrades to a write lock mid-transaction and turns the design's check-then-act guarantees back into races. `busy_timeout` is a DSN parameter for the same reason as the rest: it is per-connection and `database/sql` pools connections, so an `Exec` would apply to whichever connection served it and silently not the others.

Expose `func openDB(dataDir string, busyTimeout time.Duration) (*sql.DB, error)` so `TestTxLockIsImmediate` can open with `busy_timeout(0)`.

Schema is the three statements from the spec's Design section verbatim (`blobs`, `objects`, `objects_by_hash`), both tables `STRICT`.

A `Ping` or schema failure returns an error — the caller refuses to start. Never auto-rebuild.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./... -run 'TestOpen|TestForeignKeys|TestCheckSameDevice' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum store.go store_test.go
git commit -m "feat: open store with WAL pragmas, schema and same-device check"
```

---

### Task 3: Put and Get — the dedup core

**Files:**
- Modify: `store.go`
- Modify: `store_test.go`

**Interfaces:**
- Consumes: Task 2's `Store`, `Open`, path helpers.
- Produces:
  ```go
  var ErrNotFound    = errors.New("object not found")
  var ErrBlobMissing = errors.New("blob missing")
  type PutResult struct { Hash string; Created bool; Dedup bool }
  func (s *Store) Put(ctx context.Context, bucket, objectID string, body io.Reader) (PutResult, error)
  func (s *Store) Get(ctx context.Context, bucket, objectID string) (*os.File, string, error)
  func (s *Store) Head(ctx context.Context, bucket, objectID string) (string, error)
  func fsyncDir(path string) error
  ```
  **Task 7 adds a trailing `pre Precondition` parameter to `Put`.** Callers written here are updated there.

- [ ] **Step 1: Write the failing tests**

```go
func TestPutGetRoundTrip(t *testing.T)        // Created==true, bytes identical
func TestPutOverwrite(t *testing.T)           // second Put: Created==false, Get returns new bytes
func TestDedupOneFileTwoIDs(t *testing.T)     // same bytes to a and b: both Get, countBlobFiles==1, second Put has Dedup==true
func TestSplitOnOverwrite(t *testing.T)       // a,b share hA; Put new bytes to b: a still hA's bytes, b new, countBlobFiles==2
func TestDedupIsPerBucket(t *testing.T)       // same bytes into b1 and b2: countBlobFiles==2
func TestPutEmptyBody(t *testing.T)           // zero bytes round-trips; hash is sha256 of empty
func TestGetMissing(t *testing.T)             // errors.Is(err, ErrNotFound)
func TestGetRetryDeleted(t *testing.T)        // hookGetMiss deletes the row  -> ErrNotFound
func TestGetRetryOverwritten(t *testing.T)    // hookGetMiss repoints the row -> new content returned
func TestGetRetryBlobMissing(t *testing.T)    // hookGetMiss does nothing      -> ErrBlobMissing
```

Test helper `countBlobFiles(t *testing.T, s *Store) int` walks `{data}/blobs` counting regular files. The three retry tests unlink the blob file, then set `s.hookGetMiss` before calling `Get`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run 'TestPut|TestGet|TestDedup|TestSplit' -v`
Expected: FAIL — `undefined: (*Store).Put`.

- [ ] **Step 3: Implement `Put`**

Order is load-bearing — follow the spec's PUT flow exactly:

1. `validateBucket` / `validateObjectID`.
2. `os.CreateTemp(s.tmpDir(), "put-*")`, `defer os.Remove(tmp.Name())`.
3. `io.Copy(io.MultiWriter(tmp, sha256.New()), body)` — the body is never held in memory. Copy errors propagate unwrapped so the handler can classify them.
4. `tmp.Sync()`, `tmp.Close()`, hex the digest, `os.MkdirAll(s.blobDir(...))`.
5. `db.BeginTx(ctx, nil)`, which is `BEGIN IMMEDIATE` because of Task 2's `_txlock=immediate`. `defer tx.Rollback()`.
6. Inside: `SELECT hash FROM objects WHERE bucket=? AND object_id=?` (sets `Created`); `INSERT INTO blobs … ON CONFLICT (bucket, hash) DO UPDATE SET hash = excluded.hash` (**`DO UPDATE`, not `DO NOTHING`** — `DO NOTHING` takes no lock on the existing row); stat the blob path, and only if absent `os.Rename(tmp, blobPath)` + `fsyncDir(blobDir)` — if present, set `Dedup = true` and leave the file; `INSERT INTO objects … ON CONFLICT (bucket, object_id) DO UPDATE SET hash = excluded.hash`; call `s.hookBeforeCommit` if non-nil; commit.

- [ ] **Step 4: Implement `Get` and `Head`**

`Head` is the row read alone, returning `ErrNotFound` when absent. `Get` reads the row, `os.Open`s the derived path, and on `os.IsNotExist` calls `s.hookGetMiss` if non-nil, then re-reads the row **once**: no row → `ErrNotFound`; different hash → open that; same hash → `ErrBlobMissing`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -race ./... -run 'TestPut|TestGet|TestDedup|TestSplit' -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add store.go store_test.go
git commit -m "feat: add content-addressed Put/Get with per-bucket dedup"
```

---

### Task 4: Delete, sweeper, trash retention, temp cleanup

**Files:**
- Modify: `store.go`
- Create: `sweeper.go`, `sweeper_test.go`
- Modify: `store_test.go`

**Interfaces:**
- Consumes: Task 3's `Store`, `Put`, `Get`, `ErrNotFound`.
- Produces:
  ```go
  func (s *Store) Delete(ctx context.Context, bucket, objectID string) error
  type SweepResult struct {
      Candidates, Reclaimed, TrashDeleted, TmpDeleted int
      TrashFiles int
      TrashBytes int64
  }
  func (s *Store) Sweep(ctx context.Context) (SweepResult, error)
  var sweepLimit = 1000 // var, not const, so tests can lower it
  ```
  **Task 7 adds a trailing `pre Precondition` parameter to `Delete`.**

- [ ] **Step 1: Write the failing tests**

```go
// store_test.go
func TestDeleteRemovesRowNotFile(t *testing.T)   // Get -> ErrNotFound, countBlobFiles still 1
func TestDeleteMissing(t *testing.T)             // errors.Is(err, ErrNotFound)
func TestDeleteOneOfTwoSharedIDs(t *testing.T)   // the other ID still returns its bytes
// sweeper_test.go
func TestSweepMovesUnreferencedBlobToTrash(t *testing.T) // blobs/ empty, trash/{bucket}/{hash} exists, no blobs row
func TestSweepKeepsReferencedBlob(t *testing.T)
func TestSweepDrainsBacklogLargerThanLimit(t *testing.T) // sweepLimit=10, 25 unreferenced blobs, one Sweep reclaims all 25
func TestSweepBudgetStops(t *testing.T)                  // SweepBudget=1ns leaves work and returns nil error
func TestTrashRetention(t *testing.T)                    // os.Chtimes an entry older than retention: unlinked; a fresh one survives
func TestTrashRetentionZeroUnlinks(t *testing.T)         // TrashRetention=0: reclaim unlinks, nothing lands in trash
func TestSweepOnEmptyStore(t *testing.T)                 // no blobs, no trash dir contents: no error
func TestTmpCleanup(t *testing.T)                        // a temp older than TmpMaxAge is removed, a fresh one is not
func TestTrashRecoveryPath(t *testing.T)                 // rename a trash file back into blobs/ + reinsert both rows -> Get works
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run 'TestDelete|TestSweep|TestTrash|TestTmp' -v`
Expected: FAIL — `undefined: (*Store).Delete`.

- [ ] **Step 3: Implement `Delete`**

Unconditionally transactional (`BEGIN IMMEDIATE`): `SELECT hash` → `ErrNotFound` if no row, else `DELETE FROM objects WHERE bucket=? AND object_id=?`, commit. No filesystem work. Splitting the SELECT and the DELETE across a transaction boundary would be check-then-act; DELETE is not a hot path.

- [ ] **Step 4: Implement `Sweep` in `sweeper.go`**

Three passes, results accumulated into `SweepResult`:

1. **Reclaim.** Loop until the candidate query returns fewer than `sweepLimit` rows or `cfg.SweepBudget` elapses. The query is the spec's indexed anti-join over `blobs`/`objects` with `LIMIT sweepLimit`. Per candidate, in its own `BEGIN IMMEDIATE`: `DELETE FROM blobs WHERE bucket=? AND hash=? AND NOT EXISTS (SELECT 1 FROM objects WHERE bucket=? AND hash=?)`, and only if that deleted exactly one row, `os.Rename` the blob to `trashPath` (after `MkdirAll` of `trashDir`) — or `os.Remove` it when `cfg.TrashRetention == 0`. There is no grace period: a candidate is reclaimed the first time it is seen, because `Get`'s ENOENT retry resolves the race a grace period would only make unlikely. An `os.IsNotExist` on the rename is not an error (the blob was already gone); anything else rolls the transaction back.
2. **Trash retention.** Walk `{data}/trash`, unlink entries with `ModTime` older than `cfg.TrashRetention`, and accumulate `TrashFiles` / `TrashBytes` from what survives. A missing trash directory is not an error.
3. **Temp cleanup.** Unlink `{data}/tmp/*` with `ModTime` older than `cfg.TmpMaxAge`.

```go
// shortcut: a PUT of content currently sitting in trash writes a fresh copy
// rather than resurrecting it. Correct, slightly wasteful. Add a trash stat
// on the blob-absent path if delete/re-upload churn turns out to be common.
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -race ./... -run 'TestDelete|TestSweep|TestTrash|TestTmp' -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add store.go store_test.go sweeper.go sweeper_test.go
git commit -m "feat: add DELETE and the reclaim/trash/tmp sweeper"
```

---

### Task 5: Startup orphan walk and its two guards

**Files:**
- Modify: `sweeper.go`
- Modify: `sweeper_test.go`

**Interfaces:**
- Consumes: Task 4's sweeper internals, `trashPath`.
- Produces: `func (s *Store) SweepOrphans(ctx context.Context, startedAt time.Time) (int, error)`

- [ ] **Step 1: Write the failing tests**

```go
func TestOrphanWalkMovesUnknownBlobToTrash(t *testing.T) // file with no blobs row -> trash, NOT unlinked
func TestOrphanWalkKeepsKnownBlob(t *testing.T)
func TestOrphanWalkSkipsFilesNewerThanStart(t *testing.T) // startedAt in the past: file untouched
func TestOrphanWalkSkippedWhenBlobsTableEmpty(t *testing.T)
// non-empty blob tree + empty blobs table -> returns 0, every file still present
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run TestOrphanWalk -v`
Expected: FAIL — `undefined: (*Store).SweepOrphans`.

- [ ] **Step 3: Implement `SweepOrphans`**

Guard first: `SELECT EXISTS (SELECT 1 FROM blobs)`. If `blobs` is empty, check whether any regular file exists under `{data}/blobs`; if so, log at `slog.Error` ("blobs table empty but blob tree is not; skipping orphan walk — check that the right meta.db is mounted") and return `0, nil` **without walking**. Without this guard a lost or replaced `meta.db` deletes every object on disk, unrecoverably: blob filenames are hashes, so even with the bytes intact the object IDs can never be reconstructed.

Then `filepath.WalkDir` over `{data}/blobs`, skipping any file whose `ModTime` is not before `startedAt` (in-flight PUTs are creating blobs while the walk runs). For each remaining file, derive `(bucket, hash)` from its path, and if `SELECT 1 FROM blobs WHERE bucket=? AND hash=?` finds nothing, `os.Rename` it to `trashPath` — trash, not unlink, because this is the most dangerous operation in the system and so should be the most clearly recoverable.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./... -run TestOrphanWalk -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sweeper.go sweeper_test.go
git commit -m "feat: add startup orphan walk with empty-metadata guard"
```

---

### Task 6: HTTP handlers, routing and error mapping

**Files:**
- Create: `handler.go`, `handler_test.go`

**Interfaces:**
- Consumes: Task 3–4's `Put`, `Get`, `Head`, `Delete`, and the sentinel errors.
- Produces:
  ```go
  type Handler struct { store *Store; maxObjectSize int64 }
  func NewHandler(s *Store) *Handler      // maxObjectSize from s.cfg
  func (h *Handler) Mux() *http.ServeMux  // Task 8 adds /metrics and /healthz
  func statusFor(err error) int
  ```

- [ ] **Step 1: Write the failing tests**

```go
func TestStatusFor(t *testing.T)
// ErrNotFound->404, ErrInvalidName->400, ErrBlobMissing->500,
// syscall.ENOSPC->507, context.Canceled->0 ("client gone, write nothing")
func TestPutThenGet(t *testing.T)          // 201 then 200, identical bytes, correct Content-Length
func TestPutTwiceStatus(t *testing.T)      // 201 then 200, second body wins
func TestHead(t *testing.T)                // 200, Content-Length set, empty body
func TestGetMissingIs404(t *testing.T)
func TestDeleteIs204ThenGet404(t *testing.T)
func TestDeleteMissingIs404(t *testing.T)
func TestInvalidNamesAre400(t *testing.T)
// path segments: "..", "", 64-char bucket, "A" in bucket, "%2F" inside the objectID,
// "%2E%2E" as the objectID
func TestOversizeContentLengthIs413(t *testing.T)   // honest Content-Length, body never read
func TestOversizeChunkedIs413(t *testing.T)         // no Content-Length, MaxBytesReader catches it
func TestRangeRequest(t *testing.T)                 // Range: bytes=2-5 -> 206 and the right 4 bytes
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run 'TestStatusFor|TestPut|TestHead|TestGet|TestDelete|TestInvalid|TestOversize|TestRange' -v`
Expected: FAIL — `undefined: NewHandler`.

- [ ] **Step 3: Implement the mux and handlers**

```go
mux.HandleFunc("PUT /objects/{bucket}/{objectID}", h.put)
mux.HandleFunc("GET /objects/{bucket}/{objectID}", h.get)
mux.HandleFunc("DELETE /objects/{bucket}/{objectID}", h.delete)
```

`put`: validate → 400; `r.ContentLength > maxObjectSize` → 413 **before reading a byte**; `http.MaxBytesReader(w, r.Body, maxObjectSize)`; `store.Put`; set `ETag: "<hash>"`; 201 when `Created`, else 200.

`get`: `store.Get` → `defer f.Close()`; set `ETag` **then** `http.ServeContent(w, r, "", time.Time{}, f)`, which gives Range, Content-Length, sniffing and `If-None-Match` → 304 for free, including `*`, comma lists and weak comparison. HEAD comes free from `ServeMux` + `ServeContent`.

`delete`: `store.Delete` → 204.

`statusFor` maps, in order: `ErrInvalidName`→400, `ErrNotFound`→404, `ErrBlobMissing`→500 (and `blobMissingTotal.Inc()` in Task 8), `*http.MaxBytesError`→413, `syscall.ENOSPC`→507, SQLite busy (`errors.As` to `*sqlite.Error` with code 5 or 6)→503 with `Retry-After: 1`, `context.Canceled`→**0**, meaning the client is gone: log at debug and write nothing. Default 500. A generic 500 for ENOSPC hides the cause from whoever is paged.

```go
// shortcut: the SQLITE_BUSY -> 503 branch is not covered by a test, because
// the driver's error value is not constructible from outside the package.
// Upgrade with a real lock-contention test if 503s ever show up in production.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./... -run 'TestStatusFor|TestPut|TestHead|TestGet|TestDelete|TestInvalid|TestOversize|TestRange' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add handler.go handler_test.go
git commit -m "feat: add PUT/GET/DELETE handlers with size limits and error mapping"
```

---

### Task 7: Preconditions — ETag parsing, If-Match, If-None-Match

**Files:**
- Create: `etag.go`, `etag_test.go`
- Modify: `store.go`, `sweeper.go` (callers), `handler.go`, `handler_test.go`, `store_test.go`

**Interfaces:**
- Consumes: Task 3's `Put`, Task 4's `Delete`, Task 6's `statusFor`.
- Produces:
  ```go
  var ErrPrecondition = errors.New("precondition failed")
  type etagEntry struct { value string; weak bool }
  type TagList struct { Any bool; Tags []etagEntry }
  func parseTagList(v string) (*TagList, bool)  // ok=false on malformed input -> 400
  type Precondition struct { IfMatch, IfNoneMatch *TagList } // nil field = header absent
  func (p Precondition) check(curHash string, exists bool) error
  ```
  `Put` and `Delete` gain a trailing `pre Precondition`. `statusFor` maps `ErrPrecondition` → 412.

- [ ] **Step 1: Write the failing tests**

```go
// etag_test.go
func TestParseTagList(t *testing.T)
// `"a"` -> one strong "a"; `"a", "b"` -> two; `*` -> Any; `W/"a"` -> one weak "a";
// `a` (unquoted) -> !ok; `` (empty) -> !ok; `"a",` -> !ok
func TestPreconditionCheck(t *testing.T)
// If-Match "h" + exists/match -> nil; mismatch -> ErrPrecondition;
// If-Match "h" + !exists -> ErrPrecondition (412, not 404);
// If-Match * + exists -> nil, !exists -> ErrPrecondition;
// If-Match W/"h" -> ErrPrecondition (strong comparison required by RFC 9110);
// If-Match `"a", "h"` -> nil (either may match);
// If-None-Match * + exists -> ErrPrecondition, !exists -> nil;
// If-Match evaluated before If-None-Match (RFC 9110 §13.2.2)
// handler_test.go
func TestGetETagAndNotModified(t *testing.T)   // ETag present; If-None-Match -> 304 empty body; stale tag -> 200
func TestLostUpdatePrevented(t *testing.T)     // two If-Match PUTs from one ETag: exactly one 2xx, one 412, winner's bytes win
func TestIfMatchOnMissingIs412(t *testing.T)
func TestIfNoneMatchStar(t *testing.T)         // 201 when absent, 412 when present, existing content untouched
func TestIfMatchOnDelete(t *testing.T)         // mismatch -> 412 and still readable; match -> 204
func TestFailedPreconditionLeavesNoTrace(t *testing.T) // after a 412: tmp/ empty, no new blobs row
func TestOversizeWithFailingIfMatchIs412(t *testing.T) // advisory pre-check fires before the body transfers
func TestABA(t *testing.T)                     // A -> B -> A, then PUT with the original "A" tag succeeds
func TestIdenticalContentSharesETag(t *testing.T)
func TestNoPreconditionIsLastWriterWins(t *testing.T)
func TestMalformedPreconditionIs400(t *testing.T)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run 'TestParseTagList|TestPrecondition|TestIfMatch|TestIfNone|TestLostUpdate|TestABA' -v`
Expected: FAIL — `undefined: parseTagList`.

- [ ] **Step 3: Implement `etag.go`**

`parseTagList` splits on commas, trims OWS, accepts `*` only as the whole value, and requires each remaining element to be `W/"…"` or `"…"` with no embedded quote — anything else is malformed. `check` evaluates `If-Match` first, then `If-None-Match`: `If-Match` uses strong comparison (`!weak && value == curHash`) because RFC 9110 requires it and a weak tag would be unusable for exactly the case this exists to serve; `If-None-Match` uses weak comparison (value equality regardless of the weak flag).

- [ ] **Step 4: Thread `pre` through `Put` and `Delete`**

In both, the **authoritative** check runs inside `BEGIN IMMEDIATE`, against the hash the transaction's own `SELECT` returned. Outside it, another PUT lands between the read and the write and the lost update is back with a smaller window.

- [ ] **Step 5: Wire the handler**

`put` parses `If-Match`/`If-None-Match` (malformed → 400), then runs the **advisory** pre-check: call `store.Head` and `pre.check`, returning 412 before streaming the body. This is purely an optimization so a doomed 32 MiB upload isn't transferred; it is racy on its own and step 4's check is authoritative. `delete` parses `If-Match` and passes it through. The advisory check runs before the `ContentLength > max` 413 so an oversize body with a failing `If-Match` gets 412.

- [ ] **Step 6: Run the full suite**

Run: `go test -race ./... -v`
Expected: PASS, including every earlier task's tests with the new parameter.

- [ ] **Step 7: Commit**

```bash
git add etag.go etag_test.go store.go sweeper.go handler.go handler_test.go store_test.go
git commit -m "feat: add If-Match/If-None-Match optimistic concurrency"
```

---

### Task 8: Metrics, /metrics and /healthz

**Files:**
- Create: `metrics.go`, `metrics_test.go`
- Modify: `handler.go`, `store.go`, `sweeper.go`

**Interfaces:**
- Consumes: Task 6's `Mux`, Task 4's `SweepResult`.
- Produces: package-level collectors registered with `promauto`, and `func (h *Handler) health(w http.ResponseWriter, r *http.Request)`.

- [ ] **Step 1: Write the failing tests**

```go
func TestMetricsEndpoint(t *testing.T)     // GET /metrics -> 200, body contains objectstore_requests_total
func TestDedupHitCounter(t *testing.T)     // two identical PUTs increment objectstore_dedup_hits_total by 1
func TestNoHighCardinalityLabels(t *testing.T)
// after PUT /objects/secretbucket/secretobject, the /metrics body contains
// neither "secretbucket" nor "secretobject"
func TestHealthz(t *testing.T)             // 200 on a live store
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./... -run 'TestMetrics|TestDedupHit|TestNoHighCardinality|TestHealthz' -v`
Expected: FAIL — no `/metrics` route, 404.

- [ ] **Step 3: Define the collectors in `metrics.go`**

Exactly the spec's list, names verbatim: `objectstore_requests_total{method,code}`, `objectstore_request_duration_seconds{method}`, `objectstore_put_bytes_total`, `objectstore_dedup_hits_total`, `objectstore_blob_missing_total`, `objectstore_sweep_runs_total{result}`, `objectstore_sweep_duration_seconds`, `objectstore_sweep_last_success_timestamp_seconds`, `objectstore_sweep_deleted_total`, `objectstore_sweep_candidates`, `objectstore_trash_files`, `objectstore_trash_bytes`, `objectstore_startup_orphans_deleted_total`. Counters, not `COUNT(*)` gauges, so a scrape stays O(1) regardless of store size. **Never label by bucket or object ID** — unbounded cardinality is the standard way to take down a Prometheus server.

- [ ] **Step 4: Wire them**

`Mux` adds `GET /metrics` → `promhttp.Handler()` and `GET /healthz` → `h.health`, and the object routes are wrapped with `promhttp.InstrumentHandlerCounter` + `InstrumentHandlerDuration` — no hand-rolled middleware. `Put` increments `put_bytes_total` and `dedup_hits_total`; `Get`'s `ErrBlobMissing` path increments `blob_missing_total`; `Sweep` sets the sweep gauges and counters from its `SweepResult`; `SweepOrphans` increments `startup_orphans_deleted_total`. `health` returns 200 when `SELECT 1` succeeds and the data dir is writable, else 503.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -race ./... -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add metrics.go metrics_test.go handler.go store.go sweeper.go
git commit -m "feat: add Prometheus metrics and health endpoint"
```

---

### Task 9: main.go — flags, env fallback, server, shutdown, Dockerfile

**Files:**
- Create: `main.go`, `main_test.go`, `Dockerfile`, `.dockerignore`

**Interfaces:**
- Consumes: `Open`, `Config`, `NewHandler`, `Sweep`, `SweepOrphans`.
- Produces: `func envFallback(fs *flag.FlagSet, prefix string)`

- [ ] **Step 1: Write the failing test**

```go
func TestEnvFallback(t *testing.T)
// a flag left unset takes OBJECTSTORE_<NAME>; a flag set on the command line
// is not overridden; an unset flag with no env var keeps its default;
// an unparseable env value is reported as an error
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./... -run TestEnvFallback -v`
Expected: FAIL — `undefined: envFallback`.

- [ ] **Step 3: Implement `main.go`**

Flags with the spec's defaults: `-addr :8080`, `-data`, `-max-object-size 32MiB`, `-sweep-interval 1m`, `-sweep-budget 10s`, `-tmp-max-age 1h`, `-trash-retention 24h`. `envFallback` uses `flag.Visit` to find which were set explicitly and `fs.Set` for the rest from `OBJECTSTORE_<UPPER_SNAKE_NAME>`. Flags stay the single source of truth; env works for containers; no config dependency.

Then: `Open` (a failure here is a refusal to start, logged and `os.Exit(1)`); start `SweepOrphans` in a background goroutine with `startedAt = time.Now()` captured **before** the server starts accepting; start a sweeper goroutine on `time.Ticker(cfg.SweepInterval)`; `http.Server{ReadHeaderTimeout: 10s, IdleTimeout: 120s}` with **no `WriteTimeout` and no `ReadTimeout`** — either would abort a legitimate slow transfer of a 32 MiB object mid-flight, and `MaxBytesReader` bounds uploads by size instead, which is the right axis. `signal.NotifyContext` on SIGINT/SIGTERM → `srv.Shutdown`, then stop the sweeper and `store.Close`.

Structured `log/slog`: errors and per-sweep summaries only, no per-request access log.

- [ ] **Step 4: Run the test and build**

Run: `go test -race ./... -run TestEnvFallback -v && CGO_ENABLED=0 go build ./...`
Expected: PASS, then a clean build.

- [ ] **Step 5: Add the `Dockerfile` and `.dockerignore`**

The spec's Dockerfile verbatim, with the builder image pinned to the Go version in `go.mod`. `.dockerignore` excludes `.git`, `docs`, `.superpowers`.

- [ ] **Step 6: Run the spec's end-to-end script**

Run the spec's *Verification → End to end* curl sequence against `go run . -data $(mktemp -d)`.
Expected: 201, 201, exactly one blob file, 200, `hello`, `world`, two blob files, 204, `world`.

- [ ] **Step 7: Commit**

```bash
git add main.go main_test.go Dockerfile .dockerignore
git commit -m "feat: add server entrypoint, config and container image"
```

---

### Task 10: Concurrency, fault injection and the invariant check

**Files:**
- Create: `concurrency_test.go`
- Modify: `store_test.go`

**Interfaces:**
- Consumes: everything. Uses `hookBeforeCommit` and `hookGetMiss` from Task 2.
- Produces: `func assertConsistent(t *testing.T, s *Store)`

- [ ] **Step 1: Write `assertConsistent` and the failing tests**

`assertConsistent` checks three things: every `objects` row has a readable file at its derived path; after a `Sweep`, every file under `blobs/` has a `blobs` row; `objectstore_blob_missing_total == 0`.

```go
func TestRaceHarness(t *testing.T)
// N goroutines doing mixed PUT/GET/DELETE against one httptest.Server, plus a
// goroutine PUTting content X in a loop while Sweep runs against an
// unreferenced blob X. assertConsistent at the end.
func TestIdenticalContentStorm(t *testing.T)
// N goroutines PUT the same bytes to distinct IDs: no errors, one blob file, one blobs row
func TestCrashMidPut(t *testing.T)
// hookBeforeCommit panics/returns after the rename: a file exists with no row,
// and SweepOrphans reclaims it
func TestDiskFullIs507(t *testing.T)
// a body reader returning syscall.ENOSPC after N bytes -> 507, tmp/ empty
func TestClientDisconnectMidPut(t *testing.T)
// cancel the request context mid-body: no 5xx logged, tmp/ empty, no row committed
func TestLostDatabase(t *testing.T)
// delete meta.db between two Opens: every blob file survives and the walk is skipped
func BenchmarkPut(b *testing.B) // unique vs duplicate content
```

```go
// shortcut: hookBeforeCommit tests the crash window, not a real crash. It does
// not cover power loss during fsync. Upgrade to a SIGKILLed subprocess if that
// ever matters.
```

```go
// shortcut: TestDiskFullIs507 injects ENOSPC from the body reader rather than
// the temp file's writer, so it exercises the status mapping and the temp
// cleanup but not a genuine filesystem-full write. A tmpfs test would need
// Linux and root.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestRace|TestIdentical|TestCrash|TestDiskFull|TestClientDisconnect|TestLostDatabase' -v`
Expected: FAIL — `undefined: assertConsistent`.

- [ ] **Step 3: Make them pass**

If `TestIdenticalContentStorm` or `TestRaceHarness` fails, the cause is in the transaction mode or the `ON CONFLICT … DO UPDATE` on `blobs` — fix the root cause in `store.go`, never the test. Use superpowers:systematic-debugging.

- [ ] **Step 4: Run the whole suite under -race**

Run: `go test -race ./... 2>&1 | tail -40`
Expected: `ok  objectstore`, no data races.

- [ ] **Step 5: Commit**

```bash
git add concurrency_test.go store_test.go
git commit -m "test: add concurrency, fault-injection and invariant checks"
```

---

## Deliberately not built

Per the spec's *Deliberately not built* section: `If-Unmodified-Since`/`If-Modified-Since`, soft delete on `objects`, a channel-fed cleanup worker, authentication, bucket listing, object metadata, pagination, delete-bucket, rate limiting, quotas, global cross-bucket dedup, schema migrations, backups, and multi-instance support. No Kubernetes manifests and no README — neither is in the spec.
