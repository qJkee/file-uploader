# HTTP object store with per-bucket dedup

> **Superseded on the storage engine.** This document is the original SQLite
> design and still says `meta.db`, `_pragma`, `_txlock=immediate` and "SQLite
> has no row locks"; metadata now lives in Postgres via pgx/v5. For anything
> touching the database, locking or deployment, the authority is
> `docs/superpowers/specs/2026-11-24-postgres-migration-design.md`.

## Context

Empty repo (`/Users/bulatzamalutdinov/work/test-task`). An HTTP service storing
objects in buckets at `/objects/{bucket}/{objectID}` — PUT, GET, DELETE. Go,
production-ready.

An object is **the bytes of the request body**, opaque: never parsed, returned
byte-for-byte. Requirements that shape the design:

- **PUT overwrites.** A second PUT to a path replaces its content.
- **Per-bucket dedup.** Identical bytes under two object IDs in one bucket are
  stored once and both IDs keep working. Two buckets holding identical bytes
  store it twice — buckets stay independent.
- **Optimistic concurrency.** `ETag` on every response, with `If-Match` and
  `If-None-Match` on writes, so two clients editing one path can't silently
  lose each other's work. Opt-in: callers that send no precondition get
  last-writer-wins.

The original idea was SQLite as a path index in front of files, to save I/O on
lookup. That reason doesn't hold: with a deterministic path the `open()` *is*
the lookup, so an index query before it adds I/O. Dedup is what earns SQLite
its place — it needs a content-hash mapping and reference bookkeeping that has
to be transactional.

## Design

**Content-addressed blobs on disk, SQLite as the name→content mapping.**

```
{data}/meta.db                        the mapping (plus -wal, -shm)
{data}/blobs/{bucket}/{hh}/{hash}     the bytes, hh = first 2 hex chars
{data}/tmp/put-*                      in-flight uploads
{data}/trash/{bucket}/{hash}          reclaimed blobs, held for retention
```

The `hh` fanout keeps a directory from reaching millions of entries. Everything
sits under one `{data}` root so every rename is same-filesystem and atomic.

Trash keeps the bucket in its path for the same reason `blobs` does: dedup is
per-bucket, so two buckets can legitimately hold the same hash. Flattening it
to `trash/{hash}` would collide them and leave the documented recovery — rename
the file back — unable to tell which bucket it belonged to.

This is the standard object-store architecture rather than anything novel:
**transactional metadata as the only source of truth, immutable
content-addressed data, and asynchronous reclamation with a retention window.**
Dropbox's Magic Pocket (SHA-256 content-addressed blocks, MySQL metadata,
async GC), S3 (strongly consistent index over immutable shards) and GCS
(Spanner metadata over Colossus) all share that shape. The simplification
available to us is that they need consensus for the pointer flip and we get the
same guarantee from one SQLite transaction.

**The database never stores object content** — there is no BLOB column. It
holds only the mapping. The blob *path* isn't stored either: it's a pure
function of `(bucket, hash)`, and a derived value can't drift from where the
file actually is. Add a column only if blobs ever span volumes, and then it
should be `volume`, not a full path — store only what isn't computable.

```sql
CREATE TABLE IF NOT EXISTS blobs (
  bucket TEXT NOT NULL,
  hash   TEXT NOT NULL,           -- sha256 of body, lowercase hex
  PRIMARY KEY (bucket, hash)
) STRICT;

CREATE TABLE IF NOT EXISTS objects (
  bucket    TEXT NOT NULL,
  object_id TEXT NOT NULL,
  hash      TEXT NOT NULL,
  PRIMARY KEY (bucket, object_id),
  FOREIGN KEY (bucket, hash) REFERENCES blobs(bucket, hash)
) STRICT;

CREATE INDEX IF NOT EXISTS objects_by_hash ON objects(bucket, hash);
```

**There is no refcount.** A blob is live iff some `objects` row references it.
`blobs` stores existence, never a count — so unlike a refcount column it cannot
drift. A missed decrement leaks forever and a double-decrement destroys live
data; an anti-join cannot disagree with the rows it joins against.

The foreign key also buys **automatic per-key locking**, which is what makes the
PUT/sweeper race impossible rather than merely unlikely — see *Locking*.

### Cardinality: many-to-one

Many object IDs reference one blob; each object ID has exactly one current
content. So `objects.hash` is a plain foreign key, with no junction table and
no extra join on every GET. `objects` carries no attributes beyond its three
key columns, so the whole cardinality lives in the primary key:

```
PK (bucket, object_id)         → many-to-one, one current content per path
PK (bucket, object_id, hash)   → many-to-many, i.e. version history
```

Widening the key is the entire change needed to add versioning later.

### Overwriting one of two paths that share a blob

The case worth writing down, because it's where naive dedup corrupts data.
Start with `(b1,a) → hA` and `(b1,b) → hA`, one file on disk:

```
PUT /objects/b1/b  (new content, hash hB)

blobs:    INSERT (b1, hB)                      new row, locked
disk:     hB absent → rename temp in, fsync    hA is not touched
objects:  (b1,b) conflict → SET hash = hB      the paths separate here
commit

result:   a → hA, b → hB.  Two files, both live.
          hA still referenced by a, so the sweeper leaves it.
```

**No file is unlinked.** The separation is a pointer update. This is free
because blobs are content-addressed: `hA` and `hB` are distinct files by
construction, so sharing is never something you have to undo. Had blobs lived
at a path derived from the object ID, with `b` hardlinked to `a`'s file, this
PUT would need an explicit break-the-hardlink step and forgetting it would
corrupt `a`. That class of bug cannot occur here.

The only cleanup arises if `b` was the *last* reference to `hA` — then `hA`
goes unreferenced and the sweeper collects it, which is the ordinary path.

### Normalization, and the rule for columns added later

Both tables are in BCNF. `objects` has one nontrivial functional dependency,
`(bucket, object_id) → hash`, whose determinant is the primary key — no partial
dependency (hash needs both key columns), no transitive one. `blobs` is
all-key.

`bucket` appearing in both tables is not redundancy: it is half the foreign key
and half of `blobs`'s key, because dedup is per-bucket. The bucket *name*
repeating across rows is not a normal-form violation either — repeating a key
value never is. Extracting `buckets(id, name)` would be surrogate-key
substitution, and it adds an upsert to the PUT hot path to maintain something
nothing reads. Revisit when bucket *metadata* appears (quota, ACL) — that is
what would need a home.

**The rule that decides every future column: does the attribute depend on the
bytes, or on the name?**

- `size`, compression state, hash algorithm → **`blobs`**. Size is determined
  by content; on `objects` it would be a transitive dependency, stored twice
  whenever two IDs share a blob.
- `Content-Type`, created-at, user metadata → **`objects`**. Two names for
  identical bytes can legitimately carry different content types.

This is the one place the schema can quietly go wrong later.

### Why SQLite, and not Valkey or Postgres

The question underneath is "will this ever be more than one instance", and that
is already settled by something else: **the blobs live on local disk**, so a
second instance on another machine cannot see them. A node-local metadata store
is implied by the blob storage, not introduced by this choice.

- **Valkey** is the wrong shape for the primary record. It is a cache;
  durability is AOF/RDB and `everysec` can lose a second of writes. Losing a
  row doesn't lose one object — it strands bytes nothing can address. The
  refcount would also be hand-rolled as a set per hash: two keys per PUT with
  no transaction across them short of Lua. More code, weaker guarantee.
- **Postgres** is correct the day there are several instances — but that day
  also needs shared blob storage (S3/NFS), a far larger change than the
  database. On one node it adds a round-trip per request, a daemon to operate
  and a pool to tune, and buys nothing.
- **SQLite** does the same work in-process, ACID, in one file, with no ops.
  Writers serialize, but every PUT also writes a file and the file dominates.

The schema, constraints and statements above are valid on both, so the port is
a DSN and a driver; what it buys is row-level instead of global write locking.
Rearchitecting blob storage is the expensive move, and the one to watch.

### Deferred blob deletion, not inline

Neither PUT nor DELETE unlinks bytes; they only touch rows. A sweeper reclaims
unreferenced blobs. One mechanism buys three things:

1. **Smaller hot path.** DELETE is a single short transaction with no
   filesystem work. Inline reclamation would add a reference count and an
   unlink to every delete, holding SQLite's write lock across a filesystem
   operation each time.
2. **Crash orphans self-heal.** A crash between writing bytes and committing the
   row leaves a file nobody references; the same sweeper collects it, so there
   is no separate recovery path.
3. **Deletion cost is bounded and off the request path.** Reclaiming a 32 MiB
   unlink never shows up in a client's DELETE latency.

### Crash-safety ordering

**Make the bytes durable before committing the row.** Never the reverse. A crash
in that window leaves an orphan file (harmless, swept). The reverse leaves a row
pointing at nothing — a 500 for an object the client was told was stored.

"Durable" means `tmp.Sync()`, rename, **then fsync the parent directory**. Data
fsync alone doesn't make the directory entry durable, so power loss could drop
the rename while the committed row survives.

## Request flows

### PUT

1. Validate bucket and objectID (below) → 400.
2. `r.ContentLength > maxObjectSize` → 413 immediately, without reading a byte.
3. **Advisory precondition check** — if `If-Match`/`If-None-Match` is present,
   read the current hash and reject with 412 now, before streaming the body.
   Purely an optimization so a doomed 32 MiB upload isn't transferred; it is
   racy on its own and the authoritative check is at step 8.
4. `body := http.MaxBytesReader(w, r.Body, maxObjectSize)` — catches chunked
   uploads that omitted or lied about a length.
5. `tmp, _ := os.CreateTemp(dataDir+"/tmp", "put-*")`, and
   `defer os.Remove(tmp.Name())` — a no-op after a successful rename, and the
   thing that stops an aborted upload leaking a file.
6. `io.Copy(io.MultiWriter(tmp, sha256.New()), body)`. The body is never held
   in memory. `*http.MaxBytesError` → 413; ENOSPC → 507; client disconnect →
   log and drop.
7. `tmp.Sync()`, close. Hex the hash, `os.MkdirAll` the blob dir.
8. **`BEGIN IMMEDIATE`**
   ```sql
   SELECT hash FROM objects WHERE bucket=? AND object_id=?;
   ```
   This single read does double duty: it is the **authoritative** precondition
   check (see *Preconditions*), and it decides 201-vs-200 for the response.
   ```sql
   INSERT INTO blobs (bucket, hash) VALUES (?, ?)
     ON CONFLICT (bucket, hash) DO UPDATE SET hash = excluded.hash;
   ```
   `DO UPDATE` rather than `DO NOTHING` — **`DO NOTHING` takes no lock on the
   existing row**, which is exactly the hole this closes.
   - Stat the blob path. Absent → `os.Rename(tmp, blobPath)`, fsync the blob
     directory. Present → the content is identical by definition; leave it and
     count a dedup hit. *This is the dedup.*
   - `INSERT INTO objects … ON CONFLICT (bucket, object_id) DO UPDATE SET
     hash = excluded.hash`
   - **`COMMIT`**
9. 201 Created if the pre-image found nothing, else 200 OK. Both carry
   `ETag: "<new hash>"` so a client can chain writes without re-reading.

PUT is idempotent, so client retries are safe. Only the rename and dir fsync
run inside the transaction; streaming and hashing of up to 32 MiB finished at
step 6.

### GET (and HEAD, free from Go's mux and ServeContent)

1. `SELECT hash FROM objects WHERE bucket=? AND object_id=?` → no rows = 404.
2. `os.Open(blobPath)`.
3. Set `ETag: "<hash>"`, then `http.ServeContent(w, r, "", time.Time{}, f)` —
   Range, Content-Length, content sniffing **and `If-None-Match` → 304** all
   for free. ServeContent handles `*`, comma-separated lists and weak
   comparison correctly, which is why the header is set rather than the 304
   being answered by hand. The cost is one `open()` even on a 304 — a cached
   syscall, cheaper than the list parser the alternative needs.

**On ENOENT at step 2, re-read the row once** (bounded to one retry):

| second read | meaning | response |
|---|---|---|
| no row | concurrently deleted | 404 |
| different hash | concurrently overwritten | open the new hash |
| same hash | the invariant broke | 500, log, `blob_missing_total++` |

This is what makes a grace period unnecessary — see *Sweeper*. It sits on the
error path only, so the happy path pays nothing, and it *resolves* the race
rather than making it unlikely.

Once the file is open, a sweeper unlink can't break the transfer: on POSIX an
open fd stays valid after the directory entry is gone.

### DELETE

`BEGIN IMMEDIATE`, `SELECT hash` → not found = 404, precondition mismatch =
412, else `DELETE FROM objects WHERE bucket=? AND object_id=?`, `COMMIT`. 204.
No filesystem work.

This was one statement before preconditions. Splitting the SELECT and the
DELETE across a transaction boundary would be the check-then-act bug again, so
the transaction is unconditional rather than kept as a second code path —
DELETE is not a hot path.

### Preconditions

Strong ETags: `ETag: "<hex sha256>"`, no `W/` prefix. **RFC 9110 requires
strong comparison for `If-Match`**, so a weak tag would be unusable for the
exact case this exists to serve. Ours qualifies because we return the bytes
unmodified.

| Header | On | Condition | Else |
|---|---|---|---|
| `If-Match: "x"` | PUT, DELETE | current exists and matches | 412 |
| `If-Match: *` | PUT, DELETE | current exists | 412 |
| `If-None-Match: *` | PUT | nothing exists at the path — atomic create-only | 412 |
| `If-None-Match: "x"` | GET, HEAD | current differs | 304 |

Evaluated in RFC 9110 §13.2.2 order: `If-Match` first, then `If-None-Match`.
A missing resource with `If-Match` present is **412, not 404**.

**The authoritative check is inside `BEGIN IMMEDIATE`.** Outside it, another
PUT lands between the read and the write and the lost update is back with a
smaller window — the same check-then-act shape as the PUT/sweeper race.

Header parsing needs a ~20-line entity-tag list helper; `net/http`'s is
unexported. Since we only emit strong tags, comparison is string equality
after parsing.

**ABA, and why it's fine here.** The ETag is a content hash, not a version
counter, so content going A → B → A leaves a client holding the old "A" tag
with a *succeeding* `If-Match`; a counter would 412. That is the better
behaviour: the client's read-modify-write was computed against content A and
current content is A, so its change is still valid and the intermediate state
is irrelevant. It avoids a spurious conflict rather than missing a real one.

Two object IDs with identical content share an ETag. Harmless — HTTP scopes
ETags per URL and caches key on URL.

Clients sending no precondition header are unaffected: last-writer-wins,
exactly as before. Adding this cannot break an existing caller.

## Locking — why the foreign key is load-bearing

The race it closes: blob `h` has zero references. The sweeper queries, sees
none, and is about to unlink. A PUT of exactly that content arrives, finds the
blob already on disk, skips writing, commits its row — then the unlink lands and
a freshly committed row points at nothing.

Three ways to close it, worst to best:

| | |
|---|---|
| `LOCK TABLE` | Serializes every PUT against every other, including unrelated content. Global contention for a per-hash race. |
| `pg_advisory_xact_lock(hashtext(bucket‖hash))` | Correct per-key serialization, collisions only cost extra blocking. But nothing enforces it — a new code path that forgets the lock compiles fine and corrupts data later. |
| **Foreign key** | The lock is the schema. Adopted. |

`INSERT INTO objects` takes a `FOR KEY SHARE` lock on its parent `blobs` row;
the sweeper's `DELETE FROM blobs` needs a conflicting lock on the same row. They
block each other **for that one hash only** — two PUTs of different content
never meet. Nothing to remember, nothing to forget.

SQLite has no row locks and closes the same race with its global write lock:
coarser, identically correct, same statements.

## Sweeper

Goroutine on `-sweep-interval` (default 1 min). **No filesystem walk** — the
candidate list is an indexed anti-join over a small table:

```sql
SELECT bucket, hash FROM blobs b
WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.bucket = b.bucket AND o.hash = b.hash)
LIMIT 1000;
```

**Loop until the query returns fewer than `LIMIT` rows, under a wall-clock
budget** (10 s), then resume next interval. Without the loop, a burst of a
million deletes reclaims at 1000/interval and takes days.

**There is no grace period; a candidate is reclaimed the first time it is
seen** (reclaimed meaning moved to trash — the bytes still survive
`-trash-retention`, for a different reason, below). Two earlier designs are
recorded here so they don't get reintroduced:

- *mtime-based grace* was simply wrong — mtime records when a blob was
  **written**, possibly long before its last reference was dropped, so an old
  blob whose reference just vanished was swept on the very next pass with no
  grace at all.
- *delete only if unreferenced in two consecutive sweeps* was correct but
  stateful: the first-seen map costs ~150 MB under a million-delete burst, and
  it halves reclamation throughput.

Both existed to prevent the GET race. GET's ENOENT retry (above) **resolves**
that race instead, statelessly and on the error path only, which makes the
grace redundant.

Per candidate:

```sql
BEGIN IMMEDIATE;
  DELETE FROM blobs WHERE bucket = ? AND hash = ?
    AND NOT EXISTS (SELECT 1 FROM objects WHERE bucket = ? AND hash = ?);
  -- only if that deleted exactly one row:
  --   os.Rename(blobs/{bucket}/{hh}/{hash}, trash/{bucket}/{hash})   NOT unlink
COMMIT;
```

The re-check inside the transaction is what makes the move safe; the FK's
`RESTRICT` is a second line of defence if it is ever dropped.

### Trash, not unlink

Reclaimed blobs are renamed to `{data}/trash/{bucket}/{hash}` rather than
unlinked. A second pass deletes trash entries whose mtime is older than
`-trash-retention` (24 h).

This is what S3, GCS and Dropbox all do — none of them destroys bytes at
delete time, and GCS's soft-delete default is 7 days. The reason is **not** the
in-flight-reader race, which GET's ENOENT retry already resolves. It is a
safety net against metadata bugs and operator error: if a bad migration or a
logic error drops the wrong rows, the window is the difference between a
recoverable incident and permanent loss. Blobs in trash are named by hash, so
recovery is a rename back.

It also fixes what was wrong with the original mtime grace: mtime in `trash/`
means *when the blob became garbage*, which is exactly the quantity that
design needed and could not express.

Trash lives outside `{data}/blobs/`, so the anti-join and the startup orphan
walk never see it.

```
// shortcut: a PUT of content currently sitting in trash writes a fresh copy
// rather than resurrecting it. Correct, slightly wasteful. Add a trash stat
// on the blob-absent path if delete/re-upload churn turns out to be common.
```

`trash_files` and `trash_bytes` are accumulated during this pass, not computed
on scrape — the same reason the other metrics are counters rather than
`COUNT(*)` gauges.

### Temp cleanup

Each sweep also unlinks `{data}/tmp/*` older than `-tmp-max-age` (1 h) —
aborted uploads whose process died before the deferred remove could run. This
is unrelated to trash: a temp was never a committed object, so there is
nothing to retain.

### Startup orphan walk, and the guard that stops it destroying everything

Once per process, a background walk of `{data}/blobs/**` **moves files with no
`blobs` row to trash** — crash orphans, bytes renamed into place by a PUT that
died before committing. Trash, not unlink: this is the most dangerous
operation in the system, which is exactly why it should be the one most
clearly recoverable. Two further guards, both mandatory:

1. **Skip files with mtime newer than process start.** In-flight PUTs are
   creating blobs while the walk runs; without this the walk races them. With
   it, the walk can run in the background instead of blocking startup.
2. **If `blobs` is empty but the blob tree is not, skip the walk entirely and
   log at error level.** Without this guard, a lost or replaced `meta.db` —
   wrong volume mounted, bad restore, someone clearing the directory — means
   the service starts on an empty schema and **deletes every object on disk**,
   silently and permanently. It is unrecoverable in principle: blob filenames
   are hashes, so even with the bytes intact the object IDs that pointed at
   them can never be reconstructed.

## Validation — the trust boundary

- bucket: `^[a-z0-9][a-z0-9._-]{0,62}$`. **Lowercase only**, because the bucket
  is a directory name and APFS is case-insensitive by default while SQLite TEXT
  keys are not — `Photos` and `photos` would be two rows sharing one directory,
  so dedup would merge across them on macOS and not on Linux. S3 has the same
  rule for the same class of reason.
- objectID: `^[A-Za-z0-9._-]{1,255}$`, case-sensitive — it never touches the
  filesystem. Non-ASCII excluded deliberately: NFC/NFD would otherwise give two
  rows for one visible name.
- Both reject `.` and `..`.

Go 1.22+ `ServeMux` already prevents `/` inside either wildcard, and
`PathValue` returns the percent-decoded segment, so `%2F` lands in the regex
and gets a 400. Worth one test rather than taking my word for it.

## Failure modes

| Condition | Behaviour |
|---|---|
| Disk full mid-PUT (ENOSPC) | 507, temp removed, nothing committed. Detect with `errors.Is(err, syscall.ENOSPC)` — a generic 500 hides the cause from whoever is paged. |
| Disk full during sweep | Log, count a sweep error, retry next interval. |
| `tmp`, `blobs` or `trash` on different filesystems | **Refuse to start.** Compare `Stat_t.Dev` across all three at startup; `os.Rename` returns EXDEV, so every PUT (tmp→blobs) or every reclaim (blobs→trash) would fail — fail once at boot with a clear message, not on every request. |
| `meta.db` corrupt or unreadable | **Refuse to start.** Never auto-rebuild: the mapping cannot be reconstructed from hash-named files, and starting empty arms the orphan walk against live data. |
| `meta.db` empty, blob tree not | Start, but skip the orphan walk and log at error. See guard 2 above. |
| Row resolves, blob file missing | 500, log bucket/objectID/hash, `blob_missing_total++`. Do not self-heal by deleting the row — silently destroying data on a read is worse than an error. |
| `database is locked` past `busy_timeout` | **503 with `Retry-After`**, not 500. It is transient and the client should retry. |
| Client disconnects mid-PUT | `io.Copy` errors, deferred remove drops the temp, nothing commits. Log at debug, no 5xx. |
| Context cancelled during commit | Transaction rolls back; the already-renamed file becomes an orphan and is swept. Safe direction. |
| SQLite on NFS | Unsupported — WAL needs working POSIX locks. Documented, not detected. |

## Observability

Prometheus via `prometheus/client_golang`, `promhttp.Handler()` at `/metrics`.
The default registry also brings the Go and process collectors.

```
objectstore_requests_total{method,code}             counter
objectstore_request_duration_seconds{method}        histogram
objectstore_put_bytes_total                         counter
objectstore_dedup_hits_total                        counter
objectstore_blob_missing_total                      counter
objectstore_sweep_runs_total{result}                counter
objectstore_sweep_duration_seconds                  histogram
objectstore_sweep_last_success_timestamp_seconds    gauge
objectstore_sweep_deleted_total                     counter
objectstore_sweep_candidates                        gauge
objectstore_trash_files                             gauge
objectstore_trash_bytes                             gauge
objectstore_startup_orphans_deleted_total           counter
```

`trash_bytes` is the one to watch on a disk-pressure alert — it is deleted data
you are deliberately still paying for, and `-trash-retention` is the dial.

Request metrics come from `promhttp.InstrumentHandlerCounter` and
`InstrumentHandlerDuration` wrapping the mux — no hand-rolled middleware.

**Never label by bucket or object ID.** That is unbounded cardinality and the
standard way to take down a Prometheus server. Labels stay on `method` and
`code`.

Counters, not `COUNT(*)` gauges, so a scrape stays O(1) regardless of store
size. `sweep_candidates` is free — the sweeper already runs that anti-join.

Two alerts make the rest actionable:

- `objectstore_blob_missing_total > 0` — **page.** This is the one failure the
  entire locking design exists to prevent, so it should be identically zero
  forever. Nonzero means the invariant broke.
- `time() - objectstore_sweep_last_success_timestamp_seconds > 3 * interval` —
  the sweeper is wedged and disk is leaking.

`GET /healthz` → 200 when `SELECT 1` succeeds and the data dir is writable.

Logging: `log/slog`, structured. Errors and per-sweep summaries only — no
per-request access log, which at object-store rates is noise. Add behind a flag
if wanted.

## Files

Flat `package main` — a service this size needs no internal packages.

| File | Contents |
|---|---|
| `main.go` | flags, DB open, schema, mux, sweeper, startup walk, graceful shutdown |
| `store.go` | `Store`: `Put`, `Get`, `Delete`, `Sweep`, paths, validation |
| `handler.go` | handlers, status codes, size limit, entity-tag parsing (`net/http`'s is unexported) |
| `metrics.go` | collector definitions |
| `store_test.go`, `handler_test.go` | below |
| `go.mod` | `modernc.org/sqlite` (pure Go, no cgo), `prometheus/client_golang` |

```go
mux.HandleFunc("PUT    /objects/{bucket}/{objectID}", h.put)
mux.HandleFunc("GET    /objects/{bucket}/{objectID}", h.get)
mux.HandleFunc("DELETE /objects/{bucket}/{objectID}", h.delete)
mux.Handle("GET /metrics", promhttp.Handler())
mux.HandleFunc("GET /healthz", h.health)
```

**PRAGMAs go in the DSN, not `db.Exec`.** `busy_timeout`, `synchronous` and
`foreign_keys` are per-connection and `database/sql` pools connections, so an
`Exec` applies to whichever connection served it and silently not the others:

```
file:{data}/meta.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)
```

`foreign_keys` especially: SQLite silently ignores foreign keys when it is off,
which is the default — the constraint the locking design rests on would simply
not exist, with no error to say so.

### Configuration and defaults

Defined with `flag`, with an ~8-line fallback that reads `OBJECTSTORE_<NAME>`
for any flag not explicitly set (`flag.Visit` to find which were). Flags stay
the single source of truth, env works for containers, no config dependency.

| Setting | Default | Reasoning |
|---|---|---|
| `-max-object-size` | 32 MiB | The body streams to disk, so memory isn't the constraint, and the write lock is held across the rename, not the upload. Generous but defensible; it exists so one request cannot fill the volume. |
| `-sweep-interval` | 1 min | The candidate query is a sub-millisecond indexed anti-join, so a short interval is nearly free. Note it does **not** control when disk is freed — the sweep moves bytes to trash, so `-trash-retention` governs that. What it controls is how promptly the retention clock starts, and how fast `blobs` rows are cleaned under delete churn. |
| `-sweep-budget` | 10 s | Wall-clock cap on one sweep's delete loop. |
| `-tmp-max-age` | 1 h | **Must exceed the longest plausible upload.** The sweeper unlinking a live temp makes that PUT's rename fail with ENOENT. 32 MiB at 64 KiB/s is ~9 min, so this is ~6× margin. |
| `-trash-retention` | 24 h | How long reclaimed bytes survive. **The main disk-cost knob**: a delete-heavy workload holds a day of deleted data. GCS soft-delete defaults to 7 days; 24 h is a reasonable middle for a self-hosted single node. Set to 0 to unlink immediately and give up the safety net. |
| `busy_timeout` | 5 s | A write txn is a rename, an fsync and a commit — low milliseconds. Allows hundreds of queued writers before the 503 path. |
| bucket length | ≤ 63 | S3's limit, and well under any filesystem's. |
| objectID length | ≤ 255 | DB-only, but matches common filename limits in case it is ever used in a path. |
| `hh` fanout | 2 hex chars | 256 directories ≈ 4k files each at 1M blobs. Move to 4 chars past ~10M. |

Server timeouts, where the obvious setting is the wrong one:

- `ReadHeaderTimeout` **10 s** — Slowloris protection.
- `IdleTimeout` **120 s**.
- **No `WriteTimeout`** — it would abort a legitimate slow GET of a 32 MiB
  object mid-transfer.
- **No `ReadTimeout`** — same problem for slow uploads. `MaxBytesReader`
  bounds those by size instead, which is the right axis.

Plus `Shutdown` on SIGINT/SIGTERM.

## Packaging and deployment

Pure-Go SQLite means `CGO_ENABLED=0` and a fully static binary:

```dockerfile
FROM golang:1.23 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /objectstore .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /objectstore /objectstore
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/objectstore", "-data", "/data", "-addr", ":8080"]
```

~12 MB image. `distroless/static:nonroot` runs as UID 65532, so **the data
volume must be writable by that UID** — the usual first failure when deploying
this.

Mount one volume at `/data`. Mounting a volume at `/data/blobs` but not
`/data/tmp` puts them on different filesystems and every PUT would fail with
EXDEV; the startup device check catches that at boot rather than per request.

**Deployment: `strategy: Recreate`, one replica.** A `RollingUpdate` tries to
start the new pod before the old one releases the `ReadWriteOnce` volume and
the rollout wedges. That means a short downtime per deploy — the honest price
of local-disk storage, and it changes only when blob storage becomes shared.
`terminationGracePeriodSeconds` should exceed the time for an in-flight
max-size PUT. Probes: `/healthz` for liveness and readiness.

## Verification

`go test ./...` with `httptest` and a temp dir:

- PUT then GET returns the same bytes, correct `Content-Length`; HEAD works
- PUT twice to one path → GET returns the second body; 201 then 200
- **Dedup:** identical bytes to two IDs in one bucket → both GET correctly, and
  exactly one file exists under `blobs/`
- **Dedup safety:** DELETE one of the two → 404 for it, the other still returns
  its content
- **Split on overwrite:** `a` and `b` share a blob; PUT new content to `b` →
  `a` still returns the original bytes, `b` returns the new ones, two files
  exist, and neither is unlinked *(the corruption a naive dedup would cause)*
- **Scope:** identical bytes into two buckets → two blob files
- DELETE missing → 404; GET missing → 404
- Empty body: PUT succeeds, GET returns 200 and zero bytes
- Over the limit → 413, both with a truthful `Content-Length` and chunked
- Invalid names → 400: `..`, empty, overlong, `A` in a bucket, `%2F` in an ID
- **Sweeper:** an unreferenced blob loses its row and moves to `trash/` on the
  next sweep; a referenced one does not; the loop drains a backlog larger than
  `LIMIT` in one pass; a stale temp is removed and a fresh one is not
- **Trash retention:** a trash entry older than `-trash-retention` is unlinked
  and a newer one survives; with retention 0 the sweeper unlinks directly;
  renaming a trash file back into `blobs/` plus reinserting its row makes the
  object readable again *(the manual recovery path this exists for)*
- **Startup walk:** a file with no `blobs` row is moved to trash, not unlinked;
  one with a row is untouched; a file newer than process start is untouched;
  **a non-empty blob tree with an empty `blobs` table is left completely
  untouched**
- **FK is live:** `INSERT INTO objects` with no matching `blobs` row must fail.
  If it succeeds, `foreign_keys` is off and the locking design is not there
- **GET retry:** with a blob unlinked out from under a resolved row, GET
  returns 404 if the row was deleted, the new content if it was overwritten,
  and 500 + `blob_missing_total` only when the hash is unchanged

### Preconditions

- GET returns an `ETag`; re-requesting with `If-None-Match` gives 304 and an
  empty body; a stale tag gives 200 and the content; `*` and comma-separated
  lists both work
- **Lost update is actually prevented:** two clients read the same ETag, both
  PUT with `If-Match`; exactly one gets 2xx and the other gets 412, and the
  winner's bytes are what GET returns
- `If-Match` on a path that doesn't exist → **412, not 404**
- `If-None-Match: *` → 201 when absent, 412 when present, and the existing
  content is untouched after the 412
- `If-Match` on DELETE: mismatch → 412 and the object still readable; match →
  204
- A 412 leaves no temp file behind and creates no `blobs` row
- Oversize body **with** a failing `If-Match` → 412 is returned without the
  body being transferred (the advisory pre-check fired)
- **ABA:** content A → B → A, then a PUT with the original "A" tag succeeds
- Two IDs with identical content report the same ETag
- No precondition header → last-writer-wins, unchanged behaviour

### The invariant check

One helper, `assertConsistent(t, store)`, called at the end of every
concurrency test — this is where most of the value is:

1. every `objects` row has a readable file at its derived path
2. after a sweep, every file under `blobs/` has a `blobs` row
3. `blob_missing_total == 0`

Run the whole suite under `-race`.

### Concurrency and fault injection

- **Race harness:** N goroutines doing mixed PUT/GET/DELETE against one
  `httptest.Server`, including a goroutine PUTting content `X` in a loop while
  `Sweep` runs against an unreferenced blob `X`. Assert `assertConsistent` at
  the end.
- **Identical-content storm:** N goroutines PUT the same bytes to distinct IDs
  → no errors, one blob file, one `blobs` row.
- **Crash mid-PUT:** a test-only `beforeCommit func()` field on `Store`, nil in
  production, lets a test fail in the exact window between rename and commit.
  Assert a file exists with no row, and that the startup walk reclaims it —
  without subprocess machinery.
  ```
  // shortcut: a seam tests the crash window, not a real crash. It does not
  // cover power loss during fsync. Upgrade to a SIGKILLed subprocess if that
  // ever matters.
  ```
- **Disk full:** wrap the temp file in an `io.Writer` returning `syscall.ENOSPC`
  after N bytes → assert 507 and that the temp is removed. Portable, unlike a
  tmpfs, which needs Linux and root.
- **Lost database:** delete `meta.db` between two runs → assert every blob
  survives and the walk is skipped.
- `BenchmarkPut` with unique vs duplicate content, to catch regressions in the
  dedup path.

### Load testing

External, against the built binary — not a Go test:

```bash
oha -z 60s -m PUT -d @32kb.bin 'http://localhost:8080/objects/b1/obj-{}'
```

The question worth answering is whether `objectstore_sweep_candidates` stays
bounded under delete-heavy traffic. If it climbs monotonically, the sweeper
isn't keeping up and `-sweep-interval` or `-sweep-budget` needs changing.

End to end:

```bash
go run . -data /tmp/objstore &
curl -X PUT --data 'hello' localhost:8080/objects/b1/a   # 201
curl -X PUT --data 'hello' localhost:8080/objects/b1/b   # 201
find /tmp/objstore/blobs -type f                         # exactly one file
curl -X PUT --data 'world' localhost:8080/objects/b1/b   # 200
curl localhost:8080/objects/b1/a                         # hello  (unaffected)
curl localhost:8080/objects/b1/b                         # world
find /tmp/objstore/blobs -type f                         # now two files
curl -X DELETE localhost:8080/objects/b1/a               # 204
curl localhost:8080/objects/b1/b                         # world  (still)
curl -s localhost:8080/metrics | grep objectstore_dedup
```

## Deliberately not built

- **`If-Unmodified-Since` / `If-Modified-Since`.** The date-based variants of
  the same idea, and strictly weaker — one-second granularity means two writes
  in the same second are indistinguishable. We store no mtime per object
  anyway. Entity tags cover every case these would.
- **Soft delete on `objects`** (`deleted = true`, reaped later). The design
  already defers the expensive half: DELETE drops one row and one index entry
  immediately, and only the unlink of up to 32 MiB is deferred. Moving the
  tombstone up to `objects` costs *more* — `UPDATE` writes a new row version
  plus index maintenance and the reaper's `DELETE` writes another — while
  taxing every read with `AND NOT deleted` (a bug that fails open), pinning
  blobs until tombstones are reaped, and blocking re-PUT to a tombstoned path.
  **Where the technique is right is bucket deletion**: `DELETE FROM objects
  WHERE bucket = ?` across ten million rows is a long lock-holding
  transaction, and that wants `buckets.deleted_at` plus batched reaping.
- **A channel-fed cleanup worker.** Sound, but the periodic anti-join already
  does the job and its latency is `-sweep-interval`, a flag. If built: it
  cannot replace the periodic sweep (a channel isn't durable — a crash loses
  queued candidates and those blobs leak), and sends must be non-blocking so a
  DELETE never waits on cleanup.
- **Authentication.** Confirmed out of scope: the service sits behind something
  that handles it. If that changes, a static bearer token in middleware is ~20
  lines and no dependency; per-bucket credentials are a bigger change, because
  they need the bucket-metadata table that the normalization section identifies
  as the point where buckets stop being implicit.
- Bucket listing, object metadata, pagination, delete-bucket.
- Rate limiting, per-bucket quotas.
- Total-disk quota. The per-object cap stops one request filling the disk; it
  does not stop a million requests doing it.
- Global cross-bucket dedup — rejected in favour of bucket isolation.
- Schema migrations (`PRAGMA user_version`) and backups. `meta.db` is the
  single point of truth: lose it and every blob is unaddressable garbage.
- Empty bucket directories are left behind after the last object goes.
- Single-instance only. Two processes on one data dir are safe (SQLite locks,
  the sweep is transactional) but the second sweeper is wasted work.
