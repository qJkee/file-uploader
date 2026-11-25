# ObjectStore — Design Document

## 1. Overview

ObjectStore is a content-addressed, single-node object store. It maps
human-readable object names to SHA-256 blob hashes, stores blobs on the local
filesystem, and keeps the name→hash index in PostgreSQL.

The primary design goals are:

1. **Crash safety** — a power failure never leaves a database row pointing at a
   missing file.
2. **Simplicity** — a single binary, one PostgreSQL database, one filesystem
   volume.
3. **Automatic deduplication** — identical content uploaded under different names
   shares a single blob on disk.

---

## 2. Architecture

### 2.1 Component Diagram

<!-- See architecture-overview diagram -->

```
┌─────────────────────────────────────────────────────────┐
│                     HTTP Client                         │
└───────────────┬─────────────────────────┬───────────────┘
                │ PUT/GET/DELETE          │ GET /metrics
                │ /objects/{b}/{id}       │
        ┌───────▼───────┐         ┌──────▼──────┐
        │   pkg/api     │         │ pkg/metrics  │
        │   Handler     │────────►│ Prometheus   │
        └───────┬───────┘         └─────────────┘
                │
        ┌───────▼───────┐
        │  pkg/store    │
        │    Store      │
        └──┬─────────┬──┘
           │         │
   ┌───────▼──┐  ┌───▼──────────┐
   │PostgreSQL│  │  Filesystem  │
   │ metadata │  │    blobs     │
   └──────────┘  └──────────────┘
           ▲         ▲
        ┌──┴─────────┴──┐
        │  Sweeper      │
        │  (background) │
        └───────────────┘
```

### 2.2 Package Responsibilities

| Package | Responsibility |
|---------|---------------|
| `cmd/object-store` | Entry point. Parses flags (with `OBJECTSTORE_*` env fallback), opens the store, starts the HTTP server, and handles graceful shutdown on `SIGINT`/`SIGTERM`. |
| `pkg/api` | HTTP routing and handler logic. Translates HTTP requests into `Store` method calls and maps `Store` errors to HTTP status codes. Instruments every request with Prometheus. |
| `pkg/store` | Core storage engine. Manages the `Put`/`Get`/`Delete`/`Head` lifecycle, content hashing, filesystem layout, and database transactions. |
| `pkg/store/sweeper` | Background goroutine. Reclaims unreferenced blobs, walks for orphan files, expires trash after a retention window, and removes stale temp files. |
| `pkg/etag` | Parses `If-Match` and `If-None-Match` header values and evaluates RFC 9110 preconditions against a stored ETag. |
| `pkg/metrics` | Declares Prometheus counters, histograms, and gauges for HTTP requests, store operations, blob/object counts, and sweep activity. |
| `pkg/dbtest` | Test helper that creates ephemeral PostgreSQL databases per test for isolation. |

---

## 3. Data Model

### 3.1 Database Schema

```
┌──────────────────────────┐       ┌─────────────────────────────────┐
│         blobs            │       │           objects                │
├──────────────────────────┤       ├─────────────────────────────────┤
│ bucket  TEXT  (PK)       │◄──FK──│ bucket     TEXT  (PK)           │
│ hash    TEXT  (PK)       │       │ object_id  TEXT  (PK)           │
│                          │       │ hash       TEXT  → blobs(hash)  │
└──────────────────────────┘       └─────────────────────────────────┘
```

- **`blobs`**: Tracks which content hashes physically exist on disk for a given
  bucket. One row per unique blob.
- **`objects`**: Maps `(bucket, object_id)` names to content hashes. The foreign
  key to `blobs` guarantees every name points at a real blob.

### 3.2 Filesystem Layout

```
{data}/
├── blobs/{bucket}/{hh}/{hash}    ← content-addressed blobs
│                                    hh = first 2 hex chars (fanout)
├── tmp/                           ← in-flight uploads (atomic rename)
└── trash/{bucket}/{hash}          ← soft-deleted blobs awaiting expiry
```

**Why fanout?** A flat directory with millions of entries degrades filesystem
performance. The two-character prefix creates up to 256 subdirectories per
bucket, keeping each at a manageable size.

---

## 4. Write Path (PUT)

### 4.1 Sequence

<!-- See put-flow diagram -->

1. The client sends `PUT /objects/{bucket}/{objectID}` with the body.
2. The handler validates bucket/objectID names and size limits.
3. `Store.Put` streams the body to a temp file, computing the SHA-256 hash as
   it writes.
4. **Dedup check**: query `blobs` for the computed hash.
   - *Hit*: the blob already exists on disk — delete the temp file, skip to
     step 6.
   - *Miss*: atomically rename the temp file into the content-addressed path
     (`blobs/{bucket}/{hh}/{hash}`), then `INSERT INTO blobs`.
5. **Bytes are now durable.** If the process crashes here, the blob row exists
   but no `objects` row points to it yet — the sweeper will eventually reclaim
   it.
6. Acquire `pg_advisory_xact_lock(bucket, objectID)` to serialize concurrent
   writes to the same key.
7. `INSERT ... ON CONFLICT UPDATE` into `objects`, pointing the name at the new
   hash. If this replaces an old hash, the old blob becomes unreferenced
   (reclaimed by the sweeper).
8. Commit. Return `201 Created` (new) or `200 OK` (overwrite) with the ETag.

### 4.2 Crash Safety Invariant

> **Bytes durable before row commits.**
>
> A crash between step 4 and step 7 leaves an orphan blob file on disk. The
> sweeper will find it and move it to trash. A crash *never* leaves a database
> row pointing at a missing file, because the blob row is committed first.

### 4.3 Content Deduplication

If two objects in the same bucket happen to contain identical bytes, they share
a single blob on disk. When the second PUT computes the same hash, it finds the
existing `blobs` row and skips the filesystem write entirely. Both `objects`
rows point to the same `(bucket, hash)`.

---

## 5. Read Path (GET)

1. Query `objects` for `(bucket, objectID)` → get the hash.
2. Open the blob file at `blobs/{bucket}/{hh}/{hash}`.
3. Evaluate `If-None-Match` precondition against the ETag (`sha256:{hash}`).
   Return `304 Not Modified` on match.
4. Stream the file to the client. `Content-Length` and `ETag` headers are set.
   `Range` requests are supported via `http.ServeContent`.

If the blob file is missing (possible during a concurrent sweep), the store
retries once after a short pause to handle the race between GET and the
sweeper.

---

## 6. Delete Path

1. Acquire `pg_advisory_xact_lock(bucket, objectID)`.
2. `DELETE FROM objects WHERE bucket = ? AND object_id = ?`.
3. Return `204 No Content`.

The blob is **not** immediately removed from disk. The sweeper reclaims it
later (see §7), which avoids holding the transaction open during a potentially
slow filesystem delete.

---

## 7. Sweeper

The sweeper is a background goroutine that runs periodically (default: every
minute) with a bounded time budget (default: 10 seconds per cycle).

### 7.1 Responsibilities

| Task | Description |
|------|-------------|
| **Reclaim unreferenced blobs** | Anti-join: `SELECT FROM blobs WHERE NOT EXISTS (SELECT 1 FROM objects WHERE ...)`. Move each unreferenced blob to `trash/`, then `DELETE FROM blobs`. |
| **Expire trash** | Permanently delete trash files older than `trash-retention` (default 24h). Also enforces `max-trash-bytes` by evicting the oldest files first. |
| **Orphan walk** | Walk `blobs/` on disk and find files with no matching `blobs` row. These are crash orphans from interrupted PUTs. Move them to `trash/`. |
| **Clean tmp** | Remove temp files in `tmp/` older than `tmp-max-age` (default 1h). These are abandoned uploads from crashed or timed-out clients. |

### 7.2 Why No Reference Counting?

A reference count (`blobs.refcount`) is simpler in theory but drifts in
practice. A crash between decrementing the refcount and removing the file
leaves the count wrong. Rebuilding accurate refcounts requires a full table
scan anyway.

The anti-join approach treats the `objects` table as the **single source of
truth**. A blob is live if and only if at least one `objects` row references
it. This is re-evaluated from scratch on every sweep cycle, so it is
self-healing.

### 7.3 Trash as a Safety Net

Reclaimed blobs are not deleted immediately. They are moved to a `trash/`
directory with a retention period (default 24 hours). This provides a window
to recover from bugs in the sweep logic or accidental deletes.

---

## 8. Concurrency Control

### 8.1 Advisory Locks

Writes to the same `(bucket, objectID)` are serialized using
`pg_advisory_xact_lock`. The lock key is derived from the bucket and object ID.
This prevents:

- **Lost updates**: two concurrent PUTs to the same key could race on the
  `INSERT ... ON CONFLICT` and produce inconsistent state.
- **Delete-during-PUT**: a DELETE and a PUT to the same key execute in a
  defined order.

Advisory locks are automatically released when the transaction commits or
rolls back.

### 8.2 Sweeper Safety

The sweeper operates outside of per-object locks. Its safety comes from
ordering:

1. **Reclaim** runs an anti-join query in a consistent snapshot. If a PUT
   commits between the query and the file move, the blob row will exist and
   the move will be a no-op (the file is in use).
2. **Trash** provides a grace period. Even if a blob is incorrectly swept, it
   remains in trash for 24 hours.

---

## 9. HTTP Preconditions (RFC 9110)

The `pkg/etag` package implements `If-Match` and `If-None-Match` precondition
evaluation per RFC 9110 §13.1.

| Header | PUT | GET | DELETE |
|--------|-----|-----|--------|
| `If-Match: "sha256:..."` | Only overwrite if the current ETag matches | — | Only delete if ETag matches |
| `If-None-Match: *` | Create-only (412 if exists) | — | — |
| `If-None-Match: "sha256:..."` | — | 304 Not Modified if ETag matches | — |

This enables optimistic concurrency for clients that want compare-and-swap
semantics.

---

## 10. Observability

### 10.1 Prometheus Metrics

All metrics are exposed at `GET /metrics`.

| Metric | Type | Description |
|--------|------|-------------|
| `objectstore_http_requests_total` | Counter | Requests by method, path, status |
| `objectstore_http_request_duration_seconds` | Histogram | Request latency |
| `objectstore_store_operations_total` | Counter | Store ops by operation and result |
| `objectstore_store_operation_duration_seconds` | Histogram | Store op latency |
| `objectstore_blobs_total` | Gauge | Current blob count |
| `objectstore_objects_total` | Gauge | Current object count |
| `objectstore_sweep_*` | Various | Sweep cycle counts, durations, items reclaimed |

### 10.2 Logging

Structured log output via the standard library `log/slog`. Key events:

- Server start/stop
- Sweep cycle results
- Error conditions (database, filesystem)

---

## 11. Deployment

### 11.1 Docker

Multi-stage build producing a `scratch` image:

```dockerfile
FROM golang:1.27 AS build
# ... build with CGO_ENABLED=0 ...
FROM scratch
USER 65532:65532
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/objectstore"]
```

The `scratch` base and pure-Go `pgx` driver mean zero C dependencies and a
minimal attack surface.

### 11.2 Docker Compose

```yaml
services:
  postgres:
    image: postgres:17-alpine
  objectstore:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      OBJECTSTORE_DATABASE_URL: postgres://objectstore:objectstore@postgres:5432/objectstore
    volumes:
      - objdata:/data
```

### 11.3 Volume Requirements

- Mount **one** volume at `/data`. The `blobs/`, `tmp/`, and `trash/`
  subdirectories must reside on the same filesystem (atomic rename requirement).
- The volume must be writable by UID 65532 (the non-root container user).

---

## 12. Limitations and Future Work

| Limitation | Notes |
|------------|-------|
| Single-node only | No replication or clustering. Suitable for single-server deployments. |
| No bucket management API | Buckets are created implicitly on first PUT. No list/delete bucket API. |
| No object listing | There is no `GET /objects/{bucket}` to list objects in a bucket. |
| No multipart upload | Large files are uploaded as a single stream. |
| No authentication | The HTTP API has no auth layer. Deploy behind a reverse proxy for access control. |
| Dedup is per-bucket | Identical blobs in *different* buckets are stored separately. |
