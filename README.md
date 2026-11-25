# ObjectStore

A content-addressed, single-node object store written in Go. It provides an
S3-like HTTP API backed by PostgreSQL for metadata and the local filesystem for
blob storage. Objects are automatically deduplicated within a bucket using
SHA-256 hashing.

## Architecture

```
┌────────────┐      HTTP       ┌────────────┐
│   Client   │ ──────────────► │  pkg/api   │
└────────────┘                 │  Handler   │
                               └─────┬──────┘
                                     │
                               ┌─────▼──────┐
                               │ pkg/store  │
                               │   Store    │
                               └──┬──────┬──┘
                                  │      │
                    ┌─────────────▼┐  ┌──▼───────────┐
                    │  PostgreSQL  │  │  Filesystem   │
                    │  (metadata)  │  │  (blobs)      │
                    └──────────────┘  └───────────────┘
```

## Features

- **Content-addressed storage** — blobs are named by their SHA-256 hash,
  giving automatic deduplication within each bucket
- **Crash-safe writes** — bytes are durable on disk before the database row
  commits; a crash leaves at most an orphan file, never a dangling pointer
- **HTTP preconditions** — `If-Match` / `If-None-Match` (RFC 9110) for
  conditional uploads, downloads, and deletes
- **Background sweeper** — periodically reclaims unreferenced blobs, expires
  trash, and cleans abandoned uploads
- **Prometheus metrics** — request counts, latencies, blob/object gauges, and
  sweep statistics at `/metrics`
- **Minimal footprint** — ships as a single static binary in a `scratch`
  Docker image (non-root, UID 65532)

## Quick Start

### Docker Compose (recommended)

```bash
docker compose up --build
```

This starts PostgreSQL 17 and the object store on `localhost:8080`.

### From source

```bash
# Prerequisites: Go 1.27+, a running PostgreSQL instance
go build -o objectstore ./cmd/object-store

./objectstore \
  -data /var/lib/objectstore \
  -database-url "postgres://user:pass@localhost:5432/objectstore"
```

## API Reference

| Method   | Path                             | Description             | Success |
|----------|----------------------------------|-------------------------|---------|
| `PUT`    | `/objects/{bucket}/{objectID}`   | Upload or overwrite     | `200` / `201` |
| `GET`    | `/objects/{bucket}/{objectID}`   | Download                | `200` / `304` |
| `HEAD`   | `/objects/{bucket}/{objectID}`   | Metadata only           | `200` |
| `DELETE` | `/objects/{bucket}/{objectID}`   | Delete (lazy reclaim)   | `204` |
| `GET`    | `/metrics`                       | Prometheus metrics      | `200` |

### Upload

```bash
curl -X PUT --data-binary @photo.jpg \
  http://localhost:8080/objects/images/photo.jpg
# → 201 Created
# ETag: "sha256:a1b2c3..."
```

### Download

```bash
curl http://localhost:8080/objects/images/photo.jpg -o photo.jpg
```

### Conditional upload (create-only, don't overwrite)

```bash
curl -X PUT --data-binary @photo.jpg \
  -H 'If-None-Match: *' \
  http://localhost:8080/objects/images/photo.jpg
# → 412 Precondition Failed (if the object already exists)
```

### Delete

```bash
curl -X DELETE http://localhost:8080/objects/images/photo.jpg
# → 204 No Content
```

## Configuration

All flags have an `OBJECTSTORE_` environment variable fallback
(e.g. `-data` → `OBJECTSTORE_DATA`).

| Flag                | Env Override                    | Default  | Description |
|---------------------|---------------------------------|----------|-------------|
| `-data`             | `OBJECTSTORE_DATA`              | *(required)* | Root directory for blobs, tmp, and trash |
| `-database-url`     | `OBJECTSTORE_DATABASE_URL`      | *(required)* | PostgreSQL connection string |
| `-addr`             | `OBJECTSTORE_ADDR`              | `:8080`  | Listen address |
| `-max-object-size`  | `OBJECTSTORE_MAX_OBJECT_SIZE`   | `32MB`   | Maximum upload size |
| `-sweep-interval`   | `OBJECTSTORE_SWEEP_INTERVAL`    | `1m`     | How often the sweeper runs |
| `-sweep-budget`     | `OBJECTSTORE_SWEEP_BUDGET`      | `10s`    | Max wall-clock time per sweep cycle |
| `-tmp-max-age`      | `OBJECTSTORE_TMP_MAX_AGE`       | `1h`     | Abandoned uploads older than this are removed |
| `-trash-retention`  | `OBJECTSTORE_TRASH_RETENTION`   | `24h`    | How long reclaimed blobs sit in trash before permanent deletion |
| `-max-trash-bytes`  | `OBJECTSTORE_MAX_TRASH_BYTES`   | `2GB`    | Trash size cap (oldest evicted first) |

## Filesystem Layout

```
{data}/
├── blobs/{bucket}/{hh}/{hash}   # Content-addressed blobs (hh = first 2 hex chars)
├── tmp/                          # In-flight uploads
└── trash/{bucket}/{hash}         # Reclaimed blobs awaiting permanent deletion
```

> **Important:** Mount a single volume at `/data`. Putting `blobs/` and `tmp/`
> on different filesystems causes `EXDEV` errors (cross-device rename). The
> service validates this at startup.

## Project Structure

```
cmd/object-store/           Entry point, flag parsing, server lifecycle
pkg/
├── api/                    HTTP handlers, routing, error→status mapping
│   └── handler.go
├── store/                  Core storage engine
│   ├── store.go            Put / Get / Delete / Head, content addressing
│   └── sweeper.go          Background reclaim, orphan walk, trash expiry
├── etag/                   ETag parsing and precondition evaluation (RFC 9110)
├── metrics/                Prometheus counter/histogram/gauge definitions
└── dbtest/                 Test helpers (ephemeral PostgreSQL databases)
```

## Database Schema

```sql
CREATE TABLE blobs (
    bucket  TEXT NOT NULL,
    hash    TEXT NOT NULL,
    PRIMARY KEY (bucket, hash)
);

CREATE TABLE objects (
    bucket    TEXT NOT NULL,
    object_id TEXT NOT NULL,
    hash      TEXT NOT NULL REFERENCES blobs (bucket, hash),
    PRIMARY KEY (bucket, object_id)
);
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| [pgx/v5](https://github.com/jackc/pgx) | Pure-Go PostgreSQL driver (no CGO required) |
| [prometheus/client_golang](https://github.com/prometheus/client_golang) | Metrics instrumentation |

## License

See [LICENSE](LICENSE) for details.
