package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// schemaSQL is the whole mapping. There is no BLOB column: the database never
// stores object content, and the blob path is not stored either because it is
// a pure function of (bucket, hash) and a derived value cannot drift from
// where the file actually is.
//
// There is no refcount. A blob is live iff some objects row references it,
// which an anti-join can read directly; a count column would be free to drift,
// and a missed decrement leaks forever while a double decrement destroys live
// data.
const schemaSQL = `
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
`

// Config is the whole runtime configuration of a Store. main builds it from
// flags and environment.
type Config struct {
	DataDir        string
	MaxObjectSize  int64
	SweepInterval  time.Duration
	SweepBudget    time.Duration
	TmpMaxAge      time.Duration
	TrashRetention time.Duration
}

// ErrInvalidName is returned for a bucket or object ID that fails validation.
// The HTTP layer maps it to 400.
var ErrInvalidName = errors.New("invalid name")

// Buckets are lowercase-only because a bucket is a directory name: APFS is
// case-insensitive by default while SQLite TEXT keys are not, so "Photos" and
// "photos" would be two rows sharing one directory and dedup would merge
// across them on macOS but not on Linux.
var bucketRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// Object IDs never touch the filesystem, so they stay case-sensitive.
// Non-ASCII is excluded deliberately: NFC/NFD would give two rows for one
// visible name.
var objectIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

func validateBucket(b string) error {
	if !bucketRe.MatchString(b) {
		return fmt.Errorf("%w: bucket %q", ErrInvalidName, b)
	}
	return nil
}

func validateObjectID(id string) error {
	if !objectIDRe.MatchString(id) || id == "." || id == ".." {
		return fmt.Errorf("%w: object id %q", ErrInvalidName, id)
	}
	return nil
}

// Store maps (bucket, objectID) names onto content-addressed blobs on disk.
// The database is the only source of truth; the blob tree is immutable data.
type Store struct {
	cfg     Config
	db      *sql.DB
	dataDir string

	// Test seams, nil in production. hookBeforeCommit fires inside Put's
	// transaction after the blob is renamed into place; hookGetMiss fires in
	// Get between the failed open and the re-read of the row.
	hookBeforeCommit func()
	hookGetMiss      func()
}

// openDB opens meta.db with every per-connection pragma in the DSN. They
// cannot go in db.Exec: database/sql pools connections, so an Exec applies to
// whichever connection served it and silently not the others. foreign_keys
// especially — SQLite ignores foreign keys when it is off, which is the
// default, and the constraint the locking design rests on would simply not
// exist with no error to say so.
//
// _txlock=immediate makes every BeginTx a BEGIN IMMEDIATE. SQLite's default
// is deferred, which takes the write lock only at the first write and turns
// the in-transaction precondition checks back into check-then-act races.
func openDB(path string, busyTimeout time.Duration) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"file:%s?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=busy_timeout(%d)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)",
		path, busyTimeout.Milliseconds())
	return sql.Open("sqlite", dsn)
}

// Open prepares the data directory and the metadata database. Every failure
// here is a refusal to start: meta.db is the single source of truth and
// cannot be reconstructed from hash-named files.
func Open(cfg Config) (*Store, error) {
	s := &Store{cfg: cfg, dataDir: cfg.DataDir}

	blobs := filepath.Join(s.dataDir, "blobs")
	trash := filepath.Join(s.dataDir, "trash")
	for _, dir := range []string{blobs, s.tmpDir(), trash} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	if err := checkSameDevice(blobs, s.tmpDir(), trash); err != nil {
		return nil, err
	}

	db, err := openDB(filepath.Join(s.dataDir, "meta.db"), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("open meta.db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta.db unreadable: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	s.db = db
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for the health check and for tests.
func (s *Store) DB() *sql.DB { return s.db }

// checkSameDevice fails fast when the data directories straddle filesystems.
// os.Rename returns EXDEV across devices, so every PUT (tmp→blobs) or every
// reclaim (blobs→trash) would fail; one clear error at boot beats one per
// request. The usual cause is a volume mounted at /data/blobs but not /data.
func checkSameDevice(dirs ...string) error {
	var dev uint64
	for i, dir := range dirs {
		fi, err := os.Stat(dir)
		if err != nil {
			return fmt.Errorf("stat %s: %w", dir, err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			return nil // unsupported platform: skip rather than refuse to start
		}
		if i == 0 {
			dev = uint64(st.Dev)
			continue
		}
		if uint64(st.Dev) != dev {
			return fmt.Errorf("%s is on a different filesystem than %s: every rename would fail with EXDEV", dir, dirs[0])
		}
	}
	return nil
}

// blobDir is {data}/blobs/{bucket}/{hh}. The two-hex-character fanout keeps a
// single directory from reaching millions of entries.
func (s *Store) blobDir(bucket, hash string) string {
	return filepath.Join(s.dataDir, "blobs", bucket, hash[:2])
}

func (s *Store) blobPath(bucket, hash string) string {
	return filepath.Join(s.blobDir(bucket, hash), hash)
}

// trashDir keeps the bucket in its path for the same reason blobs does: dedup
// is per-bucket, so two buckets can legitimately hold the same hash, and a
// flat trash/{hash} would collide them and leave the documented recovery
// unable to tell which bucket a file belonged to.
func (s *Store) trashDir(bucket string) string {
	return filepath.Join(s.dataDir, "trash", bucket)
}

func (s *Store) trashPath(bucket, hash string) string {
	return filepath.Join(s.trashDir(bucket), hash)
}

func (s *Store) tmpDir() string {
	return filepath.Join(s.dataDir, "tmp")
}

var (
	// ErrNotFound means no objects row exists at the path. 404.
	ErrNotFound = errors.New("object not found")
	// ErrBlobMissing means a row resolved but its file is gone. The invariant
	// the locking design exists to protect has broken: 500, never a silent
	// repair, because destroying data on a read is worse than an error.
	ErrBlobMissing = errors.New("blob missing")
)

// PutResult describes what a Put did. Created drives 201-vs-200; Dedup says
// the bytes were already on disk under this bucket.
type PutResult struct {
	Hash    string
	Created bool
	Dedup   bool
	Size    int64
}

// Put stores the bytes of body at (bucket, objectID), overwriting whatever was
// there. Identical bytes already present in this bucket are stored once.
//
// The ordering is load-bearing: the bytes are made durable before the row
// commits, never the reverse. A crash in that window leaves an orphan file,
// which the sweeper collects; the reverse would leave a row pointing at
// nothing — a 500 for an object the client was told was stored.
func (s *Store) Put(ctx context.Context, bucket, objectID string, body io.Reader) (PutResult, error) {
	if err := validateBucket(bucket); err != nil {
		return PutResult{}, err
	}
	if err := validateObjectID(objectID); err != nil {
		return PutResult{}, err
	}

	tmp, err := os.CreateTemp(s.tmpDir(), "put-*")
	if err != nil {
		return PutResult{}, fmt.Errorf("create temp: %w", err)
	}
	// A no-op after a successful rename, and the thing that stops an aborted
	// upload leaking a file.
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	digest := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, digest), body)
	if err != nil {
		return PutResult{}, err // unwrapped: the handler classifies it
	}
	// Durable data, then a durable directory entry (see renameDurable).
	if err := tmp.Sync(); err != nil {
		return PutResult{}, fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return PutResult{}, fmt.Errorf("close temp: %w", err)
	}

	hash := hex.EncodeToString(digest.Sum(nil))
	res := PutResult{Hash: hash, Size: size}
	if err := os.MkdirAll(s.blobDir(bucket, hash), 0o755); err != nil {
		return PutResult{}, fmt.Errorf("create blob dir: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE, see openDB
	if err != nil {
		return PutResult{}, err
	}
	defer tx.Rollback()

	var current string
	err = tx.QueryRowContext(ctx,
		`SELECT hash FROM objects WHERE bucket = ? AND object_id = ?`,
		bucket, objectID).Scan(&current)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		res.Created = true
	case err != nil:
		return PutResult{}, err
	}

	// DO UPDATE rather than DO NOTHING: DO NOTHING takes no lock on the
	// existing row, and that lock is what blocks the sweeper from deleting
	// this blob between our stat and our commit.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO blobs (bucket, hash) VALUES (?, ?)
		 ON CONFLICT (bucket, hash) DO UPDATE SET hash = excluded.hash`,
		bucket, hash); err != nil {
		return PutResult{}, err
	}

	blobPath := s.blobPath(bucket, hash)
	switch _, err := os.Stat(blobPath); {
	case err == nil:
		res.Dedup = true // identical content by construction; leave the file
	case os.IsNotExist(err):
		if err := renameDurable(tmp.Name(), blobPath); err != nil {
			return PutResult{}, err
		}
	default:
		return PutResult{}, fmt.Errorf("stat blob: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO objects (bucket, object_id, hash) VALUES (?, ?, ?)
		 ON CONFLICT (bucket, object_id) DO UPDATE SET hash = excluded.hash`,
		bucket, objectID, hash); err != nil {
		return PutResult{}, err
	}

	if s.hookBeforeCommit != nil {
		s.hookBeforeCommit()
	}
	if err := tx.Commit(); err != nil {
		return PutResult{}, err
	}
	return res, nil
}

// Head returns the current content hash of an object.
func (s *Store) Head(ctx context.Context, bucket, objectID string) (string, error) {
	if err := validateBucket(bucket); err != nil {
		return "", err
	}
	if err := validateObjectID(objectID); err != nil {
		return "", err
	}
	return s.lookup(ctx, bucket, objectID)
}

func (s *Store) lookup(ctx context.Context, bucket, objectID string) (string, error) {
	var hash string
	err := s.db.QueryRowContext(ctx,
		`SELECT hash FROM objects WHERE bucket = ? AND object_id = ?`,
		bucket, objectID).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return hash, err
}

// Get opens the blob an object resolves to. The file is the caller's to close.
// Once it is open a sweeper unlink cannot break the transfer: on POSIX an open
// fd stays valid after the directory entry is gone.
func (s *Store) Get(ctx context.Context, bucket, objectID string) (*os.File, string, error) {
	if err := validateBucket(bucket); err != nil {
		return nil, "", err
	}
	if err := validateObjectID(objectID); err != nil {
		return nil, "", err
	}

	hash, err := s.lookup(ctx, bucket, objectID)
	if err != nil {
		return nil, "", err
	}
	f, err := os.Open(s.blobPath(bucket, hash))
	if err == nil {
		return f, hash, nil
	}
	if !os.IsNotExist(err) {
		return nil, "", err
	}

	// The file vanished between the row read and the open. Re-read the row
	// once: this resolves the sweeper race rather than making it unlikely,
	// which is why the sweeper needs no grace period. Error path only, so the
	// happy path pays nothing.
	if s.hookGetMiss != nil {
		s.hookGetMiss()
	}
	second, err := s.lookup(ctx, bucket, objectID)
	if err != nil {
		return nil, "", err // concurrently deleted: ErrNotFound
	}
	if second == hash {
		return nil, "", fmt.Errorf("%w: %s/%s hash %s", ErrBlobMissing, bucket, objectID, hash)
	}
	f, err = os.Open(s.blobPath(bucket, second))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", fmt.Errorf("%w: %s/%s hash %s", ErrBlobMissing, bucket, objectID, second)
		}
		return nil, "", err
	}
	return f, second, nil
}

// renameDurable moves src onto dst and makes the new directory entry durable.
// Syncing the data alone is not enough: power loss could drop the rename while
// the committed row survives.
func renameDurable(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("rename blob into place: %w", err)
	}
	return fsyncDir(filepath.Dir(dst))
}

func fsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", path, err)
	}
	return nil
}
