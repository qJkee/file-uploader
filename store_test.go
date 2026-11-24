package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// newTestStore opens a Store on a fresh temp dir with production-shaped
// defaults. Tests that care about a specific duration override cfg first.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Config{
		DataDir:        t.TempDir(),
		MaxObjectSize:  32 << 20,
		SweepInterval:  time.Minute,
		SweepBudget:    10 * time.Second,
		TmpMaxAge:      time.Hour,
		TrashRetention: 24 * time.Hour,
		MaxTrashBytes:  2 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestValidateBucket(t *testing.T) {
	accept := []string{"b1", "a", "0", "a.b-c_d", strings.Repeat("a", 63)}
	reject := []string{
		"", "A", "Photos", "-a", "_a", ".a", ".", "..", "a/b", "a b",
		"a%2Fb", strings.Repeat("a", 64), "é", "a\x00b",
	}

	for _, b := range accept {
		if err := validateBucket(b); err != nil {
			t.Errorf("validateBucket(%q) = %v, want nil", b, err)
		}
	}
	for _, b := range reject {
		err := validateBucket(b)
		if !errors.Is(err, ErrInvalidName) {
			t.Errorf("validateBucket(%q) = %v, want ErrInvalidName", b, err)
		}
	}
}

func TestValidateObjectID(t *testing.T) {
	accept := []string{"a", "A", "0", "A.b-c_d", "a..b", strings.Repeat("a", 255)}
	reject := []string{
		"", ".", "..", "a/b", "a b", "a%2Fb", strings.Repeat("a", 256),
		"café", "a\x00b",
	}

	for _, id := range accept {
		if err := validateObjectID(id); err != nil {
			t.Errorf("validateObjectID(%q) = %v, want nil", id, err)
		}
	}
	for _, id := range reject {
		err := validateObjectID(id)
		if !errors.Is(err, ErrInvalidName) {
			t.Errorf("validateObjectID(%q) = %v, want ErrInvalidName", id, err)
		}
	}
}

func TestBlobPathFanout(t *testing.T) {
	s := &Store{dataDir: "/d"}
	const hash = "ab12cd34"

	if got, want := s.blobDir("b1", hash), "/d/blobs/b1/ab"; got != want {
		t.Errorf("blobDir = %q, want %q", got, want)
	}
	if got, want := s.blobPath("b1", hash), "/d/blobs/b1/ab/ab12cd34"; got != want {
		t.Errorf("blobPath = %q, want %q", got, want)
	}
	if got, want := s.trashDir("b1"), "/d/trash/b1"; got != want {
		t.Errorf("trashDir = %q, want %q", got, want)
	}
	if got, want := s.trashPath("b1", hash), "/d/trash/b1/ab12cd34"; got != want {
		t.Errorf("trashPath = %q, want %q", got, want)
	}
	if got, want := s.tmpDir(), "/d/tmp"; got != want {
		t.Errorf("tmpDir = %q, want %q", got, want)
	}
}

func TestOpenCreatesLayout(t *testing.T) {
	s := newTestStore(t)
	for _, name := range []string{"blobs", "tmp", "trash", "meta.db"} {
		if _, err := os.Stat(filepath.Join(s.dataDir, name)); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
}

// insertOrphanObject inserts an objects row whose (bucket, hash) has no blobs
// parent. It must always fail: if it succeeds, foreign_keys is off on that
// connection and the whole PUT/sweeper locking design is not there.
func insertOrphanObject(ctx context.Context, s *Store, objectID string) error {
	_, err := s.DB().ExecContext(ctx,
		`INSERT INTO objects (bucket, object_id, hash) VALUES (?, ?, ?)`,
		"b1", objectID, strings.Repeat("a", 64))
	return err
}

func TestForeignKeysAreLive(t *testing.T) {
	s := newTestStore(t)
	if err := insertOrphanObject(t.Context(), s, "o1"); err == nil {
		t.Fatal("insert of an objects row with no blobs parent succeeded; foreign_keys is off")
	}
}

func TestForeignKeysOnEveryPooledConnection(t *testing.T) {
	s := newTestStore(t)
	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = insertOrphanObject(t.Context(), s, "o"+strconv.Itoa(i))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			t.Errorf("goroutine %d: insert succeeded; foreign_keys is off on that connection", i)
		}
	}
}

func TestCheckSameDevice(t *testing.T) {
	dir := t.TempDir()
	if err := checkSameDevice(dir, dir); err != nil {
		t.Errorf("checkSameDevice(dir, dir) = %v, want nil", err)
	}

	if deviceOf(t, dir) == deviceOf(t, "/dev") {
		t.Skip("/dev is on the same device as the temp dir")
	}
	if err := checkSameDevice(dir, "/dev"); err == nil {
		t.Error("checkSameDevice across devices = nil, want an error")
	}
}

func deviceOf(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return uint64(fi.Sys().(*syscall.Stat_t).Dev)
}

// TestTxLockIsImmediate proves the DSN's _txlock=immediate took effect. A
// deferred transaction holds no write lock until its first write, so the
// second connection's insert would succeed and the design's in-transaction
// precondition checks would be check-then-act races again.
func TestTxLockIsImmediate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")

	db1, err := openDB(path, 0)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db1.Close()
	if _, err := db1.Exec(schemaSQL); err != nil {
		t.Fatalf("schema: %v", err)
	}

	db2, err := openDB(path, 0)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db2.Close()

	tx, err := db1.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()

	_, err = db2.ExecContext(t.Context(),
		`INSERT INTO blobs (bucket, hash) VALUES ('b1', 'deadbeef')`)
	if err == nil {
		t.Fatal("write on a second connection succeeded while a transaction was open; _txlock=immediate is not in effect")
	}
}
