package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// testDatabaseURL is the live Postgres every test needs. It fails rather than
// skips: a silently skipped concurrency suite is how this port ships broken.
func testDatabaseURL(tb testing.TB) string {
	tb.Helper()
	u := os.Getenv("OBJECTSTORE_TEST_DATABASE_URL")
	if u == "" {
		tb.Fatal("OBJECTSTORE_TEST_DATABASE_URL is unset; these tests need a live Postgres, e.g. " +
			"OBJECTSTORE_TEST_DATABASE_URL=postgres://objectstore:objectstore@localhost:15432/objectstore")
	}
	return u
}

// newTestSchema creates a private schema and returns a URL pointing every
// connection's search_path at it. pgx forwards unrecognised query parameters
// as startup RuntimeParams, so this covers every connection the pool opens
// and not merely the first — TestSearchPathAppliesToEveryPooledConnection
// pins that.
func newTestSchema(tb testing.TB) string {
	tb.Helper()
	base := testDatabaseURL(tb)
	name := fmt.Sprintf("test_%016x", rand.Uint64())
	quoted := pgx.Identifier{name}.Sanitize()

	execOnDatabase(tb, base, "CREATE SCHEMA "+quoted)
	tb.Cleanup(func() { execOnDatabase(tb, base, "DROP SCHEMA "+quoted+" CASCADE") })

	u, err := url.Parse(base)
	if err != nil {
		tb.Fatalf("parse %s: %v", base, err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// execOnDatabase runs one statement on a connection of its own, outside any
// Store's pool.
func execOnDatabase(tb testing.TB, databaseURL, sql string) {
	tb.Helper()
	ctx := context.WithoutCancel(tb.Context()) // cleanup runs after the test context is cancelled
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		tb.Fatalf("exec %q: %v", sql, err)
	}
}

// newTestConfig gives a Store a fresh temp dir and a private Postgres schema,
// with production-shaped defaults. Tests that care about a specific duration
// override it.
func newTestConfig(tb testing.TB) Config {
	tb.Helper()
	return Config{
		DataDir:        tb.TempDir(),
		DatabaseURL:    newTestSchema(tb),
		MaxObjectSize:  32 << 20,
		SweepInterval:  time.Minute,
		SweepBudget:    10 * time.Second,
		TmpMaxAge:      time.Hour,
		TrashRetention: 24 * time.Hour,
		MaxTrashBytes:  2 << 30,
	}
}

func newTestStore(tb testing.TB) *Store {
	tb.Helper()
	return openTestStore(tb, newTestConfig(tb))
}

func openTestStore(tb testing.TB, cfg Config) *Store {
	tb.Helper()
	s, err := Open(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(s.Close)
	return s
}

// TestSchemaIsolation: two stores from newTestStore share a database but must
// not share rows, or every test would see every other test's objects.
func TestSchemaIsolation(t *testing.T) {
	a, b := newTestStore(t), newTestStore(t)
	mustPut(t, a, "b1", "only-in-a", "hello")

	var n int
	if err := b.Pool().QueryRow(t.Context(), `SELECT count(*) FROM objects`).Scan(&n); err != nil {
		t.Fatalf("count objects: %v", err)
	}
	if n != 0 {
		t.Errorf("the second store sees %d objects rows, want 0 — the schemas are not isolated", n)
	}
}

// TestSearchPathAppliesToEveryPooledConnection: search_path arrives in the
// startup packet, so it must hold on every connection the pool opens, not
// just the one that ran the schema DDL.
func TestSearchPathAppliesToEveryPooledConnection(t *testing.T) {
	s := newTestStore(t)
	const n = 16
	schemas := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.Pool().QueryRow(t.Context(), `SELECT current_schema()`).Scan(&schemas[i])
		}()
	}
	wg.Wait()

	// Without this the test can pass vacuously: the pool only opens a
	// connection when Acquire finds no idle one, so 16 goroutines that happen
	// to serialise all run on the same connection and prove nothing about the
	// risk this test exists to rule out.
	if n := s.Pool().Stat().NewConnsCount(); n < 2 {
		t.Errorf("the pool opened %d connection(s); this test only means something across several", n)
	}

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if schemas[i] != schemas[0] {
			t.Errorf("goroutine %d ran in schema %q, goroutine 0 in %q", i, schemas[i], schemas[0])
		}
	}
	if !strings.HasPrefix(schemas[0], "test_") {
		t.Errorf("current_schema() = %q, want the per-test test_* schema", schemas[0])
	}
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
	for _, name := range []string{"blobs", "tmp", "trash"} {
		if _, err := os.Stat(filepath.Join(s.dataDir, name)); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
}

// insertOrphanObject inserts an objects row whose (bucket, hash) has no blobs
// parent. It must always fail: the foreign key is what blocks a PUT from
// adopting a hash the sweeper is reclaiming, and the whole PUT/sweeper design
// rests on it being enforced.
func insertOrphanObject(ctx context.Context, s *Store, objectID string) error {
	_, err := s.Pool().Exec(ctx,
		`INSERT INTO objects (bucket, object_id, hash) VALUES ($1, $2, $3)`,
		"b1", objectID, strings.Repeat("a", 64))
	return err
}

func TestForeignKeysAreLive(t *testing.T) {
	s := newTestStore(t)
	if err := insertOrphanObject(t.Context(), s, "o1"); err == nil {
		t.Fatal("insert of an objects row with no blobs parent succeeded; the foreign key is not enforced")
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
