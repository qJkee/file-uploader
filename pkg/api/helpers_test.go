package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/dbtest"
	"github.com/qjkee/objectstore/pkg/store"
)

// newTestConfig gives a Store a fresh temp dir and a private Postgres schema,
// with production-shaped defaults. Tests that need a different limit open
// their own Store rather than mutating a live one.
func newTestConfig(tb testing.TB) store.Config {
	tb.Helper()
	return store.Config{
		DataDir:        tb.TempDir(),
		DatabaseURL:    dbtest.NewSchemaURL(tb),
		MaxObjectSize:  32 << 20,
		SweepInterval:  time.Minute,
		SweepBudget:    10 * time.Second,
		TmpMaxAge:      time.Hour,
		TrashRetention: 24 * time.Hour,
		MaxTrashBytes:  2 << 30,
	}
}

func newTestStore(tb testing.TB) *store.Store {
	tb.Helper()
	return openTestStore(tb, newTestConfig(tb))
}

func openTestStore(tb testing.TB, cfg store.Config) *store.Store {
	tb.Helper()
	s, err := store.Open(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(s.Close)
	return s
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return n
}

func hashOf(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func newTestServerWithConfig(t *testing.T, cfg store.Config) *testServer {
	t.Helper()
	s := openTestStore(t, cfg)
	srv := httptest.NewServer(NewHandler(s).Mux())
	t.Cleanup(srv.Close)
	return &testServer{Server: srv, store: s}
}

// unlinkBlob removes the file a resolved row points at, simulating a sweeper
// unlink that landed between the row read and the open.
func unlinkBlob(t *testing.T, s *store.Store, bucket, body string) {
	t.Helper()
	if err := os.Remove(s.BlobPath(bucket, hashOf(body))); err != nil {
		t.Fatalf("unlink blob: %v", err)
	}
}

// walkFiles visits every regular file under root. A local copy of the
// store's own walk: the invariant check needs it, and exporting an internal
// helper purely for a test would widen the package's surface for no
// production caller.
func walkFiles(ctx context.Context, root string, fn func(path string, info fs.FileInfo) error) error {
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		return fn(path, info)
	})
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

// parseBlobPath reverses the documented layout blobs/{bucket}/{hh}/{hash}.
func parseBlobPath(root, path string) (bucket, hash string, ok bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", "", false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 3 {
		return "", "", false
	}
	bucket, hh, hash := parts[0], parts[1], parts[2]
	if len(hash) < 2 || hh != hash[:2] {
		return "", "", false
	}
	return bucket, hash, true
}
