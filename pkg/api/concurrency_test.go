package api

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/metrics"
	"github.com/qjkee/objectstore/pkg/store"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// assertConsistent checks the three invariants the whole design exists to
// hold. This is where most of the value of the concurrency tests is.
//
// blobMissingBefore is the counter's value when the test started: the
// collector is process-global, so only the delta is this test's business.
func assertConsistent(t *testing.T, s *store.Store, blobMissingBefore float64) {
	t.Helper()

	// 1. Every objects row has a readable file at its derived path.
	rows, err := s.Pool().Query(t.Context(), `SELECT bucket, object_id, hash FROM objects`)
	if err != nil {
		t.Fatalf("query objects: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var bucket, objectID, hash string
		if err := rows.Scan(&bucket, &objectID, &hash); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if _, err := os.Stat(s.BlobPath(bucket, hash)); err != nil {
			t.Errorf("%s/%s resolves to %s but the file is unreadable: %v",
				bucket, objectID, hash, err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	// 2. After a sweep, every file under blobs/ has a blobs row.
	if _, err := s.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	blobsRoot := filepath.Join(s.DataDir(), "blobs")
	err = walkFiles(t.Context(), blobsRoot, func(path string, _ fs.FileInfo) error {
		bucket, hash, ok := parseBlobPath(blobsRoot, path)
		if !ok {
			t.Errorf("unexpected file in the blob tree: %s", path)
			return nil
		}
		var known bool
		if err := s.Pool().QueryRow(t.Context(),
			`SELECT EXISTS (SELECT 1 FROM blobs WHERE bucket = $1 AND hash = $2)`,
			bucket, hash).Scan(&known); err != nil {
			return err
		}
		if !known {
			t.Errorf("blob %s/%s survived a sweep with no blobs row", bucket, hash)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk blobs: %v", err)
	}

	// 3. The invariant the locking design protects never broke.
	if got := testutil.ToFloat64(metrics.BlobMissingTotal) - blobMissingBefore; got != 0 {
		t.Errorf("blob_missing_total rose by %v during this test, want 0", got)
	}
}

// TestRaceHarness runs mixed traffic against one server while the sweeper
// chases the blob a PUT loop keeps recreating — the exact PUT/sweeper race
// the foreign key exists to close.
func TestRaceHarness(t *testing.T) {
	before := testutil.ToFloat64(metrics.BlobMissingTotal)
	ts := newTestServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/objects/b1/obj-%d", i)
			for n := 0; ctx.Err() == nil && n < 40; n++ {
				ts.do(t, http.MethodPut, path, strings.NewReader(fmt.Sprintf("body-%d-%d", i, n)), nil)
				ts.do(t, http.MethodGet, path, nil, nil)
				if n%3 == 0 {
					ts.do(t, http.MethodDelete, path, nil, nil)
				}
			}
		}()
	}

	// One contender PUTs the same bytes over and over while the sweeper runs
	// against exactly that hash.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; ctx.Err() == nil && n < 100; n++ {
			ts.do(t, http.MethodPut, "/objects/b1/contended", strings.NewReader("X"), nil)
			ts.do(t, http.MethodDelete, "/objects/b1/contended", nil, nil)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; ctx.Err() == nil && n < 100; n++ {
			if _, err := ts.store.Sweep(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("Sweep: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	assertConsistent(t, ts.store, before)
}

type errReader struct {
	remaining int
	err       error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, r.err
	}
	n := min(len(p), r.remaining)
	r.remaining -= n
	return n, nil
}

func cancelAfter(cancel context.CancelFunc) error {
	cancel()
	return context.Canceled
}
