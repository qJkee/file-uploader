package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/dbtest"
	"github.com/qjkee/objectstore/pkg/etag"
	"github.com/qjkee/objectstore/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// assertConsistent checks the three invariants the whole design exists to
// hold. This is where most of the value of the concurrency tests is.
//
// blobMissingBefore is the counter's value when the test started: the
// collector is process-global, so only the delta is this test's business.
func assertConsistent(t *testing.T, s *Store, blobMissingBefore float64) {
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
		if _, err := os.Stat(s.blobPath(bucket, hash)); err != nil {
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

func TestIdenticalContentStorm(t *testing.T) {
	before := testutil.ToFloat64(metrics.BlobMissingTotal)
	s := newTestStore(t)
	const n = 32

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.Put(t.Context(), "b1", fmt.Sprintf("obj-%d", i),
				strings.NewReader("identical bytes"), etag.Precondition{})
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Put %d: %v", i, err)
		}
	}
	if got := countBlobFiles(t, s); got != 1 {
		t.Errorf("blob files = %d, want 1", got)
	}
	var blobs int
	if err := s.Pool().QueryRow(t.Context(), `SELECT count(*) FROM blobs`).Scan(&blobs); err != nil {
		t.Fatal(err)
	}
	if blobs != 1 {
		t.Errorf("blobs rows = %d, want 1", blobs)
	}
	assertConsistent(t, s, before)
}

// TestCrashMidPut fails in the exact window between the rename and the
// commit, where crash-safety ordering says we must leave an orphan file
// rather than a row pointing at nothing.
//
// shortcut: the seam tests the crash window, not a real crash. It does not
// cover power loss during fsync. Upgrade to a SIGKILLed subprocess if that
// ever matters.
func TestCrashMidPut(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "survivor", "survivor") // the orphan walk guard needs a row

	s.hookBeforeCommit = func() { panic("simulated crash between rename and commit") }
	func() {
		defer func() { _ = recover() }()
		s.Put(t.Context(), "b1", "doomed", strings.NewReader("doomed bytes"), etag.Precondition{})
	}()
	s.hookBeforeCommit = nil

	orphan := s.blobPath("b1", hashOf("doomed bytes"))
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("the renamed blob is gone: %v — bytes must be durable before the row commits", err)
	}
	if _, err := s.Head(t.Context(), "b1", "doomed"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Head = %v, want ErrNotFound — the row must not have committed", err)
	}

	n, err := s.SweepOrphans(t.Context(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if n != 1 {
		t.Errorf("reclaimed = %d, want 1", n)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("the crash orphan was not reclaimed: %v", err)
	}
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

// TestLostDatabase is the disaster the orphan walk's guard exists for: the
// metadata is gone, so every blob must be left exactly where it is.
func TestLostDatabase(t *testing.T) {
	cfg := newTestConfig(t)

	first, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 3 {
		mustPut(t, first, "b1", fmt.Sprintf("o%d", i), fmt.Sprintf("body-%d", i))
	}
	beforeFiles := countBlobFiles(t, first)
	first.Close()

	// The metadata is gone: a wrong -database-url, a bad restore, a dropped
	// schema. Open recreates an empty one, which is the dangerous case.
	dbtest.ExecOnDatabase(t, cfg.DatabaseURL, `DROP TABLE objects, blobs`)

	second, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()

	n, err := second.SweepOrphans(t.Context(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if n != 0 {
		t.Errorf("reclaimed = %d, want 0 — the walk must be skipped", n)
	}
	if got := countBlobFiles(t, second); got != beforeFiles {
		t.Errorf("blob files = %d, want %d — lost metadata must destroy nothing", got, beforeFiles)
	}
}

func BenchmarkPut(b *testing.B) {
	run := func(b *testing.B, unique bool) {
		s := newTestStore(b)
		payload := strings.Repeat("x", 4096)

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			body := payload
			if unique {
				body = fmt.Sprintf("%s-%d", payload, i)
			}
			if _, err := s.Put(b.Context(), "b1", fmt.Sprintf("o%d", i),
				strings.NewReader(body), etag.Precondition{}); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("unique", func(b *testing.B) { run(b, true) })
	b.Run("duplicate", func(b *testing.B) { run(b, false) })
}
