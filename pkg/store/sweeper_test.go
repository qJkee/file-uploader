package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/etag"
	"github.com/qjkee/objectstore/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func trashRoot(s *Store) string { return filepath.Join(s.DataDir(), "trash") }

func mustSweep(t *testing.T, s *Store) SweepResult {
	t.Helper()
	res, err := s.Sweep(t.Context())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	return res
}

// withSweepLimit lowers the candidate batch size so a backlog test does not
// have to create a thousand blobs.
func withSweepLimit(t *testing.T, n int) {
	t.Helper()
	old := sweepLimit
	sweepLimit = n
	t.Cleanup(func() { sweepLimit = old })
}

func TestSweepMovesUnreferencedBlobToTrash(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	if err := s.Delete(t.Context(), "b1", "a", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	res := mustSweep(t, s)

	if res.Reclaimed != 1 {
		t.Errorf("Reclaimed = %d, want 1", res.Reclaimed)
	}
	if got := countBlobFiles(t, s); got != 0 {
		t.Errorf("blob files = %d, want 0", got)
	}
	if _, err := os.Stat(s.trashPath("b1", hashOf("hello"))); err != nil {
		t.Errorf("blob is not in trash: %v — reclaim must rename, not unlink", err)
	}
	var n int
	if err := s.Pool().QueryRow(t.Context(), `SELECT count(*) FROM blobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("blobs rows = %d, want 0", n)
	}
}

func TestSweepKeepsReferencedBlob(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")

	res := mustSweep(t, s)

	if res.Reclaimed != 0 {
		t.Errorf("Reclaimed = %d, want 0", res.Reclaimed)
	}
	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("a = %q, want %q", got, "hello")
	}
}

// TestSweepDrainsBacklogLargerThanLimit guards the loop: without it a burst of
// deletes reclaims one batch per interval and takes days to catch up.
func TestSweepDrainsBacklogLargerThanLimit(t *testing.T) {
	withSweepLimit(t, 10)
	s := newTestStore(t)
	const n = 25
	for i := range n {
		id := fmt.Sprintf("o%d", i)
		mustPut(t, s, "b1", id, fmt.Sprintf("body-%d", i))
		if err := s.Delete(t.Context(), "b1", id, etag.Precondition{}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	res := mustSweep(t, s)

	if res.Reclaimed != n {
		t.Errorf("Reclaimed = %d, want %d — one sweep must drain the backlog", res.Reclaimed, n)
	}
	if got := countBlobFiles(t, s); got != 0 {
		t.Errorf("blob files = %d, want 0", got)
	}
}

func TestSweepBudgetStops(t *testing.T) {
	withSweepLimit(t, 10)
	s := newTestStore(t)
	s.cfg.SweepBudget = time.Nanosecond
	const n = 25
	for i := range n {
		id := fmt.Sprintf("o%d", i)
		mustPut(t, s, "b1", id, fmt.Sprintf("body-%d", i))
		if err := s.Delete(t.Context(), "b1", id, etag.Precondition{}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	res := mustSweep(t, s)

	if res.Reclaimed >= n {
		t.Errorf("Reclaimed = %d, want fewer than %d — the budget did not stop the loop", res.Reclaimed, n)
	}
}

func TestTrashRetention(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TrashRetention = time.Hour
	mustPut(t, s, "b1", "old", "old-body")
	mustPut(t, s, "b1", "new", "new-body")
	for _, id := range []string{"old", "new"} {
		if err := s.Delete(t.Context(), "b1", id, etag.Precondition{}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	mustSweep(t, s)

	stale := s.trashPath("b1", hashOf("old-body"))
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	res := mustSweep(t, s)

	if res.TrashDeleted != 1 {
		t.Errorf("TrashDeleted = %d, want 1", res.TrashDeleted)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale trash entry survived: %v", err)
	}
	if _, err := os.Stat(s.trashPath("b1", hashOf("new-body"))); err != nil {
		t.Errorf("fresh trash entry was deleted: %v", err)
	}
	if res.TrashFiles != 1 {
		t.Errorf("TrashFiles = %d, want 1", res.TrashFiles)
	}
	if res.TrashBytes != int64(len("new-body")) {
		t.Errorf("TrashBytes = %d, want %d", res.TrashBytes, len("new-body"))
	}
}

func TestTrashRetentionZeroUnlinks(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TrashRetention = 0
	mustPut(t, s, "b1", "a", "hello")
	if err := s.Delete(t.Context(), "b1", "a", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	res := mustSweep(t, s)

	if res.Reclaimed != 1 {
		t.Errorf("Reclaimed = %d, want 1", res.Reclaimed)
	}
	if got := countBlobFiles(t, s); got != 0 {
		t.Errorf("blob files = %d, want 0", got)
	}
	if got := countFiles(t, trashRoot(s)); got != 0 {
		t.Errorf("trash files = %d, want 0 — retention 0 must unlink directly", got)
	}
}

func TestSweepOnEmptyStore(t *testing.T) {
	s := newTestStore(t)
	if err := os.RemoveAll(trashRoot(s)); err != nil {
		t.Fatalf("remove trash: %v", err)
	}

	res, err := s.Sweep(t.Context())
	if err != nil {
		t.Fatalf("Sweep on an empty store with no trash dir: %v", err)
	}
	if res.Reclaimed != 0 || res.TrashFiles != 0 {
		t.Errorf("res = %+v, want zeroes", res)
	}
}

func TestTmpCleanup(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TmpMaxAge = time.Hour

	stale := filepath.Join(s.tmpDir(), "put-stale")
	fresh := filepath.Join(s.tmpDir(), "put-fresh")
	for _, p := range []string{stale, fresh} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	res := mustSweep(t, s)

	if res.TmpDeleted != 1 {
		t.Errorf("TmpDeleted = %d, want 1", res.TmpDeleted)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale temp survived: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh temp was removed: %v — it may be a live upload", err)
	}
}

// writeOrphanBlob drops a file into the blob tree with no blobs row — what a
// crash between the rename and the commit leaves behind.
func writeOrphanBlob(t *testing.T, s *Store, bucket, body string) string {
	t.Helper()
	hash := hashOf(body)
	path := s.blobPath(bucket, hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	return path
}

func TestOrphanWalkMovesUnknownBlobToTrash(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "kept") // the guard needs a non-empty blobs table
	orphan := writeOrphanBlob(t, s, "b1", "orphan")

	n, err := s.SweepOrphans(t.Context(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if n != 1 {
		t.Errorf("reclaimed = %d, want 1", n)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan survived the walk: %v", err)
	}
	if _, err := os.Stat(s.trashPath("b1", hashOf("orphan"))); err != nil {
		t.Errorf("orphan is not in trash: %v — the walk must rename, not unlink", err)
	}
}

func TestOrphanWalkKeepsKnownBlob(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")

	if _, err := s.SweepOrphans(t.Context(), time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("a = %q, want %q", got, "hello")
	}
}

// TestOrphanWalkSkipsFilesNewerThanStart covers the race with in-flight PUTs,
// which are creating blobs while the walk runs.
func TestOrphanWalkSkipsFilesNewerThanStart(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "kept")
	orphan := writeOrphanBlob(t, s, "b1", "orphan")

	n, err := s.SweepOrphans(t.Context(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if n != 0 {
		t.Errorf("reclaimed = %d, want 0", n)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Errorf("a blob newer than process start was reclaimed: %v", err)
	}
}

// TestOrphanWalkSkippedWhenBlobsTableEmpty is the guard that stops a lost or
// replaced metadata database from deleting every object on disk. It is
// unrecoverable in principle: blob filenames are hashes, so even with the
// bytes intact the object IDs that pointed at them can never be reconstructed.
func TestOrphanWalkSkippedWhenBlobsTableEmpty(t *testing.T) {
	s := newTestStore(t)
	first := writeOrphanBlob(t, s, "b1", "one")
	second := writeOrphanBlob(t, s, "b2", "two")

	n, err := s.SweepOrphans(t.Context(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if n != 0 {
		t.Errorf("reclaimed = %d, want 0 — the walk must not run against an empty blobs table", n)
	}
	for _, p := range []string{first, second} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("blob destroyed with an empty blobs table: %v", err)
		}
	}
}

// TestTrashRecoveryPath exercises the manual recovery the trash window exists
// for: rename the file back and reinsert its rows.
func TestTrashRecoveryPath(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	if err := s.Delete(t.Context(), "b1", "a", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustSweep(t, s)

	hash := hashOf("hello")
	if err := os.MkdirAll(s.blobDir("b1", hash), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Rename(s.trashPath("b1", hash), s.blobPath("b1", hash)); err != nil {
		t.Fatalf("rename back: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(),
		`INSERT INTO blobs (bucket, hash) VALUES ('b1', $1)`, hash); err != nil {
		t.Fatalf("reinsert blob row: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(),
		`INSERT INTO objects (bucket, object_id, hash) VALUES ('b1', 'a', $1)`, hash); err != nil {
		t.Fatalf("reinsert object row: %v", err)
	}

	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("recovered object = %q, want %q", got, "hello")
	}
}

// TestReclaimSkipsAdoptedBlob exercises the race where a concurrent PUT holds
// an uncommitted objects row that blocks the sweeper's DELETE, then commits
// while the DELETE waits. Under READ COMMITTED the DELETE's NOT EXISTS reads a
// snapshot from before the commit, but the FK trigger sees current data. This
// raises SQLSTATE 23503. The correct response is (false, nil): the blob was
// adopted, leave it.
func TestReclaimSkipsAdoptedBlob(t *testing.T) {
	s := newTestStore(t)
	hash := hashOf("adopted")

	// Create a blob candidate: a blobs row and file, but no objects row.
	mustPut(t, s, "b1", "temp", "adopted")
	if err := s.Delete(t.Context(), "b1", "temp", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Open a transaction and insert the objects row, but do not commit yet.
	// This holds a FOR KEY SHARE lock on the blobs row.
	blocker, err := s.Pool().Begin(t.Context())
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	defer blocker.Rollback(t.Context())

	var blockerPID int
	if err := blocker.QueryRow(t.Context(), `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("get blocker backend pid: %v", err)
	}

	if _, err := blocker.Exec(t.Context(),
		`INSERT INTO objects (bucket, object_id, hash) VALUES ('b1', 'adopted', $1)`,
		hash); err != nil {
		t.Fatalf("insert uncommitted objects row: %v", err)
	}

	// Run the sweep in a goroutine. The DELETE will block on the row lock.
	done := make(chan SweepResult)
	sweepErr := make(chan error, 1)
	go func() {
		res, err := s.Sweep(t.Context())
		sweepErr <- err
		done <- res
	}()

	// Poll until a backend is blocked specifically by our blocker transaction.
	// The 2s deadline is safely under lock_timeout=5s: if the poll gives up,
	// the test proved nothing, but it fails cleanly rather than racing the
	// timeout and surfacing as a confusing 55P03.
	deadline := time.Now().Add(2 * time.Second)
	foundBlocked := false
	for time.Now().Before(deadline) {
		var blocked int
		if err := s.Pool().QueryRow(t.Context(),
			`SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))`,
			blockerPID).Scan(&blocked); err != nil {
			t.Fatalf("poll pg_stat_activity: %v", err)
		}
		if blocked > 0 {
			foundBlocked = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !foundBlocked {
		t.Fatal("deadline expired: the DELETE was never observed blocked on a lock — the race did not trigger, so this test proved nothing")
	}

	// Commit the blocker. The DELETE unblocks, and the FK trigger sees the
	// now-committed objects row -> 23503.
	if err := blocker.Commit(t.Context()); err != nil {
		t.Fatalf("commit blocker: %v", err)
	}

	// Wait for the sweep to complete.
	if err := <-sweepErr; err != nil {
		// If lock_timeout fired before the blocker committed, this is a test
		// configuration error, not a sweep bug.
		if strings.Contains(err.Error(), "55P03") {
			t.Fatalf("Sweep: lock_timeout beat the test deadline — lower lock_timeout or raise poll deadline: %v", err)
		}
		t.Fatalf("Sweep: %v", err)
	}
	res := <-done

	// The blob was adopted, so it must survive.
	if _, err := os.Stat(s.blobPath("b1", hash)); err != nil {
		t.Errorf("blob file is missing: %v — a 23503 must leave the file alone", err)
	}
	var blobExists bool
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM blobs WHERE bucket = 'b1' AND hash = $1)`,
		hash).Scan(&blobExists); err != nil {
		t.Fatalf("check blobs row: %v", err)
	}
	if !blobExists {
		t.Error("blobs row was deleted despite the foreign-key violation")
	}

	// A 23503 that surfaced as an error would be the bug: the sweeper must
	// treat it as a lost race, not a failure.
	if res.Failed != 0 {
		t.Errorf("Failed = %d, want 0 — a 23503 must return (false, nil), not an error", res.Failed)
	}
	if res.Reclaimed != 0 {
		t.Errorf("Reclaimed = %d, want 0 — the blob was adopted", res.Reclaimed)
	}

	// Verify the object is readable via the adopted row.
	if got := mustGet(t, s, "b1", "adopted"); got != "adopted" {
		t.Errorf("adopted object = %q, want %q", got, "adopted")
	}
}

func TestSweepMetrics(t *testing.T) {
	s := newTestStore(t)
	before := testutil.ToFloat64(metrics.SweepDeletedTotal)
	beforeRuns := testutil.ToFloat64(metrics.SweepRunsTotal.WithLabelValues("success"))
	mustPut(t, s, "b1", "a", "hello")
	if err := s.Delete(t.Context(), "b1", "a", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	mustSweep(t, s)

	if got := testutil.ToFloat64(metrics.SweepDeletedTotal) - before; got != 1 {
		t.Errorf("sweep_deleted_total delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.SweepRunsTotal.WithLabelValues("success")) - beforeRuns; got != 1 {
		t.Errorf("sweep_runs_total{success} delta = %v, want 1", got)
	}
	if testutil.ToFloat64(metrics.SweepLastSuccess) == 0 {
		t.Error("sweep_last_success_timestamp_seconds was never set")
	}
}
