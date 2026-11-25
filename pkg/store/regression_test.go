package store

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/etag"
	"github.com/qjkee/objectstore/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestOrphanWalkDoesNotStealAnAdoptedBlob covers the window between the
// walk's "is this blob known?" read and its rename into trash. A PUT of
// exactly that content — the idempotent retry the spec invites after a crash
// — commits a row in that window, and the walk must not then move the file
// out from under it.
func TestOrphanWalkDoesNotStealAnAdoptedBlob(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "survivor", "survivor") // the guard needs a non-empty blobs table

	const body = "bytes from the crashed put"
	orphan := writeOrphanBlob(t, s, "b1", body)
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(orphan, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	var once sync.Once
	s.hookOrphanWindow = func() {
		once.Do(func() { mustPut(t, s, "b1", "retried", body) })
	}

	n, err := s.SweepOrphans(t.Context(), time.Now())
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if n != 0 {
		t.Errorf("reclaimed = %d, want 0 — the blob was adopted by a concurrent PUT", n)
	}
	if got := mustGet(t, s, "b1", "retried"); got != body {
		t.Errorf("Get = %q, want %q", got, body)
	}
}

// TestOrphanWalkDoesNotStealABlobAdoptedMidTransaction covers the second,
// narrower window: the one *inside* reclaimOrphan's transaction, between its
// decision that the blob is unknown and the rename into trash.
//
// TestOrphanWalkDoesNotStealAnAdoptedBlob above cannot reach it.
// hookOrphanWindow fires before the transaction opens, so the racing PUT has
// already committed by the time the transaction starts and any re-check at
// all catches it. Under SQLite's `_txlock=immediate` that was the only
// interleaving there was: the whole transaction was mutually exclusive with
// every writer. Under Postgres a plain SELECT takes no row lock and there is
// no row to lock, so the PUT can commit *after* the check and still have its
// bytes renamed away — which is why reclaimOrphan now claims the row instead
// of reading it.
func TestOrphanWalkDoesNotStealABlobAdoptedMidTransaction(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "survivor", "survivor") // the guard needs a non-empty blobs table

	const body = "bytes from the crashed put"
	orphan := writeOrphanBlob(t, s, "b1", body)
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(orphan, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Buffered and never cancelled: if an assertion below aborts the test
	// while the PUT is still blocked, the goroutine must not wedge on a send.
	raced := make(chan error, 1)
	ctx := context.WithoutCancel(t.Context())
	var once sync.Once
	s.hookOrphanRecheck = func() {
		once.Do(func() {
			go func() {
				_, err := s.Put(ctx, "b1", "retried", strings.NewReader(body), etag.Precondition{})
				raced <- err
			}()
			// Hand control back to the rename only once that PUT is
			// observably blocked on our claim. The 2s bound is safely under
			// lock_timeout=5s: giving up fails the test cleanly instead of
			// surfacing as a confusing 55P03.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-raced:
					raced <- err // the assertions below still want it
					t.Error("the racing PUT committed without ever blocking: " +
						"reclaimOrphan is not holding the blobs key, so the rename " +
						"below is about to steal a live object's bytes")
					return
				default:
				}
				var blocked int
				if err := s.Pool().QueryRow(ctx,
					`SELECT count(*) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0`,
				).Scan(&blocked); err != nil {
					t.Errorf("poll pg_stat_activity: %v", err)
					return
				}
				if blocked > 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Error("deadline expired: the racing PUT was never observed blocked, so this test proved nothing")
		})
	}

	n, err := s.SweepOrphans(t.Context(), time.Now())
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	// The claim is taken before the hook, so the sweeper always wins this
	// ordering. A 0 here would mean the PUT got in first and the hard window
	// was never entered.
	if n != 1 {
		t.Fatalf("reclaimed = %d, want 1 — the sweeper claimed the row first, so it must win", n)
	}

	if err := <-raced; err != nil {
		t.Fatalf("racing PUT: %v", err)
	}
	// The whole point: the client was told 201, so the object must be
	// readable, with its own bytes. Losing the race must cost the PUT a
	// re-write of the file, never the object.
	if got := mustGet(t, s, "b1", "retried"); got != body {
		t.Errorf("Get = %q, want %q", got, body)
	}
}

// safeBuffer collects log output from the server's goroutines.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLogs(t *testing.T) *safeBuffer {
	t.Helper()
	buf := &safeBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// TestSweepSurvivesOneUnmovableBlob: a single file the sweeper cannot move
// must not stop reclamation of every other blob, nor the trash and temp
// passes. Otherwise one bad permission leaks disk without bound.
func TestSweepSurvivesOneUnmovableBlob(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: chmod does not deny access")
	}
	s := newTestStore(t)
	s.cfg.TrashRetention = time.Hour

	for _, body := range []string{"stuck", "movable"} {
		mustPut(t, s, "b1", body, body)
		if err := s.Delete(t.Context(), "b1", body, etag.Precondition{}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	stale := filepath.Join(s.tmpDir(), "put-stale")
	if err := os.WriteFile(stale, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatal(err)
	}

	stuckDir := s.blobDir("b1", hashOf("stuck"))
	if err := os.Chmod(stuckDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(stuckDir, 0o755) })

	res, err := s.Sweep(t.Context())
	if err == nil {
		t.Error("Sweep = nil error, want the unmovable blob reported")
	}

	if res.Reclaimed != 1 {
		t.Errorf("Reclaimed = %d, want 1 — the movable blob must still be reclaimed", res.Reclaimed)
	}
	if _, statErr := os.Stat(stale); !os.IsNotExist(statErr) {
		t.Errorf("stale temp survived a sweep that hit an unmovable blob: %v", statErr)
	}
}

func TestMissingAncestors(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "b1", "ab")

	got, err := missingAncestors(target)
	if err != nil {
		t.Fatalf("missingAncestors: %v", err)
	}
	want := []string{filepath.Join(root, "b1"), target}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("missingAncestors(%q) = %v, want %v", target, got, want)
	}

	got, err = missingAncestors(root)
	if err != nil {
		t.Fatalf("missingAncestors: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("missingAncestors on an existing dir = %v, want none", got)
	}
}

// TestTrashRetentionClockStartsAtReclaim: rename(2) preserves mtime, so a
// trashed file keeps the time it was *written*, not the time it became
// garbage. Without resetting it, any blob older than the retention window is
// unlinked by the very same sweep that trashed it — the recovery net is
// absent for exactly the long-lived objects that need it.
func TestTrashRetentionClockStartsAtReclaim(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TrashRetention = time.Hour

	const body = "uploaded long ago"
	mustPut(t, s, "b1", "a", body)
	hash := hashOf(body)
	long := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(s.blobPath("b1", hash), long, long); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := s.Delete(t.Context(), "b1", "a", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	res := mustSweep(t, s)

	if res.Reclaimed != 1 {
		t.Fatalf("Reclaimed = %d, want 1", res.Reclaimed)
	}
	if _, err := os.Stat(s.trashPath("b1", hash)); err != nil {
		t.Errorf("a blob reclaimed seconds ago is already gone from trash: %v", err)
	}
	if res.TrashDeleted != 0 {
		t.Errorf("TrashDeleted = %d, want 0 — the retention clock must start at reclaim", res.TrashDeleted)
	}
}

// TestPeriodicSweepCollectsRowlessOrphans: a rollback after the blob is
// renamed into place leaves a file with no blobs row. The reclaim anti-join
// only sees blobs that *have* a row, so without a periodic orphan pass that
// file leaks for the whole life of the process — no crash required.
func TestPeriodicSweepCollectsRowlessOrphans(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TmpMaxAge = time.Hour
	mustPut(t, s, "b1", "survivor", "survivor") // the empty-table guard needs a row

	orphan := writeOrphanBlob(t, s, "b1", "leaked by a rolled-back put")
	long := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(orphan, long, long); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	res := mustSweep(t, s)

	if res.Orphans != 1 {
		t.Errorf("Orphans = %d, want 1", res.Orphans)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("a row-less blob survived a periodic sweep: %v", err)
	}
	if got := mustGet(t, s, "b1", "survivor"); got != "survivor" {
		t.Errorf("survivor = %q — the pass must not touch referenced blobs", got)
	}
}

// TestPeriodicSweepSparesRecentOrphans: an in-flight PUT has already renamed
// its blob into place and has not committed yet. The pass must not race it.
func TestPeriodicSweepSparesRecentOrphans(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TmpMaxAge = time.Hour
	mustPut(t, s, "b1", "survivor", "survivor")
	orphan := writeOrphanBlob(t, s, "b1", "written just now")

	res := mustSweep(t, s)

	if res.Orphans != 0 {
		t.Errorf("Orphans = %d, want 0", res.Orphans)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Errorf("a blob younger than TmpMaxAge was reclaimed: %v", err)
	}
}

// TestSweepMetricsSurviveAPartialFailure: the three passes deliberately run
// independently, so one unmovable file must not discard the counts of
// everything that did succeed. sweep_deleted_total is a counter — a skipped
// increment is lost permanently.
func TestSweepMetricsSurviveAPartialFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: chmod does not deny access")
	}
	s := newTestStore(t)
	s.cfg.TrashRetention = time.Hour
	before := testutil.ToFloat64(metrics.SweepDeletedTotal)

	for _, body := range []string{"stuck", "movable"} {
		mustPut(t, s, "b1", body, body)
		if err := s.Delete(t.Context(), "b1", body, etag.Precondition{}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	stuckDir := s.blobDir("b1", hashOf("stuck"))
	if err := os.Chmod(stuckDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(stuckDir, 0o755) })

	if _, err := s.Sweep(t.Context()); err == nil {
		t.Fatal("Sweep = nil error, want the unmovable blob reported")
	}

	if got := testutil.ToFloat64(metrics.SweepDeletedTotal) - before; got != 1 {
		t.Errorf("sweep_deleted_total delta = %v, want 1 — the successful reclaim was discarded", got)
	}
}

// --- audit follow-ups ---

// TestConnectionPoolIsBounded: every blocked writer holds a connection, so
// an unbounded pool lets connection count track concurrency with nothing
// capping memory.
func TestConnectionPoolIsBounded(t *testing.T) {
	s := newTestStore(t)
	if got := s.Pool().Stat().MaxConns(); got != maxConns {
		t.Errorf("MaxConns = %d, want %d", got, maxConns)
	}
}

// TestOrphanWalkUsesBulkLookups: the walk must not cost one query per file.
// Almost every file in a healthy store is known, so the per-known-file cost
// is what dominates an hourly walk of a million blobs.
func TestKnownHashesLooksUpABatch(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "one")
	mustPut(t, s, "b1", "b", "two")
	mustPut(t, s, "b2", "c", "three") // another bucket must not leak in

	got, err := s.knownHashes(t.Context(), "b1", []string{
		hashOf("one"), hashOf("two"), hashOf("three"), hashOf("absent"),
	})
	if err != nil {
		t.Fatalf("knownHashes: %v", err)
	}

	for _, body := range []string{"one", "two"} {
		if !got[hashOf(body)] {
			t.Errorf("%q should be known in b1", body)
		}
	}
	for _, body := range []string{"three", "absent"} {
		if got[hashOf(body)] {
			t.Errorf("%q should not be known in b1", body)
		}
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
}

// TestOrphanWalkBatchesAcrossTheBoundary runs more files than one batch
// holds, so a bug in the flush-the-remainder path shows up.
func TestOrphanWalkBatchesAcrossTheBoundary(t *testing.T) {
	old := orphanBatchSize
	orphanBatchSize = 4
	t.Cleanup(func() { orphanBatchSize = old })

	s := newTestStore(t)
	const known = 11
	for i := range known {
		mustPut(t, s, "b1", fmt.Sprintf("o%d", i), fmt.Sprintf("body-%d", i))
	}
	orphan := writeOrphanBlob(t, s, "b1", "not referenced")

	n, err := s.SweepOrphans(t.Context(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if n != 1 {
		t.Errorf("reclaimed = %d, want 1", n)
	}
	if _, statErr := os.Stat(orphan); !os.IsNotExist(statErr) {
		t.Errorf("orphan survived: %v", statErr)
	}
	for i := range known {
		if got := mustGet(t, s, "b1", fmt.Sprintf("o%d", i)); got != fmt.Sprintf("body-%d", i) {
			t.Fatalf("known blob %d was reclaimed or damaged: %q", i, got)
		}
	}
}

// TestTrashSizeCapEvictsOldestFirst: retention alone cannot bound disk when
// the same object is uploaded and deleted repeatedly — every cycle leaves
// another copy in trash and none expire for a day.
func TestTrashSizeCapEvictsOldestFirst(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TrashRetention = time.Hour
	s.cfg.MaxTrashBytes = 30 // bytes: small enough that 4x10 does not fit

	// Four 10-byte objects, trashed oldest-first.
	bodies := []string{"aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc", "dddddddddd"}
	for i, body := range bodies {
		id := fmt.Sprintf("o%d", i)
		mustPut(t, s, "b1", id, body)
		if err := s.Delete(t.Context(), "b1", id, etag.Precondition{}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		mustSweep(t, s)
		stamp := time.Now().Add(time.Duration(i-len(bodies)) * time.Minute)
		if err := os.Chtimes(s.trashPath("b1", hashOf(body)), stamp, stamp); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	res := mustSweep(t, s)

	if res.TrashBytes > s.cfg.MaxTrashBytes {
		t.Errorf("TrashBytes = %d, want <= %d", res.TrashBytes, s.cfg.MaxTrashBytes)
	}
	// The oldest must go first, the newest must survive.
	if _, err := os.Stat(s.trashPath("b1", hashOf(bodies[0]))); !os.IsNotExist(err) {
		t.Errorf("the oldest trash entry survived the size cap: %v", err)
	}
	if _, err := os.Stat(s.trashPath("b1", hashOf(bodies[3]))); err != nil {
		t.Errorf("the newest trash entry was evicted: %v", err)
	}
}

func TestTrashSizeCapDisabledAtZero(t *testing.T) {
	s := newTestStore(t)
	s.cfg.TrashRetention = time.Hour
	s.cfg.MaxTrashBytes = 0

	mustPut(t, s, "b1", "a", "some bytes here")
	if err := s.Delete(t.Context(), "b1", "a", etag.Precondition{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustSweep(t, s)

	res := mustSweep(t, s)
	if res.TrashFiles != 1 {
		t.Errorf("TrashFiles = %d, want 1 — a zero cap must disable the limit", res.TrashFiles)
	}
}
