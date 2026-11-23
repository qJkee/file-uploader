package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func trashRoot(s *Store) string { return filepath.Join(s.dataDir, "trash") }

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
	if err := s.Delete(t.Context(), "b1", "a"); err != nil {
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
	if err := s.DB().QueryRow(`SELECT count(*) FROM blobs`).Scan(&n); err != nil {
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
		if err := s.Delete(t.Context(), "b1", id); err != nil {
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
		if err := s.Delete(t.Context(), "b1", id); err != nil {
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
		if err := s.Delete(t.Context(), "b1", id); err != nil {
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
	if err := s.Delete(t.Context(), "b1", "a"); err != nil {
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

// TestTrashRecoveryPath exercises the manual recovery the trash window exists
// for: rename the file back and reinsert its rows.
func TestTrashRecoveryPath(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	if err := s.Delete(t.Context(), "b1", "a"); err != nil {
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
	if _, err := s.DB().Exec(`INSERT INTO blobs (bucket, hash) VALUES ('b1', ?)`, hash); err != nil {
		t.Fatalf("reinsert blob row: %v", err)
	}
	if _, err := s.DB().Exec(
		`INSERT INTO objects (bucket, object_id, hash) VALUES ('b1', 'a', ?)`, hash); err != nil {
		t.Fatalf("reinsert object row: %v", err)
	}

	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("recovered object = %q, want %q", got, "hello")
	}
}
