package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustPut(t *testing.T, s *Store, bucket, objectID, body string) PutResult {
	t.Helper()
	res, err := s.Put(t.Context(), bucket, objectID, strings.NewReader(body))
	if err != nil {
		t.Fatalf("Put(%s/%s): %v", bucket, objectID, err)
	}
	return res
}

func mustGet(t *testing.T, s *Store, bucket, objectID string) string {
	t.Helper()
	f, _, err := s.Get(t.Context(), bucket, objectID)
	if err != nil {
		t.Fatalf("Get(%s/%s): %v", bucket, objectID, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %s/%s: %v", bucket, objectID, err)
	}
	return string(b)
}

func countBlobFiles(t *testing.T, s *Store) int {
	t.Helper()
	return countFiles(t, filepath.Join(s.dataDir, "blobs"))
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

func TestPutGetRoundTrip(t *testing.T) {
	s := newTestStore(t)
	res := mustPut(t, s, "b1", "a", "hello")

	if !res.Created {
		t.Error("first Put: Created = false, want true")
	}
	if res.Hash != hashOf("hello") {
		t.Errorf("Hash = %q, want sha256 of the body", res.Hash)
	}
	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("Get = %q, want %q", got, "hello")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	res := mustPut(t, s, "b1", "a", "world")

	if res.Created {
		t.Error("second Put to the same path: Created = true, want false")
	}
	if got := mustGet(t, s, "b1", "a"); got != "world" {
		t.Errorf("Get = %q, want %q", got, "world")
	}
}

func TestDedupOneFileTwoIDs(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	res := mustPut(t, s, "b1", "b", "hello")

	if !res.Dedup {
		t.Error("second Put of identical bytes: Dedup = false, want true")
	}
	if got := countBlobFiles(t, s); got != 1 {
		t.Errorf("blob files = %d, want 1", got)
	}
	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("a = %q, want %q", got, "hello")
	}
	if got := mustGet(t, s, "b1", "b"); got != "hello" {
		t.Errorf("b = %q, want %q", got, "hello")
	}
}

// TestSplitOnOverwrite is the case where naive dedup corrupts data: two IDs
// share one file, and one of them is overwritten. The separation must be a
// pointer update with no file unlinked.
func TestSplitOnOverwrite(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	mustPut(t, s, "b1", "b", "hello")

	mustPut(t, s, "b1", "b", "world")

	if got := mustGet(t, s, "b1", "a"); got != "hello" {
		t.Errorf("a = %q, want %q — the shared blob was corrupted", got, "hello")
	}
	if got := mustGet(t, s, "b1", "b"); got != "world" {
		t.Errorf("b = %q, want %q", got, "world")
	}
	if got := countBlobFiles(t, s); got != 2 {
		t.Errorf("blob files = %d, want 2", got)
	}
}

func TestDedupIsPerBucket(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	res := mustPut(t, s, "b2", "a", "hello")

	if res.Dedup {
		t.Error("identical bytes in a second bucket: Dedup = true, want false — buckets stay independent")
	}
	if got := countBlobFiles(t, s); got != 2 {
		t.Errorf("blob files = %d, want 2", got)
	}
}

func TestPutEmptyBody(t *testing.T) {
	s := newTestStore(t)
	res := mustPut(t, s, "b1", "a", "")

	if res.Hash != hashOf("") {
		t.Errorf("Hash = %q, want sha256 of the empty string", res.Hash)
	}
	if got := mustGet(t, s, "b1", "a"); got != "" {
		t.Errorf("Get = %q, want empty", got)
	}
}

func TestPutRejectsInvalidNames(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Put(t.Context(), "B1", "a", strings.NewReader("x")); !errors.Is(err, ErrInvalidName) {
		t.Errorf("Put with an uppercase bucket = %v, want ErrInvalidName", err)
	}
	if _, err := s.Put(t.Context(), "b1", "..", strings.NewReader("x")); !errors.Is(err, ErrInvalidName) {
		t.Errorf("Put with objectID %q = %v, want ErrInvalidName", "..", err)
	}
}

func TestGetMissing(t *testing.T) {
	s := newTestStore(t)
	if _, _, err := s.Get(t.Context(), "b1", "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of a missing object = %v, want ErrNotFound", err)
	}
}

func TestHeadMissing(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Head(t.Context(), "b1", "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Head of a missing object = %v, want ErrNotFound", err)
	}
}

func TestHeadReturnsHash(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	got, err := s.Head(t.Context(), "b1", "a")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if got != hashOf("hello") {
		t.Errorf("Head = %q, want sha256 of the body", got)
	}
}

// unlinkBlob removes the file a resolved row points at, simulating a sweeper
// unlink that landed between the row read and the open.
func unlinkBlob(t *testing.T, s *Store, bucket, body string) {
	t.Helper()
	if err := os.Remove(s.blobPath(bucket, hashOf(body))); err != nil {
		t.Fatalf("unlink blob: %v", err)
	}
}

func TestGetRetryDeleted(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	unlinkBlob(t, s, "b1", "hello")
	s.hookGetMiss = func() {
		if _, err := s.db.Exec(`DELETE FROM objects WHERE bucket='b1' AND object_id='a'`); err != nil {
			t.Errorf("hook delete: %v", err)
		}
	}

	if _, _, err := s.Get(t.Context(), "b1", "a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after a concurrent delete = %v, want ErrNotFound", err)
	}
}

func TestGetRetryOverwritten(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	mustPut(t, s, "b1", "other", "world") // puts hash(world) on disk
	unlinkBlob(t, s, "b1", "hello")
	s.hookGetMiss = func() {
		if _, err := s.db.Exec(
			`UPDATE objects SET hash = ? WHERE bucket='b1' AND object_id='a'`,
			hashOf("world")); err != nil {
			t.Errorf("hook update: %v", err)
		}
	}

	if got := mustGet(t, s, "b1", "a"); got != "world" {
		t.Errorf("Get after a concurrent overwrite = %q, want %q", got, "world")
	}
}

func TestDeleteRemovesRowNotFile(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")

	if err := s.Delete(t.Context(), "b1", "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := s.Get(t.Context(), "b1", "a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
	if got := countBlobFiles(t, s); got != 1 {
		t.Errorf("blob files = %d, want 1 — DELETE must do no filesystem work", got)
	}
}

func TestDeleteMissing(t *testing.T) {
	s := newTestStore(t)
	if err := s.Delete(t.Context(), "b1", "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete of a missing object = %v, want ErrNotFound", err)
	}
}

func TestDeleteOneOfTwoSharedIDs(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	mustPut(t, s, "b1", "b", "hello")

	if err := s.Delete(t.Context(), "b1", "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := mustGet(t, s, "b1", "b"); got != "hello" {
		t.Errorf("b = %q, want %q — deleting a sibling destroyed the shared blob", got, "hello")
	}
}

func TestGetRetryBlobMissing(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "b1", "a", "hello")
	unlinkBlob(t, s, "b1", "hello")

	if _, _, err := s.Get(t.Context(), "b1", "a"); !errors.Is(err, ErrBlobMissing) {
		t.Errorf("Get with an unchanged row and no file = %v, want ErrBlobMissing", err)
	}
}
