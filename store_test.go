package main

import (
	"errors"
	"strings"
	"testing"
)

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
