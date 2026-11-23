package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
)

// ErrInvalidName is returned for a bucket or object ID that fails validation.
// The HTTP layer maps it to 400.
var ErrInvalidName = errors.New("invalid name")

// Buckets are lowercase-only because a bucket is a directory name: APFS is
// case-insensitive by default while SQLite TEXT keys are not, so "Photos" and
// "photos" would be two rows sharing one directory and dedup would merge
// across them on macOS but not on Linux.
var bucketRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// Object IDs never touch the filesystem, so they stay case-sensitive.
// Non-ASCII is excluded deliberately: NFC/NFD would give two rows for one
// visible name.
var objectIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

func validateBucket(b string) error {
	if !bucketRe.MatchString(b) {
		return fmt.Errorf("%w: bucket %q", ErrInvalidName, b)
	}
	return nil
}

func validateObjectID(id string) error {
	if !objectIDRe.MatchString(id) || id == "." || id == ".." {
		return fmt.Errorf("%w: object id %q", ErrInvalidName, id)
	}
	return nil
}

// Store maps (bucket, objectID) names onto content-addressed blobs on disk.
type Store struct {
	dataDir string
}

// blobDir is {data}/blobs/{bucket}/{hh}. The two-hex-character fanout keeps a
// single directory from reaching millions of entries.
func (s *Store) blobDir(bucket, hash string) string {
	return filepath.Join(s.dataDir, "blobs", bucket, hash[:2])
}

func (s *Store) blobPath(bucket, hash string) string {
	return filepath.Join(s.blobDir(bucket, hash), hash)
}

// trashDir keeps the bucket in its path for the same reason blobs does: dedup
// is per-bucket, so two buckets can legitimately hold the same hash, and a
// flat trash/{hash} would collide them and leave the documented recovery
// unable to tell which bucket a file belonged to.
func (s *Store) trashDir(bucket string) string {
	return filepath.Join(s.dataDir, "trash", bucket)
}

func (s *Store) trashPath(bucket, hash string) string {
	return filepath.Join(s.trashDir(bucket), hash)
}

func (s *Store) tmpDir() string {
	return filepath.Join(s.dataDir, "tmp")
}
