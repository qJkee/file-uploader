package main

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// sweepLimit is how many reclaim candidates one query returns. A var so tests
// can exercise the drain loop without creating a thousand blobs.
var sweepLimit = 1000

// SweepResult is one sweep's work, accumulated as it goes rather than
// computed on scrape, so a /metrics request stays O(1) regardless of store
// size.
type SweepResult struct {
	Candidates   int
	Reclaimed    int
	TrashDeleted int
	TmpDeleted   int
	TrashFiles   int
	TrashBytes   int64
}

// Sweep reclaims unreferenced blobs, expires trash and removes abandoned
// uploads. It is safe to run concurrently with live traffic: each reclaim
// re-checks the anti-join inside its own transaction, and the foreign key
// from objects to blobs blocks a PUT of the same content for exactly that
// hash while the reclaim holds the row.
func (s *Store) Sweep(ctx context.Context) (res SweepResult, err error) {
	start := time.Now()
	defer func() { observeSweep(res, err, time.Since(start).Seconds()) }()

	if err = s.reclaim(ctx, &res); err != nil {
		return res, err
	}
	if err = s.expireTrash(ctx, &res); err != nil {
		return res, err
	}
	if err = s.cleanTmp(ctx, &res); err != nil {
		return res, err
	}
	return res, nil
}

type candidate struct{ bucket, hash string }

// reclaim loops until a batch comes back short or the wall-clock budget
// expires. Without the loop a burst of a million deletes would reclaim one
// batch per interval and take days.
func (s *Store) reclaim(ctx context.Context, res *SweepResult) error {
	deadline := time.Now().Add(s.cfg.SweepBudget)
	for {
		if time.Now().After(deadline) {
			return nil
		}
		batch, err := s.reclaimCandidates(ctx)
		if err != nil {
			return err
		}
		res.Candidates += len(batch)
		for _, c := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			ok, err := s.reclaimOne(ctx, c)
			if err != nil {
				return err
			}
			if ok {
				res.Reclaimed++
			}
		}
		if len(batch) < sweepLimit {
			return nil
		}
	}
}

// reclaimCandidates is an indexed anti-join over a small table, not a
// filesystem walk. A blob is live iff some objects row references it.
func (s *Store) reclaimCandidates(ctx context.Context) ([]candidate, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT bucket, hash FROM blobs b
		WHERE NOT EXISTS (
		  SELECT 1 FROM objects o WHERE o.bucket = b.bucket AND o.hash = b.hash
		)
		LIMIT ?`, sweepLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.bucket, &c.hash); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// reclaimOne removes one blob row and stages its file, in a transaction whose
// re-check is what makes the move safe: a PUT that adopted this hash between
// the candidate query and now leaves the DELETE matching no rows.
func (s *Store) reclaimOne(ctx context.Context, c candidate) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE, see openDB
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		DELETE FROM blobs WHERE bucket = ? AND hash = ?
		  AND NOT EXISTS (SELECT 1 FROM objects WHERE bucket = ? AND hash = ?)`,
		c.bucket, c.hash, c.bucket, c.hash)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil // adopted by a concurrent PUT; leave it alone
	}

	if err := s.stageOrRemove(c); err != nil {
		return false, err // rollback keeps the row, so the next sweep retries
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// stageOrRemove moves a reclaimed blob to trash, or unlinks it outright when
// retention is disabled. Trash rather than unlink is a safety net against
// metadata bugs and operator error, not against in-flight readers — Get's
// ENOENT retry already resolves that race.
func (s *Store) stageOrRemove(c candidate) error {
	src := s.blobPath(c.bucket, c.hash)
	if s.cfg.TrashRetention <= 0 {
		if err := os.Remove(src); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("unlink reclaimed blob: %w", err)
		}
		return nil
	}
	if err := os.MkdirAll(s.trashDir(c.bucket), 0o755); err != nil {
		return fmt.Errorf("create trash dir: %w", err)
	}
	// mtime in trash means "when the blob became garbage", which is exactly
	// what the retention clock needs and what a blob's own mtime cannot say.
	if err := os.Rename(src, s.trashPath(c.bucket, c.hash)); err != nil {
		if os.IsNotExist(err) {
			return nil // already gone; the row removal still stands
		}
		return fmt.Errorf("move reclaimed blob to trash: %w", err)
	}
	return nil
}

// expireTrash unlinks trash entries past the retention window and accumulates
// what survives, so trash_files and trash_bytes cost nothing at scrape time.
func (s *Store) expireTrash(ctx context.Context, res *SweepResult) error {
	cutoff := time.Now().Add(-s.cfg.TrashRetention)
	return walkFiles(ctx, filepath.Join(s.dataDir, "trash"), func(path string, info fs.FileInfo) error {
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			res.TrashDeleted++
			return nil
		}
		res.TrashFiles++
		res.TrashBytes += info.Size()
		return nil
	})
}

// cleanTmp removes aborted uploads whose process died before the deferred
// remove could run. TmpMaxAge must exceed the longest plausible upload:
// unlinking a live temp makes that PUT's rename fail with ENOENT.
func (s *Store) cleanTmp(ctx context.Context, res *SweepResult) error {
	cutoff := time.Now().Add(-s.cfg.TmpMaxAge)
	return walkFiles(ctx, s.tmpDir(), func(path string, info fs.FileInfo) error {
		if !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		res.TmpDeleted++
		return nil
	})
}

// SweepOrphans moves blob files with no blobs row to trash — crash orphans,
// bytes renamed into place by a PUT that died before committing. It runs once
// per process. Trash rather than unlink because this is the most dangerous
// operation in the system, which is exactly why it should be the one most
// clearly recoverable.
//
// startedAt is captured before the server accepts traffic: files newer than
// it belong to in-flight PUTs and are left alone, which is what lets the walk
// run in the background instead of blocking startup.
func (s *Store) SweepOrphans(ctx context.Context, startedAt time.Time) (int, error) {
	blobsRoot := filepath.Join(s.dataDir, "blobs")

	// The guard. Without it a lost or replaced meta.db — wrong volume
	// mounted, bad restore, someone clearing the directory — means the
	// service starts on an empty schema and deletes every object on disk,
	// silently and permanently.
	var hasRows bool
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM blobs)`).Scan(&hasRows); err != nil {
		return 0, err
	}
	if !hasRows {
		empty, err := isEmptyTree(ctx, blobsRoot)
		if err != nil {
			return 0, err
		}
		if !empty {
			slog.Error("blobs table is empty but the blob tree is not; skipping the orphan walk",
				"blobs_dir", blobsRoot,
				"hint", "check that the expected meta.db is mounted before restarting")
		}
		return 0, nil
	}

	reclaimed := 0
	err := walkFiles(ctx, blobsRoot, func(path string, info fs.FileInfo) error {
		if !info.ModTime().Before(startedAt) {
			return nil
		}
		bucket, hash, ok := parseBlobPath(blobsRoot, path)
		if !ok {
			return nil // not something this service wrote; leave it
		}
		var known bool
		if err := s.db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM blobs WHERE bucket = ? AND hash = ?)`,
			bucket, hash).Scan(&known); err != nil {
			return err
		}
		if known {
			return nil
		}
		if err := os.MkdirAll(s.trashDir(bucket), 0o755); err != nil {
			return err
		}
		if err := os.Rename(path, s.trashPath(bucket, hash)); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		reclaimed++
		startupOrphansDeleted.Inc()
		return nil
	})
	return reclaimed, err
}

// parseBlobPath reverses blobPath: blobs/{bucket}/{hh}/{hash}. A file that
// does not fit that shape is not ours to move.
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

func isEmptyTree(ctx context.Context, root string) (bool, error) {
	empty := true
	err := walkFiles(ctx, root, func(string, fs.FileInfo) error {
		empty = false
		return filepath.SkipAll
	})
	return empty, err
}

// walkFiles visits every regular file under root. A missing root is not an
// error: nothing has been written there yet.
func walkFiles(ctx context.Context, root string, fn func(path string, info fs.FileInfo) error) error {
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // raced with our own removal, or never existed
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
