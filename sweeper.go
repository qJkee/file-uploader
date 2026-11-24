package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// sqlstateForeignKeyViolation is Postgres' foreign_key_violation class.
const sqlstateForeignKeyViolation = "23503"

// sweepLimit is how many reclaim candidates one query returns. A var so tests
// can exercise the drain loop without creating a thousand blobs.
var sweepLimit = 1000

// SweepResult is one sweep's work, accumulated as it goes rather than
// computed on scrape, so a /metrics request stays O(1) regardless of store
// size.
type SweepResult struct {
	Candidates   int
	Reclaimed    int
	Orphans      int
	Failed       int
	TrashDeleted int
	TrashEvicted int
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

	// Every pass runs even if an earlier one failed. One blob the sweeper
	// cannot move — wrong ownership after an operator chown, an EIO — must
	// not stop trash expiry and temp cleanup too, or a single bad file leaks
	// disk without bound.
	err = errors.Join(
		s.reclaim(ctx, &res),
		s.sweepOrphansIfDue(ctx, &res),
		s.expireTrash(ctx, &res),
		s.cleanTmp(ctx, &res),
	)
	return res, err
}

// orphanSweepInterval bounds how often the blob tree is walked. The reclaim
// anti-join only sees blobs that have a row, so a PUT that rolled back after
// renaming its blob into place leaks a file nothing will ever select — but
// that is rare, and a full tree walk is the one genuinely expensive thing
// the sweeper can do. It does not belong on the reclaim cadence.
var orphanSweepInterval = time.Hour

// sweepOrphansIfDue runs the orphan walk at most once per
// orphanSweepInterval. Blobs younger than TmpMaxAge are spared, which is the
// same bound a live upload gets: a PUT holding an uncommitted rename for
// longer than that has already lost its temp file too.
func (s *Store) sweepOrphansIfDue(ctx context.Context, res *SweepResult) error {
	now := time.Now()
	last := s.lastOrphanSweep.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < orphanSweepInterval {
		return nil
	}
	if !s.lastOrphanSweep.CompareAndSwap(last, now.UnixNano()) {
		return nil // another sweep got there first
	}
	n, err := s.SweepOrphans(ctx, now.Add(-s.cfg.TmpMaxAge))
	res.Orphans += n
	return err
}

type candidate struct{ bucket, hash string }

// reclaim loops until a batch comes back short or the wall-clock budget
// expires. Without the loop a burst of a million deletes would reclaim one
// batch per interval and take days.
func (s *Store) reclaim(ctx context.Context, res *SweepResult) error {
	deadline := time.Now().Add(s.cfg.SweepBudget)
	var firstErr error
	for {
		if time.Now().After(deadline) {
			break
		}
		batch, err := s.reclaimCandidates(ctx)
		if err != nil {
			return errors.Join(firstErr, err)
		}
		res.Candidates += len(batch)
		progress := 0
		for _, c := range batch {
			if err := ctx.Err(); err != nil {
				return errors.Join(firstErr, err)
			}
			ok, err := s.reclaimOne(ctx, c)
			switch {
			case err != nil:
				// One unmovable candidate is counted and skipped, not
				// escalated: the query has no ORDER BY, so returning here
				// would hand back the same row forever and stall reclamation
				// of everything behind it.
				res.Failed++
				if firstErr == nil {
					firstErr = err
				}
			case ok:
				res.Reclaimed++
				progress++
			}
		}
		if len(batch) < sweepLimit || progress == 0 {
			break // short batch, or nothing movable left to make progress on
		}
	}
	if firstErr != nil {
		return fmt.Errorf("%d candidate(s) could not be reclaimed, first: %w", res.Failed, firstErr)
	}
	return nil
}

// reclaimCandidates is an indexed anti-join over a small table, not a
// filesystem walk. A blob is live iff some objects row references it.
func (s *Store) reclaimCandidates(ctx context.Context) ([]candidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT bucket, hash FROM blobs b
		WHERE NOT EXISTS (
		  SELECT 1 FROM objects o WHERE o.bucket = b.bucket AND o.hash = b.hash
		)
		LIMIT $1`, sweepLimit)
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `
		DELETE FROM blobs WHERE bucket = $1 AND hash = $2
		  AND NOT EXISTS (SELECT 1 FROM objects WHERE bucket = $1 AND hash = $2)`,
		c.bucket, c.hash)
	if err != nil {
		// Under READ COMMITTED the NOT EXISTS subquery reads a snapshot taken
		// when the statement began, so a PUT that commits an objects row after
		// that snapshot still lets the DELETE through — and the foreign key
		// trigger, which runs against current data, rejects it. That is the
		// same outcome as zero rows affected: the blob was adopted by a
		// concurrent PUT, so leave it alone. The violation has already aborted
		// the transaction, so return here and let the deferred Rollback run
		// rather than falling through to Commit.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateForeignKeyViolation {
			return false, nil
		}
		return false, err
	}
	if result.RowsAffected() == 0 {
		return false, nil // adopted by a concurrent PUT; leave it alone
	}

	if err := s.stageOrRemove(c); err != nil {
		return false, err // rollback keeps the row, so the next sweep retries
	}
	if err := tx.Commit(ctx); err != nil {
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
	_, err := s.moveToTrash(c.bucket, c.hash, src)
	return err
}

// moveToTrash stages a reclaimed blob and starts its retention clock.
//
// The mtime reset is load-bearing. rename(2) preserves mtime, so a trashed
// file otherwise keeps the time it was *written*, not the time it became
// garbage — and any blob older than -trash-retention would be expired by the
// very sweep that trashed it. The recovery net would then be absent for
// exactly the long-lived objects that need it most. This is the same mistake
// the design rejected in "mtime-based grace"; moving the file does not fix
// it on its own.
func (s *Store) moveToTrash(bucket, hash, src string) (bool, error) {
	if err := os.MkdirAll(s.trashDir(bucket), 0o755); err != nil {
		return false, fmt.Errorf("create trash dir: %w", err)
	}
	dst := s.trashPath(bucket, hash)
	if err := os.Rename(src, dst); err != nil {
		if os.IsNotExist(err) {
			return false, nil // already gone; the row removal still stands
		}
		return false, fmt.Errorf("move reclaimed blob to trash: %w", err)
	}
	now := time.Now()
	if err := os.Chtimes(dst, now, now); err != nil {
		return false, fmt.Errorf("start retention clock on %s: %w", dst, err)
	}
	return true, nil
}

// expireTrash unlinks trash entries past the retention window and accumulates
// what survives, so trash_files and trash_bytes cost nothing at scrape time.
func (s *Store) expireTrash(ctx context.Context, res *SweepResult) error {
	cutoff := time.Now().Add(-s.cfg.TrashRetention)
	var kept []trashEntry

	err := walkFiles(ctx, filepath.Join(s.dataDir, "trash"), func(path string, info fs.FileInfo) error {
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			res.TrashDeleted++
			return nil
		}
		kept = append(kept, trashEntry{path: path, mtime: info.ModTime(), size: info.Size()})
		res.TrashBytes += info.Size()
		return nil
	})
	if err != nil {
		return err
	}

	// Retention alone cannot bound disk: uploading and deleting the same
	// object in a loop leaves a copy per cycle and none expire for a day.
	// Past the cap, evict oldest first — the entries closest to expiring
	// anyway, and the least likely to be the one an operator wants back.
	if s.cfg.MaxTrashBytes > 0 && res.TrashBytes > s.cfg.MaxTrashBytes {
		slices.SortFunc(kept, func(a, b trashEntry) int { return a.mtime.Compare(b.mtime) })
		evicted := 0
		for _, e := range kept {
			if res.TrashBytes <= s.cfg.MaxTrashBytes {
				break
			}
			if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
				return err
			}
			res.TrashBytes -= e.size
			res.TrashDeleted++
			res.TrashEvicted++
			evicted++
		}
		kept = kept[evicted:]
	}
	res.TrashFiles = len(kept)
	return nil
}

type trashEntry struct {
	path  string
	mtime time.Time
	size  int64
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
	// Recorded for both callers, so the startup walk and the first periodic
	// sweep do not walk the tree twice at boot.
	s.lastOrphanSweep.Store(time.Now().UnixNano())

	blobsRoot := filepath.Join(s.dataDir, "blobs")

	// The guard. Without it a lost or replaced metadata database — wrong
	// server, bad restore, someone clearing the schema — means the service
	// starts on an empty schema and deletes every object on disk, silently
	// and permanently.
	var hasRows bool
	if err := s.pool.QueryRow(ctx,
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
				"hint", "check that -database-url points at the expected database before restarting")
		}
		return 0, nil
	}

	reclaimed := 0
	batch := make([]blobFile, 0, orphanBatchSize)
	flush := func() error {
		n, err := s.reclaimOrphanBatch(ctx, batch)
		reclaimed += n
		batch = batch[:0]
		return err
	}

	err := walkFiles(ctx, blobsRoot, func(path string, info fs.FileInfo) error {
		if !info.ModTime().Before(startedAt) {
			return nil
		}
		bucket, hash, ok := parseBlobPath(blobsRoot, path)
		if !ok {
			return nil // not something this service wrote; leave it
		}
		batch = append(batch, blobFile{bucket: bucket, hash: hash, path: path})
		if len(batch) < orphanBatchSize {
			return nil
		}
		return flush()
	})
	if err != nil {
		return reclaimed, err
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return reclaimed, err
		}
	}
	return reclaimed, nil
}

// orphanBatchSize is how many files are checked per query. Almost every file
// in a healthy store is known, so this is what decides the cost of the walk:
// one query per file meant a million queries an hour at a million blobs.
var orphanBatchSize = 500

type blobFile struct{ bucket, hash, path string }

// reclaimOrphanBatch resolves a batch with one query per bucket, then
// reclaims only the files that query did not account for. The bulk lookup is
// a cheap pre-filter, not the safety check — reclaimOrphan still re-reads
// each row inside its own transaction, which is what stops a concurrent PUT
// from having its blob pulled out from under it.
func (s *Store) reclaimOrphanBatch(ctx context.Context, batch []blobFile) (int, error) {
	byBucket := map[string][]string{}
	for _, f := range batch {
		byBucket[f.bucket] = append(byBucket[f.bucket], f.hash)
	}
	known := map[string]map[string]bool{}
	for bucket, hashes := range byBucket {
		found, err := s.knownHashes(ctx, bucket, hashes)
		if err != nil {
			return 0, err
		}
		known[bucket] = found
	}

	reclaimed := 0
	for _, f := range batch {
		if known[f.bucket][f.hash] {
			continue
		}
		if s.hookOrphanWindow != nil {
			s.hookOrphanWindow()
		}
		moved, err := s.reclaimOrphan(ctx, f.bucket, f.hash, f.path)
		if err != nil {
			return reclaimed, err
		}
		if moved {
			reclaimed++
			orphansReclaimed.Inc()
		}
	}
	return reclaimed, nil
}

// knownHashes returns which of hashes have a blobs row in bucket.
func (s *Store) knownHashes(ctx context.Context, bucket string, hashes []string) (map[string]bool, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT hash FROM blobs WHERE bucket = $1 AND hash = ANY($2)`,
		bucket, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		found[h] = true
	}
	return found, rows.Err()
}

// reclaimOrphan moves one orphan under the write lock. The re-check inside
// the transaction is what makes it safe: the walk's own read happened
// outside any lock, and a PUT of exactly these bytes — the idempotent retry
// a client makes after the crash that created the orphan — can commit a row
// in between. Without the re-check that PUT ends up with a committed row
// pointing at a file now sitting in trash: a permanent 500 on every read,
// and the one alert the whole locking design exists to keep at zero.
func (s *Store) reclaimOrphan(ctx context.Context, bucket, hash, path string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var known bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM blobs WHERE bucket = $1 AND hash = $2)`,
		bucket, hash).Scan(&known); err != nil {
		return false, err
	}
	if known {
		return false, nil // adopted while we walked; leave it alone
	}

	moved, err := s.moveToTrash(bucket, hash, path)
	if err != nil || !moved {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
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
