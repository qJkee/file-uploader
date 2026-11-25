package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The audit harness this file recreates: 8 workers PUTting to distinct keys
// for 6 seconds against one httptest.Server, measured with and without a
// concurrent drain of a large backlog of unreferenced blobs. The readers are
// new — a writer blocked in the database holds one of the pool's 10
// connections, so a PUT-only measurement cannot see reads being starved.
const (
	contentionWriters = 8
	contentionReaders = 4
	contentionWindow  = 6 * time.Second
	contentionBacklog = 60000
	liveBucket        = "live"
	drainBucket       = "drain"
)

// contentionPayload keeps a PUT body in the same order of magnitude as the
// audit's without making the measurement about bandwidth.
var contentionPayload = strings.Repeat("x", 256)

// seedBacklog inserts n unreferenced blobs rows, bypassing Put entirely:
// 60k real PUTs would take far longer than the whole test, and reclaim only
// needs a row to delete and a file to move.
//
// withFiles writes the files those rows name. Without them reclaim still
// takes its full database path — moveToTrash just finds nothing to rename
// and reclaimOne commits anyway — which is what the database-only sibling
// test wants.
func seedBacklog(t *testing.T, s *Store, n int, withFiles bool) {
	t.Helper()

	hashes := make([]string, n)
	for i := range hashes {
		sum := sha256.Sum256(fmt.Appendf(nil, "backlog-%d", i))
		hashes[i] = hex.EncodeToString(sum[:])
	}

	rows := pgx.CopyFromSlice(n, func(i int) ([]any, error) {
		return []any{drainBucket, hashes[i]}, nil
	})
	if _, err := s.Pool().CopyFrom(t.Context(),
		pgx.Identifier{"blobs"}, []string{"bucket", "hash"}, rows); err != nil {
		t.Fatalf("seed blobs rows: %v", err)
	}

	if !withFiles {
		return
	}
	for i := range 256 {
		dir := s.blobDir(drainBucket, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create %s: %v", dir, err)
		}
	}

	// Empty files: the drain's cost is the rename and the transaction, not
	// the bytes. Parallel because 60k serial creates are a visible share of
	// the test's runtime.
	const shards = 8
	errs := make([]error, shards)
	var wg sync.WaitGroup
	for w := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < n; i += shards {
				if err := os.WriteFile(s.blobPath(drainBucket, hashes[i]), nil, 0o644); err != nil {
					errs[w] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("seed blob files: %v", err)
		}
	}
}

// backlogRows counts what is left of the seeded backlog. Used to prove the
// drain really was running during the measured window — a sweeper that
// silently did nothing would make the whole test pass for the wrong reason.
func backlogRows(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(context.WithoutCancel(t.Context()),
		`SELECT count(*) FROM blobs WHERE bucket = $1`, drainBucket).Scan(&n); err != nil {
		t.Fatalf("count backlog: %v", err)
	}
	return n
}

// phase is one measured window.
type phase struct {
	name             string
	elapsed          time.Duration
	puts, gets       []time.Duration // sorted
	putFail, getFail int
	drained          int
	err              error // first failure of each worker, if any
}

func (p phase) putRate() float64 { return float64(len(p.puts)) / p.elapsed.Seconds() }

// pct is the nearest-rank percentile of a sorted sample set.
func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(max(int(math.Ceil(q*float64(len(sorted))))-1, 0), len(sorted)-1)]
}

// dur keeps sub-millisecond GET latencies from all printing as "0s".
func dur(d time.Duration) time.Duration {
	if d < 10*time.Millisecond {
		return d.Round(100 * time.Microsecond)
	}
	return d.Round(time.Millisecond)
}

func (p phase) row(kind string, d []time.Duration, fail int) string {
	return fmt.Sprintf("%-10s %-4s %7d %8d %9v %9v %9v %9v",
		p.name, kind, len(d), fail,
		dur(pct(d, 0.50)), dur(pct(d, 0.95)), dur(pct(d, 0.99)), dur(pct(d, 1.0)))
}

// poolWait reports how much of a phase was spent blocked because every one
// of the pool's 10 connections was busy. Row locks never collide on
// Postgres, so the shared pool is the one route by which a sweeper can slow
// the request path.
//
// EmptyAcquireWaitTime, not AcquireDuration: the latter totals every
// successful acquire whether it waited or not, so it is dominated by ~10 µs
// of per-acquire bookkeeping and its floor scales with request count rather
// than with contention.
func poolWait(t *testing.T, name string, before, after *pgxpool.Stat) time.Duration {
	t.Helper()
	waited := after.EmptyAcquireWaitTime() - before.EmptyAcquireWaitTime()
	t.Logf("%-10s pool: %d acquires, %d found the pool empty, %v spent waiting", name,
		after.AcquireCount()-before.AcquireCount(),
		after.EmptyAcquireCount()-before.EmptyAcquireCount(), waited.Round(time.Millisecond))
	return waited
}

// watchBlobsLockQueue samples pg_locks for backends queued on a *relation*
// lock on blobs, and returns the peak depth seen.
//
// This is the test's one calibration-free instrument. Every other assertion
// here is a wall-clock number that holds on one laptop; a client backend
// stuck behind `LOCK TABLE blobs` appears in this queue on any hardware,
// however fast or slow, because it is the mechanism rather than a symptom of
// it. Row locks — which are all this design should ever take on blobs — are
// tuples, not relations, and never appear here.
//
// It samples rather than integrates: a lock held for microseconds at a time
// can be missed, so a non-zero reading is strong evidence and a zero reading
// is weak. That asymmetry is the right way round for an assertion that must
// not flake.
func watchBlobsLockQueue(t *testing.T, s *Store, done <-chan struct{}) <-chan int {
	t.Helper()
	peak := make(chan int, 1)
	go func() {
		ctx := context.WithoutCancel(t.Context())
		worst := 0
		for {
			select {
			case <-done:
				peak <- worst
				return
			case <-time.After(20 * time.Millisecond):
			}
			var queued int
			// to_regclass, not a cast: the per-test schema is dropped during
			// cleanup and a cast to a missing relation would error.
			if err := s.Pool().QueryRow(ctx, `
				SELECT count(*) FROM pg_locks
				WHERE NOT granted AND locktype = 'relation'
				  AND relation = to_regclass('blobs')`).Scan(&queued); err != nil {
				continue // the pool is busy or shutting down; not our business
			}
			worst = max(worst, queued)
		}
	}()
	return peak
}

// timeRequest issues one request and reports how long it took end to end.
// ts.do is unusable here: it calls t.Fatalf from a worker goroutine and
// registers a t.Cleanup per request, and this harness makes thousands.
func timeRequest(ctx context.Context, c *http.Client, method, url, body string) (time.Duration, error) {
	start := time.Now()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return time.Since(start), err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return time.Since(start), err
	}
	if resp.StatusCode/100 != 2 {
		return time.Since(start), fmt.Errorf("%s %s: %s", method, url, resp.Status)
	}
	return time.Since(start), nil
}

// dbWriteWindow drives Put's *database* shape — advisory lock, precondition
// read, both upserts, commit — with no file I/O at all, for window, and
// returns the sorted latencies.
func dbWriteWindow(t *testing.T, s *Store, window time.Duration) []time.Duration {
	t.Helper()
	deadline := time.Now().Add(window)
	per := make([][]time.Duration, contentionWriters)
	errs := make([]error, contentionWriters)

	var wg sync.WaitGroup
	for w := range contentionWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("writer-%d", w)
			for i := 0; time.Now().Before(deadline); i++ {
				start := time.Now()
				errs[w] = cmp.Or(errs[w], dbWriteOnce(t.Context(), s, key, hashOf(fmt.Sprintf("%s-%d", key, i))))
				per[w] = append(per[w], time.Since(start))
			}
		}()
	}
	wg.Wait()

	var all []time.Duration
	for w := range per {
		if errs[w] != nil {
			t.Fatalf("writer %d: %v", w, errs[w])
		}
		all = append(all, per[w]...)
	}
	slices.Sort(all)
	return all
}

func dbWriteOnce(ctx context.Context, s *Store, key, hash string) error {
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	if err := lockObjectKey(ctx, tx, liveBucket, key); err != nil {
		return err
	}
	var current string
	if err := tx.QueryRow(ctx, `SELECT hash FROM objects WHERE bucket = $1 AND object_id = $2`,
		liveBucket, key).Scan(&current); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO blobs (bucket, hash) VALUES ($1, $2)
		 ON CONFLICT (bucket, hash) DO UPDATE SET hash = excluded.hash`, liveBucket, hash); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO objects (bucket, object_id, hash) VALUES ($1, $2, $3)
		 ON CONFLICT (bucket, object_id) DO UPDATE SET hash = excluded.hash`,
		liveBucket, key, hash); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// TestSweepDoesNotStarveDatabaseWrites is the sibling that measures Postgres
// rather than APFS.
//
// TestSweepDoesNotStarveWriters goes through HTTP and Put, where a request
// costs ~60 ms of which the database is ~5 ms. That bounds total request
// cost, which is the right thing to promise a client — but it means a
// Postgres that got three times slower under a drain would move p99 from
// 60 ms to 70 ms and sail through a 30% slack. Since database contention is
// the entire thing this migration was bought to fix, it needs a measurement
// that can see it.
//
// So: the same transaction shape with the fsyncs removed, which raises the
// write rate from ~130/s to ~2000/s and makes the database the bottleneck.
// The assertion is a ratio of p99 to p99, so it needs no calibration and
// does not care how fast the host is.
func TestSweepDoesNotStarveDatabaseWrites(t *testing.T) {
	const window = 4 * time.Second

	cfg := newTestConfig(t)
	cfg.SweepBudget = 4 * window
	s := openTestStore(t, cfg)
	// No files: this test is about what the database does. Reclaim still
	// runs its full transaction per blob.
	seedBacklog(t, s, 20000, false)
	s.lastOrphanSweep.Store(time.Now().UnixNano())

	dbWriteWindow(t, s, time.Second) // discarded, as in the sibling test
	base := dbWriteWindow(t, s, window)

	drainCtx, stopDrain := context.WithCancel(context.WithoutCancel(t.Context()))
	defer stopDrain()
	swept := make(chan struct{})
	var sweepRes SweepResult
	go func() {
		defer close(swept)
		sweepRes, _ = s.Sweep(drainCtx)
	}()
	for start := time.Now(); backlogRows(t, s) == 20000; {
		if time.Since(start) > 10*time.Second {
			t.Fatal("the drain reclaimed nothing in 10s; the measurement would be meaningless")
		}
		time.Sleep(10 * time.Millisecond)
	}

	before := backlogRows(t, s)
	watching := make(chan struct{})
	lockQueue := watchBlobsLockQueue(t, s, watching)
	drain := dbWriteWindow(t, s, window)
	close(watching)
	peakLockQueue := <-lockQueue
	drained := before - backlogRows(t, s)
	stopDrain()
	<-swept

	rate := func(d []time.Duration) float64 { return float64(len(d)) / window.Seconds() }
	t.Logf("db-only writes: %.0f/s baseline, %.0f/s during the drain (%.0f%% of baseline)",
		rate(base), rate(drain), 100*rate(drain)/rate(base))
	t.Logf("db-only p50 %v -> %v, p95 %v -> %v, p99 %v -> %v",
		dur(pct(base, 0.5)), dur(pct(drain, 0.5)), dur(pct(base, 0.95)), dur(pct(drain, 0.95)),
		dur(pct(base, 0.99)), dur(pct(drain, 0.99)))
	t.Logf("drain reclaimed %d during the window (%.0f/s), %d total; peak blobs lock queue %d",
		drained, float64(drained)/window.Seconds(), sweepRes.Reclaimed, peakLockQueue)

	if peakLockQueue > 0 {
		t.Errorf("%d client backend(s) were queued on a relation lock on blobs during "+
			"the drain, want 0", peakLockQueue)
	}
	if drained < 200 {
		t.Errorf("the drain reclaimed only %d rows during the window (want >= 200); "+
			"the sweeper was not really running", drained)
	}
	// The two ratios. Both are calibration-free: they compare two windows of
	// the same work on the same machine, minutes apart.
	if got := rate(drain) / rate(base); got < 0.70 {
		t.Errorf("database write throughput during the drain is %.0f%% of baseline, want >= 70%%",
			100*got)
	}
	if got := float64(pct(drain, 0.99)) / float64(pct(base, 0.99)); got > 2.5 {
		t.Errorf("database write p99 rose %.1fx during the drain (%v -> %v), want <= 2.5x",
			got, dur(pct(base, 0.99)), dur(pct(drain, 0.99)))
	}
}
