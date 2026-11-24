package main

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
	"net/http/httptest"
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

// runPhase drives writers and readers flat out for window and returns their
// latency distributions. Requests use the test's context, not a deadline, so
// the last request of the window completes instead of counting as a failure.
func runPhase(t *testing.T, ts *testServer, c *http.Client, name string, window time.Duration) phase {
	t.Helper()
	deadline := time.Now().Add(window)
	puts := make([][]time.Duration, contentionWriters)
	gets := make([][]time.Duration, contentionReaders)
	putFail := make([]int, contentionWriters)
	getFail := make([]int, contentionReaders)
	// First failure only: logging each one would put a mutex in the hot loop
	// and bury the table under thousands of identical lines.
	firstErr := make([]error, contentionWriters+contentionReaders)

	start := time.Now()
	var wg sync.WaitGroup
	for i := range contentionWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			url := fmt.Sprintf("%s/objects/%s/writer-%d", ts.URL, liveBucket, i)
			for n := 0; time.Now().Before(deadline); n++ {
				body := fmt.Sprintf("%s-%d-%d-%s", name, i, n, contentionPayload)
				d, err := timeRequest(t.Context(), c, http.MethodPut, url, body)
				if err != nil {
					putFail[i]++
					firstErr[i] = cmp.Or(firstErr[i], err)
					continue
				}
				puts[i] = append(puts[i], d)
			}
		}()
	}
	for i := range contentionReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			url := fmt.Sprintf("%s/objects/%s/reader-%d", ts.URL, liveBucket, i)
			for time.Now().Before(deadline) {
				d, err := timeRequest(t.Context(), c, http.MethodGet, url, "")
				if err != nil {
					getFail[i]++
					firstErr[contentionWriters+i] = cmp.Or(firstErr[contentionWriters+i], err)
					continue
				}
				gets[i] = append(gets[i], d)
			}
		}()
	}
	wg.Wait()

	p := phase{name: name, elapsed: time.Since(start)}
	for i := range puts {
		p.puts = append(p.puts, puts[i]...)
		p.putFail += putFail[i]
	}
	for i := range gets {
		p.gets = append(p.gets, gets[i]...)
		p.getFail += getFail[i]
	}
	p.err = errors.Join(firstErr...)
	slices.Sort(p.puts)
	slices.Sort(p.gets)
	return p
}

// TestSweepDoesNotStarveWriters is the audit's measurement, rerun on
// Postgres. Under SQLite the same shape collapsed from 1837 PUTs to 99 and
// pushed p99 from 117 ms to 3.37 s, because every reclaim re-took the one
// global write lock and the busy handler has no queue. Postgres should make
// the sweeper's deletes and the clients' writes touch disjoint rows.
//
// It asserts two different things, because the obvious one is not enough on
// its own. The client-visible bounds (throughput ratio, p99) say the drain
// did not hurt anybody. The two instruments below them — reclaims completed,
// and time spent queueing for a pool connection — are what actually catch a
// write-lock regression on hardware where the sweeper is too slow to hurt
// anybody even when it holds a global lock. See the task-6 report.
func TestSweepDoesNotStarveWriters(t *testing.T) {
	cfg := newTestConfig(t)
	// One Sweep call has to outlast the measured window: a loop of Sweeps
	// would re-walk a growing trash directory between reclaim batches and
	// measure filesystem walking rather than lock contention.
	cfg.SweepBudget = 4 * contentionWindow
	s := openTestStore(t, cfg)
	srv := httptest.NewServer(NewHandler(s).Mux())
	t.Cleanup(srv.Close)
	ts := &testServer{Server: srv, store: s}

	// The default transport keeps 2 idle connections per host, so 12
	// goroutines would spend the window in TCP setup instead of in the
	// database.
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: contentionWriters + contentionReaders,
	}}
	t.Cleanup(client.CloseIdleConnections)

	for i := range contentionReaders {
		mustPut(t, s, liveBucket, fmt.Sprintf("reader-%d", i), "readable body")
	}
	// Seeded before the baseline, not between the phases, so the sweeper is
	// the only thing that differs between them.
	seedBacklog(t, s, contentionBacklog, true)

	// Discarded: without it the baseline pays for opening the pool's ten
	// connections, filling their statement caches and growing the Go heap,
	// and the drain phase comes out ~10% *faster* than the baseline it is
	// supposed to be degraded against.
	runPhase(t, ts, client, "warmup", 2*time.Second)

	statBase := s.Pool().Stat()
	base := runPhase(t, ts, client, "baseline", contentionWindow)
	poolWait(t, "baseline", statBase, s.Pool().Stat())

	// The orphan walk is an hourly job, not part of the reclaim cadence;
	// letting it fire here would measure a walk of 60k files instead of the
	// contention this test exists to detect.
	s.lastOrphanSweep.Store(time.Now().UnixNano())

	drainCtx, stopDrain := context.WithCancel(context.WithoutCancel(t.Context()))
	defer stopDrain()
	var sweepRes SweepResult
	var sweepErr error
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		sweepRes, sweepErr = s.Sweep(drainCtx)
	}()

	// Do not start the clock on an idle sweeper: the first batch query over
	// 60k rows takes a moment.
	for start := time.Now(); backlogRows(t, s) == contentionBacklog; {
		if time.Since(start) > 10*time.Second {
			t.Fatal("the drain reclaimed nothing in 10s; the measurement would be meaningless")
		}
		time.Sleep(10 * time.Millisecond)
	}

	before := backlogRows(t, s)
	statDrain := s.Pool().Stat()
	watching := make(chan struct{})
	// The watcher runs only here, so the drain phase carries one extra
	// pg_locks query every 20ms that the baseline did not — the one respect
	// in which the two phases are not identical work. The bias runs against
	// the drain, so it can only understate the throughput ratio below.
	lockQueue := watchBlobsLockQueue(t, s, watching)
	drain := runPhase(t, ts, client, "drain", contentionWindow)
	close(watching)
	peakLockQueue := <-lockQueue
	drainWait := poolWait(t, "drain", statDrain, s.Pool().Stat())
	drain.drained = before - backlogRows(t, s)

	stopDrain()
	<-swept
	// errors.Is is not enough on its own: reclaim returns
	// errors.Join(firstErr, ctx.Err()), so a sweep in which every candidate
	// failed with EIO still matches context.Canceled. Failed is the honest
	// check, and its bound is exact rather than calibrated — cancelling can
	// abort at most the one reclaimOne in flight before the loop's next
	// ctx.Err() check.
	if sweepErr != nil && !errors.Is(sweepErr, context.Canceled) {
		t.Errorf("Sweep: %v", sweepErr)
	}
	if sweepRes.Failed > 1 {
		t.Errorf("the drain failed on %d candidates (want <= 1, the one cancellation "+
			"aborts): %v", sweepRes.Failed, sweepErr)
	}

	t.Logf("%-10s %-4s %7s %8s %9s %9s %9s %9s", "phase", "op", "count", "failed", "p50", "p95", "p99", "max")
	t.Log(base.row("PUT", base.puts, base.putFail))
	t.Log(base.row("GET", base.gets, base.getFail))
	t.Log(drain.row("PUT", drain.puts, drain.putFail))
	t.Log(drain.row("GET", drain.gets, drain.getFail))
	t.Logf("PUT throughput: %.0f/s baseline, %.0f/s during the drain (%.0f%% of baseline)",
		base.putRate(), drain.putRate(), 100*drain.putRate()/base.putRate())
	t.Logf("drain: %d blobs reclaimed during the window (%.0f/s); whole sweep "+
		"reclaimed %d of %d candidates, %d failed; peak blobs lock queue %d",
		drain.drained, float64(drain.drained)/drain.elapsed.Seconds(),
		sweepRes.Reclaimed, sweepRes.Candidates, sweepRes.Failed, peakLockQueue)
	t.Log("SQLite, same shape: 1837 -> 99 PUTs (5% of baseline), p99 117ms -> 3.37s")

	// The assertion that carries this test, and the only one here that needs
	// no calibration. The client-visible bounds further down are blunt:
	// re-adding a global write lock (`LOCK TABLE blobs IN EXCLUSIVE MODE` in
	// reclaimOne) leaves PUT throughput at 96% of baseline and p99 at 115 ms,
	// because macOS F_FULLFSYNC throttles the sweeper to ~50 reclaims/s and a
	// lock taken 50 times a second cannot starve anyone. A slower sweeper
	// makes those bounds *more* forgiving, which is backwards. This one gets
	// sharper instead: the slower the sweeper, the longer each client backend
	// sits in the queue.
	if peakLockQueue > 0 {
		t.Errorf("%d client backend(s) were queued on a relation lock on blobs during "+
			"the drain, want 0 — reclaim is taking a table-level lock, which is the "+
			"SQLite behaviour this migration removed", peakLockQueue)
	}
	// Loose sanity floor only: a sweeper that got no candidates at all, or
	// one wedged outright, would make everything above vacuous. Deliberately
	// 6x below the ~320 this laptop sustains, because the rate varies with
	// the disk by an order of magnitude and this must not be the thing that
	// fails on a slower machine.
	if drain.drained < 50 {
		t.Errorf("the drain reclaimed only %d blobs during the window (want >= 50); "+
			"the sweeper was not really running, so this measurement proves nothing",
			drain.drained)
	}
	// Not an assertion: the floor scales with the machine, and the pg_locks
	// check above covers the regression this would have caught.
	if drainWait > 2500*time.Millisecond {
		t.Logf("NOTE: requests spent %v blocked on an empty pool during the drain "+
			"(typically well under 1s here); worth a look",
			drainWait.Round(time.Millisecond))
	}
	// SQLite shed ~1% of a hot key's requests as 503 under its busy handler.
	// Row locks mean there is nothing here for a client write to lose to.
	if n := base.putFail + base.getFail + drain.putFail + drain.getFail; n != 0 {
		t.Errorf("%d requests failed (baseline %d PUT / %d GET, drain %d PUT / %d GET): %v",
			n, base.putFail, base.getFail, drain.putFail, drain.getFail,
			errors.Join(base.err, drain.err))
	}

	// The client-visible bounds. The throughput one compares two windows
	// measured back to back on the same machine, so -race and a busy laptop
	// move both alike.
	//
	// The bounds are tighter than the brief's 50% / 1s because the
	// measurement turned out to be far steadier than expected: over eight
	// runs — plain, under -race, and under eight CPU hogs — the ratio stayed
	// between 0.95 and 1.14 and PUT p99 between 84 and 104 ms. 50% and 1s
	// could not have failed for any reason short of total collapse.
	if got := drain.putRate() / base.putRate(); got < 0.70 {
		t.Errorf("PUT throughput during the drain is %.0f%% of baseline, want >= 70%% "+
			"(SQLite managed 5%%)", 100*got)
	}
	if got := pct(drain.puts, 0.99); got > 500*time.Millisecond {
		t.Errorf("PUT p99 during the drain is %v, want < 500ms (SQLite: 3.37s)", got)
	}
	// Reads share the pool with writers, so a writer stuck in the database
	// holds a connection a reader needs. 250 ms is ~10x the measured p99 and
	// far below the 5s lock_timeout a genuine stall would sit on.
	if got := pct(drain.gets, 0.99); got > 250*time.Millisecond {
		t.Errorf("GET p99 during the drain is %v, want < 250ms — the drain is starving reads", got)
	}
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
