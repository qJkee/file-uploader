package api

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

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

// TestRealClientDisconnectMidPut aborts a live upload at the socket, which is
// what a phone losing signal actually does. The spec is explicit: log at
// debug, no 5xx.
func TestRealClientDisconnectMidPut(t *testing.T) {
	logs := captureLogs(t)
	ts := newTestServer(t)
	before := testutil.ToFloat64(metrics.RequestsTotal.WithLabelValues("PUT", "500"))

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		// Close sends RST rather than FIN, so the server sees ECONNRESET.
		if err := tcp.SetLinger(0); err != nil {
			t.Fatalf("set linger: %v", err)
		}
	}
	fmt.Fprint(conn, "PUT /objects/b1/a HTTP/1.1\r\nHost: test\r\nContent-Length: 1000000\r\n\r\n")
	conn.Write([]byte(strings.Repeat("x", 100)))
	conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && logs.String() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	out := logs.String()
	if out == "" {
		t.Fatal("the server logged nothing; the aborted request never reached the handler")
	}

	if strings.Contains(out, "request failed") {
		t.Errorf("an aborted upload was logged as a server error:\n%s", out)
	}
	if got := testutil.ToFloat64(metrics.RequestsTotal.WithLabelValues("PUT", "500")) - before; got != 0 {
		t.Errorf("requests_total{PUT,500} rose by %v, want 0 — a vanished client is not a server error", got)
	}
	if n := countFiles(t, ts.store.TmpDir()); n != 0 {
		t.Errorf("temp files = %d, want 0", n)
	}
}

// TestObjectsAreNotSniffed: an object is opaque and is never parsed, so its
// bytes must not be allowed to choose their own Content-Type. Without this,
// anyone who can PUT can serve scripted HTML from this service's origin.
func TestObjectsAreNotSniffed(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/evil", `<html><script>alert(1)</script></html>`)

	resp := ts.do(t, http.MethodGet, "/objects/b1/evil", nil, nil)

	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream", got)
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := readBody(t, resp); got != `<html><script>alert(1)</script></html>` {
		t.Errorf("body = %q, want the bytes returned verbatim", got)
	}
}

// TestHealthzDoesNoDiskWrite: /healthz is unauthenticated and uncacheable by
// clients, so doing filesystem work per call turns a trickle of traffic into
// heavy disk load.
func TestHealthzDoesNoDiskWrite(t *testing.T) {
	ts := newTestServer(t)
	for range 20 {
		if got := ts.do(t, http.MethodGet, "/healthz", nil, nil).StatusCode; got != http.StatusOK {
			t.Fatalf("GET /healthz = %d, want 200", got)
		}
	}
	if n := countFiles(t, ts.store.TmpDir()); n != 0 {
		t.Errorf("temp files after 20 health checks = %d, want 0", n)
	}
}

// TestRepeatedIfMatchHeaderLines: RFC 9110 §5.3 makes two header lines
// equivalent to one comma-separated list, so a legitimate write must not get
// a spurious 412.
func TestRepeatedIfMatchHeaderLines(t *testing.T) {
	ts := newTestServer(t)
	etag := ts.put(t, "/objects/b1/a", "hello").Header.Get("ETag")

	resp := ts.do(t, http.MethodPut, "/objects/b1/a", strings.NewReader("world"),
		http.Header{"If-Match": {`"wrong"`, etag}})

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 — either tag matching is enough", resp.StatusCode)
	}
}

func allPut(int) string { return http.MethodPut }

// putOrDelete makes the even writers update and the odd ones delete.
func putOrDelete(i int) string {
	if i%2 == 0 {
		return http.MethodPut
	}
	return http.MethodDelete
}

// concurrentWrites fires 16 writes at one path, all carrying the same
// precondition, and returns each writer's status beside the body it sent —
// empty for the DELETE writers, which send none. method picks writer i's
// verb. 16 rather than TestLostUpdatePrevented's 2 because two writers under
// READ COMMITTED serialise by accident often enough to hide a missing lock.
func concurrentWrites(t *testing.T, ts *testServer, path string, header http.Header, method func(i int) string) (statuses []int, bodies []string) {
	t.Helper()
	const n = 16
	statuses, bodies = make([]int, n), make([]string, n)
	start := make(chan struct{})
	var warm, wg sync.WaitGroup
	for i := range n {
		warm.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A HEAD first: it goes through the same pgxpool as the write, so
			// by the time the barrier opens the pool's connections are
			// already open. Opening one costs far more than the window under
			// test, so without this the writers arrive too scattered to
			// overlap and the bug hides about half the time. (The HTTP
			// connections do not survive: httptest's client caps idle
			// connections per host at 2.) Deferred, because ts.do calls
			// t.Fatalf on a transport error and a missed warm.Done would hang
			// the package rather than fail it.
			func() {
				defer warm.Done()
				ts.do(t, http.MethodHead, path, nil, nil)
			}()
			<-start // release them together, so the windows actually overlap
			if method(i) == http.MethodDelete {
				statuses[i] = ts.do(t, http.MethodDelete, path, nil, header).StatusCode
				return
			}
			bodies[i] = fmt.Sprintf("writer-%02d", i)
			statuses[i] = ts.do(t, http.MethodPut, path, strings.NewReader(bodies[i]), header).StatusCode
		}()
	}
	warm.Wait()
	close(start)
	wg.Wait()
	return statuses, bodies
}

// TestConcurrentIfMatchPicksOneWinner: every writer read the same ETag, so
// exactly one may win. Without the per-key lock the precondition read and the
// write sit in different snapshots and several writers pass the check.
func TestConcurrentIfMatchPicksOneWinner(t *testing.T) {
	ts := newTestServer(t)
	etag := ts.put(t, "/objects/b1/a", "base").Header.Get("ETag")

	statuses, bodies := concurrentWrites(t, ts, "/objects/b1/a", hdr("If-Match", etag), allPut)

	winner, wins, conflicts := "", 0, 0
	for i, st := range statuses {
		switch {
		case st >= 200 && st < 300:
			wins++
			winner = bodies[i]
		case st == http.StatusPreconditionFailed:
			conflicts++
		default:
			t.Errorf("writer %d: status %d, want 2xx or 412", i, st)
		}
	}
	if want := len(statuses) - 1; wins != 1 || conflicts != want {
		t.Fatalf("got %d winners and %d conflicts, want 1 and %d — a second winner is a lost update", wins, conflicts, want)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != winner {
		t.Errorf("stored body = %q, want the winner's %q", got, winner)
	}
}

// TestConcurrentCreateOnlyPicksOneWinner: If-None-Match: * is create-only, so
// of 16 writers racing for one absent path exactly one may create it. This is
// the case SELECT ... FOR UPDATE cannot cover — there is no row to lock.
func TestConcurrentCreateOnlyPicksOneWinner(t *testing.T) {
	ts := newTestServer(t)

	statuses, bodies := concurrentWrites(t, ts, "/objects/b1/a", hdr("If-None-Match", "*"), allPut)

	winner, created, conflicts := "", 0, 0
	for i, st := range statuses {
		switch st {
		case http.StatusCreated:
			created++
			winner = bodies[i]
		case http.StatusPreconditionFailed:
			conflicts++
		default:
			t.Errorf("writer %d: status %d, want 201 or 412", i, st)
		}
	}
	if want := len(statuses) - 1; created != 1 || conflicts != want {
		t.Fatalf("got %d creations and %d conflicts, want 1 and %d — create-only let a second writer through", created, conflicts, want)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != winner {
		t.Errorf("stored body = %q, want the creator's %q", got, winner)
	}
}

// TestConcurrentIfMatchDeleteAndPutPicksOneWinner: Delete takes the same
// per-key lock as Put and needs it for the same reason, but neither test
// above issues a DELETE, so dropping that call would go unnoticed. Sixteen
// clients hold one ETag, half updating and half deleting: without the lock a
// DELETE reads h1, passes its precondition, and then removes a row a
// concurrent PUT has already moved to h2 — a lost update with nothing left
// behind, and two clients both told they succeeded.
func TestConcurrentIfMatchDeleteAndPutPicksOneWinner(t *testing.T) {
	ts := newTestServer(t)
	etag := ts.put(t, "/objects/b1/a", "base").Header.Get("ETag")

	statuses, bodies := concurrentWrites(t, ts, "/objects/b1/a", hdr("If-Match", etag), putOrDelete)

	winner, wins, conflicts := -1, 0, 0
	for i, st := range statuses {
		switch {
		case st >= 200 && st < 300:
			wins++
			winner = i
		case st == http.StatusPreconditionFailed:
			conflicts++
		default:
			t.Errorf("writer %d (%s): status %d, want 2xx or 412", i, putOrDelete(i), st)
		}
	}
	if want := len(statuses) - 1; wins != 1 || conflicts != want {
		t.Fatalf("got %d winners and %d conflicts, want 1 and %d — writers holding one ETag both wrote", wins, conflicts, want)
	}

	resp := ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)
	if putOrDelete(winner) == http.MethodDelete {
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("writer %d deleted the object, but GET = %d, want 404", winner, resp.StatusCode)
		}
		return
	}
	if got := readBody(t, resp); got != bodies[winner] {
		t.Errorf("stored body = %q, want the winner's %q", got, bodies[winner])
	}
}
