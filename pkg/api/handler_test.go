package api

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/qjkee/objectstore/pkg/store"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type testServer struct {
	*httptest.Server
	store *store.Store
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	s := newTestStore(t)
	srv := httptest.NewServer(NewHandler(s).Mux())
	t.Cleanup(srv.Close)
	return &testServer{Server: srv, store: s}
}

func (ts *testServer) do(t *testing.T, method, path string, body io.Reader, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func (ts *testServer) put(t *testing.T, path, body string) *http.Response {
	t.Helper()
	return ts.do(t, http.MethodPut, path, strings.NewReader(body), nil)
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func TestStatusFor(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"invalid name", store.ErrInvalidName, http.StatusBadRequest},
		{"not found", store.ErrNotFound, http.StatusNotFound},
		{"blob missing", store.ErrBlobMissing, http.StatusInternalServerError},
		{"disk full", syscall.ENOSPC, http.StatusInsufficientStorage},
		{"client gone", context.Canceled, 0},
		{"connection reset", syscall.ECONNRESET, 0},
		{"broken pipe", syscall.EPIPE, 0},
		{"truncated body", io.ErrUnexpectedEOF, 0},
		{"unknown", errors.New("something else"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		if got := statusFor(c.err); got != c.want {
			t.Errorf("statusFor(%s) = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestStatusForPostgresErrors(t *testing.T) {
	cases := []struct {
		name string
		code string
		want int
	}{
		{"serialization_failure", "40001", http.StatusServiceUnavailable},
		{"lock_not_available", "55P03", http.StatusServiceUnavailable},
		{"query_canceled", "57014", http.StatusServiceUnavailable},
		// What a real outage sends, as opposed to what is easy to construct:
		// 57P01 is every backend's farewell from a clean `pg_ctl stop`.
		{"connection_failure", "08006", http.StatusServiceUnavailable},
		{"connection_does_not_exist", "08003", http.StatusServiceUnavailable},
		{"admin_shutdown", "57P01", http.StatusServiceUnavailable},
		{"cannot_connect_now", "57P03", http.StatusServiceUnavailable},
		{"unique_violation", "23505", http.StatusInternalServerError},
	}
	for _, c := range cases {
		err := &pgconn.PgError{Code: c.code}
		if got := statusFor(err); got != c.want {
			t.Errorf("statusFor(%s) = %d, want %d", c.name, got, c.want)
		}
	}

	// A database that is simply not listening never produces a PgError at
	// all, which is how it used to fall through to a generic 500 with no
	// Retry-After.
	down := realConnectError(t)
	var connErr *pgconn.ConnectError
	if !errors.As(down, &connErr) {
		t.Fatalf("pgx returned %T for an unreachable server, want *pgconn.ConnectError: %v", down, down)
	}
	if got := statusFor(down); got != http.StatusServiceUnavailable {
		t.Errorf("statusFor(database unreachable) = %d, want 503", got)
	}
}

// realConnectError returns the error pgx produces when nothing is listening.
// *pgconn.ConnectError keeps its cause unexported and its Error() dereferences
// store.Config, so a literal is neither constructible nor safe to call — the only
// honest way to cover this shape is to provoke one.
func realConnectError(t *testing.T) error {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // the port is now known-free, so the dial is refused at once

	conn, err := pgx.Connect(t.Context(), "postgres://objectstore@"+addr+"/objectstore")
	if err == nil {
		conn.Close(t.Context())
		t.Fatalf("connected to %s, which nothing is listening on", addr)
	}
	return err
}

// tcpProxy forwards to a backend and can reset every connection it opened.
// That is what a Postgres kill, an OOM or a failover looks like to a client
// in the middle of a transaction, and it is not reproducible by closing the
// pool: a clean close is a FIN, not an RST.
type tcpProxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []*net.TCPConn
}

func newTCPProxy(t *testing.T, backend string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &tcpProxy{ln: ln}
	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			up, err := net.Dial("tcp", backend)
			if err != nil {
				down.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, down.(*net.TCPConn))
			p.mu.Unlock()
			go func() { io.Copy(up, down); up.Close() }()
			go func() { io.Copy(down, up); down.Close() }()
		}
	}()
	t.Cleanup(func() { ln.Close(); p.reset() })
	return p
}

// reset tears every connection down with RST rather than FIN, so the client
// side surfaces ECONNRESET/EPIPE — the same error values a vanished HTTP
// client produces, which is the whole difficulty.
func (p *tcpProxy) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.SetLinger(0)
		c.Close()
	}
	p.conns = nil
}

// TestDatabaseResetMidPutIsNotAnEmpty200 is the one that matters: the
// database socket dies inside Put's transaction while the HTTP client is
// still perfectly connected. statusFor sees ECONNRESET and says "client
// gone, write nothing" — correct before the port, when that error could only
// come from the request body. Writing nothing lets net/http complete the
// response as 200 with an empty body and no ETag, and promhttp records a
// 200: a success-shaped answer for a row that was never committed.
func TestDatabaseResetMidPutIsNotAnEmpty200(t *testing.T) {
	cfg := newTestConfig(t)
	direct, err := url.Parse(cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	proxy := newTCPProxy(t, direct.Host)
	through := *direct
	through.Host = proxy.ln.Addr().String()
	cfg.DatabaseURL = through.String()

	s := openTestStore(t, cfg)
	srv := httptest.NewServer(NewHandler(s).Mux())
	t.Cleanup(srv.Close)

	// Fires inside the transaction, after the objects row is written and
	// before the commit that would make it real.
	var once sync.Once
	s.SetBeforeCommitHook(func() { once.Do(proxy.reset) })

	resp, err := srv.Client().Do(mustRequest(t, http.MethodPut, srv.URL+"/objects/b1/a", "hello"))
	if err != nil {
		t.Fatalf("PUT: %v — the client's own connection must survive, or this test is measuring the wrong socket", err)
	}
	defer resp.Body.Close()
	body := readBody(t, resp)

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		t.Fatalf("PUT answered %d etag=%q body=%q after the database connection was reset "+
			"mid-transaction; nothing was committed", resp.StatusCode, resp.Header.Get("ETag"), body)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("PUT = %d, want 503 (body %q)", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want \"1\"", got)
	}

	// And the 503 told the truth: no row survived. Retried because the pool
	// is still holding connections the reset killed and finds out one at a
	// time.
	ctx := context.WithoutCancel(t.Context())
	var headErr error
	for range 5 {
		if _, headErr = s.Head(ctx, "b1", "a"); headErr == nil || errors.Is(headErr, store.ErrNotFound) {
			break
		}
	}
	if !errors.Is(headErr, store.ErrNotFound) {
		t.Errorf("Head after the failed PUT = %v, want store.ErrNotFound — the response claimed nothing was stored", headErr)
	}
}

func mustRequest(t *testing.T, method, url, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return req
}

func TestLockTimeoutActuallyFires(t *testing.T) {
	ts := newTestServer(t)
	ctx := t.Context()

	tx1, err := ts.store.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)

	if _, err := tx1.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, "b1", "hotkey"); err != nil {
		t.Fatalf("acquire lock in tx1: %v", err)
	}

	tx2, err := ts.store.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)

	start := time.Now()
	_, err = tx2.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, "b1", "hotkey")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected lock timeout, got success")
	}

	if elapsed < 4*time.Second || elapsed > 7*time.Second {
		t.Errorf("lock timeout fired after %v, want ~5s", elapsed)
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError, got %T", err)
	}
	if pgErr.Code != "55P03" {
		t.Errorf("error code = %s, want 55P03 (lock_not_available)", pgErr.Code)
	}

	status := statusFor(err)
	if status != http.StatusServiceUnavailable {
		t.Errorf("statusFor(lock timeout) = %d, want 503", status)
	}
}

func TestPutThenGet(t *testing.T) {
	ts := newTestServer(t)

	resp := ts.put(t, "/objects/b1/a", "hello")
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("PUT status = %d, want 201", resp.StatusCode)
	}

	resp = ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET status = %d, want 200", resp.StatusCode)
	}
	if got := resp.ContentLength; got != 5 {
		t.Errorf("Content-Length = %d, want 5", got)
	}
	if got := readBody(t, resp); got != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}
}

func TestPutTwiceStatus(t *testing.T) {
	ts := newTestServer(t)

	if got := ts.put(t, "/objects/b1/a", "hello").StatusCode; got != http.StatusCreated {
		t.Errorf("first PUT = %d, want 201", got)
	}
	if got := ts.put(t, "/objects/b1/a", "world").StatusCode; got != http.StatusOK {
		t.Errorf("second PUT = %d, want 200", got)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != "world" {
		t.Errorf("body = %q, want %q", got, "world")
	}
}

func TestHeadReturnsLengthWithoutBody(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")

	resp := ts.do(t, http.MethodHead, "/objects/b1/a", nil, nil)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("HEAD status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != 5 {
		t.Errorf("Content-Length = %d, want 5", resp.ContentLength)
	}
	if got := readBody(t, resp); got != "" {
		t.Errorf("HEAD body = %q, want empty", got)
	}
}

func TestGetMissingIs404(t *testing.T) {
	ts := newTestServer(t)
	if got := ts.do(t, http.MethodGet, "/objects/b1/nope", nil, nil).StatusCode; got != http.StatusNotFound {
		t.Errorf("GET missing = %d, want 404", got)
	}
}

func TestDeleteIs204ThenGet404(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")

	if got := ts.do(t, http.MethodDelete, "/objects/b1/a", nil, nil).StatusCode; got != http.StatusNoContent {
		t.Errorf("DELETE = %d, want 204", got)
	}
	if got := ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil).StatusCode; got != http.StatusNotFound {
		t.Errorf("GET after DELETE = %d, want 404", got)
	}
}

func TestDeleteMissingIs404(t *testing.T) {
	ts := newTestServer(t)
	if got := ts.do(t, http.MethodDelete, "/objects/b1/nope", nil, nil).StatusCode; got != http.StatusNotFound {
		t.Errorf("DELETE missing = %d, want 404", got)
	}
}

// TestInvalidNamesAre400 includes the percent-encoded forms: PathValue
// returns the decoded segment, so %2F and %2E%2E must reach validation rather
// than escaping the bucket directory.
func TestInvalidNamesAre400(t *testing.T) {
	ts := newTestServer(t)
	paths := []string{
		"/objects/b1/%2E%2E",
		"/objects/b1/a%2Fb",
		"/objects/" + strings.Repeat("a", 64) + "/a",
		"/objects/B1/a",
		"/objects/b1/" + strings.Repeat("a", 256),
		"/objects/%2E%2E/a",
	}
	for _, p := range paths {
		for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
			resp := ts.do(t, method, p, strings.NewReader("x"), nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s %s = %d, want 400", method, p, resp.StatusCode)
			}
		}
	}
}

// TestDotDotSegmentNeverReachesAHandler pins what net/http does with a
// literal ".." before routing: it cleans the path and redirects, so the
// segment can never reach PathValue and can never traverse out of the bucket
// directory. The encoded form %2E%2E does reach validation and is covered by
// TestInvalidNamesAre400.
func TestDotDotSegmentNeverReachesAHandler(t *testing.T) {
	ts := newTestServer(t)
	client := *ts.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/objects/b1/..", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatalf("status = %d, want a redirect", resp.StatusCode)
	}
	if got := resp.Header.Get("Location"); got != "/objects" {
		t.Errorf("Location = %q, want %q — the segment must be resolved away, not passed through", got, "/objects")
	}
}

func TestOversizeContentLengthIs413(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.MaxObjectSize = 8
	s := openTestStore(t, cfg)
	srv := httptest.NewServer(NewHandler(s).Mux())
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		srv.URL+"/objects/b1/a", strings.NewReader("way too long"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize with Content-Length = %d, want 413", resp.StatusCode)
	}
	if n := countFiles(t, s.TmpDir()); n != 0 {
		t.Errorf("temp files left behind = %d, want 0", n)
	}
}

func TestOversizeChunkedIs413(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.MaxObjectSize = 8
	s := openTestStore(t, cfg)
	srv := httptest.NewServer(NewHandler(s).Mux())
	defer srv.Close()

	// An io.Reader of unknown length makes net/http use chunked encoding, so
	// there is no Content-Length to check and MaxBytesReader has to catch it.
	body := io.NopCloser(strings.NewReader("way too long for the limit"))
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		srv.URL+"/objects/b1/a", body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize chunked = %d, want 413", resp.StatusCode)
	}
	if n := countFiles(t, s.TmpDir()); n != 0 {
		t.Errorf("temp files left behind = %d, want 0", n)
	}
}

func hdr(k, v string) http.Header { return http.Header{k: {v}} }

func TestGetETagAndNotModified(t *testing.T) {
	ts := newTestServer(t)
	etag := ts.put(t, "/objects/b1/a", "hello").Header.Get("ETag")
	if etag == "" {
		t.Fatal("PUT returned no ETag")
	}
	if strings.HasPrefix(etag, "W/") {
		t.Errorf("ETag = %q, want a strong tag — If-Match needs strong comparison", etag)
	}

	resp := ts.do(t, http.MethodGet, "/objects/b1/a", nil, hdr("If-None-Match", etag))
	if resp.StatusCode != http.StatusNotModified {
		t.Errorf("If-None-Match with the current tag = %d, want 304", resp.StatusCode)
	}
	if got := readBody(t, resp); got != "" {
		t.Errorf("304 body = %q, want empty", got)
	}

	resp = ts.do(t, http.MethodGet, "/objects/b1/a", nil, hdr("If-None-Match", `"stale"`))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("If-None-Match with a stale tag = %d, want 200", resp.StatusCode)
	}
	if got := readBody(t, resp); got != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}

	resp = ts.do(t, http.MethodGet, "/objects/b1/a", nil, hdr("If-None-Match", `"stale", `+etag))
	if resp.StatusCode != http.StatusNotModified {
		t.Errorf("If-None-Match list containing the current tag = %d, want 304", resp.StatusCode)
	}
	resp = ts.do(t, http.MethodGet, "/objects/b1/a", nil, hdr("If-None-Match", "*"))
	if resp.StatusCode != http.StatusNotModified {
		t.Errorf("If-None-Match: * on an existing object = %d, want 304", resp.StatusCode)
	}
}

// TestLostUpdatePrevented is the whole point of the feature: two clients read
// the same ETag and both write back.
func TestLostUpdatePrevented(t *testing.T) {
	ts := newTestServer(t)
	etag := ts.put(t, "/objects/b1/a", "base").Header.Get("ETag")

	type result struct {
		status int
		body   string
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i, body := range []string{"first", "second"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := ts.do(t, http.MethodPut, "/objects/b1/a",
				strings.NewReader(body), hdr("If-Match", etag))
			results[i] = result{resp.StatusCode, body}
		}()
	}
	wg.Wait()

	var winner string
	wins, conflicts := 0, 0
	for _, r := range results {
		switch {
		case r.status >= 200 && r.status < 300:
			wins++
			winner = r.body
		case r.status == http.StatusPreconditionFailed:
			conflicts++
		default:
			t.Errorf("unexpected status %d", r.status)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("got %d winners and %d conflicts, want exactly 1 of each", wins, conflicts)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != winner {
		t.Errorf("stored body = %q, want the winner's %q", got, winner)
	}
}

func TestIfMatchOnMissingIs412(t *testing.T) {
	ts := newTestServer(t)
	resp := ts.do(t, http.MethodPut, "/objects/b1/nope",
		strings.NewReader("x"), hdr("If-Match", `"whatever"`))
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("If-Match on a missing path = %d, want 412 (not 404)", resp.StatusCode)
	}
}

func TestIfNoneMatchStar(t *testing.T) {
	ts := newTestServer(t)

	resp := ts.do(t, http.MethodPut, "/objects/b1/a",
		strings.NewReader("first"), hdr("If-None-Match", "*"))
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("create-only on an absent path = %d, want 201", resp.StatusCode)
	}

	resp = ts.do(t, http.MethodPut, "/objects/b1/a",
		strings.NewReader("second"), hdr("If-None-Match", "*"))
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("create-only on an existing path = %d, want 412", resp.StatusCode)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != "first" {
		t.Errorf("body after the 412 = %q, want %q", got, "first")
	}
}

func TestIfMatchOnDelete(t *testing.T) {
	ts := newTestServer(t)
	etag := ts.put(t, "/objects/b1/a", "hello").Header.Get("ETag")

	resp := ts.do(t, http.MethodDelete, "/objects/b1/a", nil, hdr("If-Match", `"wrong"`))
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("DELETE with a mismatched If-Match = %d, want 412", resp.StatusCode)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != "hello" {
		t.Errorf("body after the 412 = %q, want %q", got, "hello")
	}

	resp = ts.do(t, http.MethodDelete, "/objects/b1/a", nil, hdr("If-Match", etag))
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("DELETE with a matching If-Match = %d, want 204", resp.StatusCode)
	}
}

func TestFailedPreconditionLeavesNoTrace(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")

	resp := ts.do(t, http.MethodPut, "/objects/b1/a",
		strings.NewReader("other"), hdr("If-None-Match", "*"))
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412", resp.StatusCode)
	}

	if n := countFiles(t, ts.store.TmpDir()); n != 0 {
		t.Errorf("temp files = %d, want 0", n)
	}
	var blobs int
	if err := ts.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM blobs`).Scan(&blobs); err != nil {
		t.Fatal(err)
	}
	if blobs != 1 {
		t.Errorf("blobs rows = %d, want 1 — a 412 must create none", blobs)
	}
}

// TestOversizeWithFailingIfMatchIs412 proves the advisory pre-check fires:
// the doomed body is never transferred, so the answer is 412 and not 413.
func TestOversizeWithFailingIfMatchIs412(t *testing.T) {
	// Opened at the small limit from the start: "hello" is 5 bytes, so the
	// setup PUT still fits, and the oversize request below meets a handler
	// that would reject it on size if the precondition did not fire first.
	cfg := newTestConfig(t)
	cfg.MaxObjectSize = 8
	ts := newTestServerWithConfig(t, cfg)
	ts.put(t, "/objects/b1/a", "hello")
	srv := ts.Server

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		srv.URL+"/objects/b1/a", strings.NewReader(strings.Repeat("x", 100)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("If-Match", `"wrong"`)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("status = %d, want 412 — the advisory check must run before the size check", resp.StatusCode)
	}
}

// TestABA: the ETag is a content hash, not a version counter, so content
// going A -> B -> A leaves an old "A" tag succeeding. That is the better
// behaviour: the client's read-modify-write was computed against A and
// current content is A, so its change is still valid.
func TestABA(t *testing.T) {
	ts := newTestServer(t)
	etagA := ts.put(t, "/objects/b1/a", "A").Header.Get("ETag")
	ts.put(t, "/objects/b1/a", "B")
	ts.put(t, "/objects/b1/a", "A")

	resp := ts.do(t, http.MethodPut, "/objects/b1/a",
		strings.NewReader("C"), hdr("If-Match", etagA))

	if resp.StatusCode != http.StatusOK {
		t.Errorf("If-Match with the original tag after A->B->A = %d, want 200", resp.StatusCode)
	}
}

func TestIdenticalContentSharesETag(t *testing.T) {
	ts := newTestServer(t)
	first := ts.put(t, "/objects/b1/a", "hello").Header.Get("ETag")
	second := ts.put(t, "/objects/b1/b", "hello").Header.Get("ETag")

	if first != second {
		t.Errorf("ETags %q and %q differ for identical content", first, second)
	}
}

func TestNoPreconditionIsLastWriterWins(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "first")

	if got := ts.put(t, "/objects/b1/a", "second").StatusCode; got != http.StatusOK {
		t.Errorf("status = %d, want 200", got)
	}
	if got := readBody(t, ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil)); got != "second" {
		t.Errorf("body = %q, want %q", got, "second")
	}
}

func TestMalformedPreconditionIs400(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")

	for _, v := range []string{`a`, `"a",`, `""x`} {
		resp := ts.do(t, http.MethodPut, "/objects/b1/a", strings.NewReader("x"), hdr("If-Match", v))
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("If-Match: %s = %d, want 400", v, resp.StatusCode)
		}
	}
}

func TestRangeRequest(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "0123456789")

	resp := ts.do(t, http.MethodGet, "/objects/b1/a", nil,
		http.Header{"Range": {"bytes=2-5"}})

	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206", resp.StatusCode)
	}
	if got := readBody(t, resp); got != "2345" {
		t.Errorf("body = %q, want %q", got, "2345")
	}
}
