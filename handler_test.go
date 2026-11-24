package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
)

type testServer struct {
	*httptest.Server
	store *Store
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
		{"invalid name", ErrInvalidName, http.StatusBadRequest},
		{"not found", ErrNotFound, http.StatusNotFound},
		{"blob missing", ErrBlobMissing, http.StatusInternalServerError},
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
	ts := newTestServer(t)
	ts.store.cfg.MaxObjectSize = 8
	srv := httptest.NewServer(NewHandler(ts.store).Mux())
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
	if n := countFiles(t, ts.store.tmpDir()); n != 0 {
		t.Errorf("temp files left behind = %d, want 0", n)
	}
}

func TestOversizeChunkedIs413(t *testing.T) {
	ts := newTestServer(t)
	ts.store.cfg.MaxObjectSize = 8
	srv := httptest.NewServer(NewHandler(ts.store).Mux())
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
	if n := countFiles(t, ts.store.tmpDir()); n != 0 {
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

	if n := countFiles(t, ts.store.tmpDir()); n != 0 {
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
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")
	ts.store.cfg.MaxObjectSize = 8
	srv := httptest.NewServer(NewHandler(ts.store).Mux())
	defer srv.Close()

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
