package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/qjkee/objectstore/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsEndpoint(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")

	resp := ts.do(t, http.MethodGet, "/metrics", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", resp.StatusCode)
	}
	body := readBody(t, resp)
	for _, name := range []string{
		"objectstore_requests_total",
		"objectstore_put_bytes_total",
		"objectstore_request_duration_seconds",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics does not mention %s", name)
		}
	}
}

func TestDedupHitCounter(t *testing.T) {
	ts := newTestServer(t)
	before := testutil.ToFloat64(metrics.DedupHitsTotal)

	ts.put(t, "/objects/b1/a", "same bytes")
	ts.put(t, "/objects/b1/b", "same bytes")

	if got := testutil.ToFloat64(metrics.DedupHitsTotal) - before; got != 1 {
		t.Errorf("dedup hits = %v, want 1", got)
	}
}

// TestNoHighCardinalityLabels guards the one metrics mistake that takes down
// a Prometheus server.
func TestNoHighCardinalityLabels(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/secretbucket/secretobject", "hello")
	ts.do(t, http.MethodGet, "/objects/secretbucket/secretobject", nil, nil)

	body := readBody(t, ts.do(t, http.MethodGet, "/metrics", nil, nil))

	for _, s := range []string{"secretbucket", "secretobject"} {
		if strings.Contains(body, s) {
			t.Errorf("/metrics leaks %q as a label value — unbounded cardinality", s)
		}
	}
}

func TestHealthz(t *testing.T) {
	ts := newTestServer(t)
	if got := ts.do(t, http.MethodGet, "/healthz", nil, nil).StatusCode; got != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", got)
	}
}

func TestHealthzFailsOnAClosedStore(t *testing.T) {
	ts := newTestServer(t)
	ts.store.Close()

	if got := ts.do(t, http.MethodGet, "/healthz", nil, nil).StatusCode; got != http.StatusServiceUnavailable {
		t.Errorf("GET /healthz on a closed pool = %d, want 503", got)
	}
}
func TestBlobMissingCounter(t *testing.T) {
	ts := newTestServer(t)
	ts.put(t, "/objects/b1/a", "hello")
	before := testutil.ToFloat64(metrics.BlobMissingTotal)
	unlinkBlob(t, ts.store, "b1", "hello")

	if got := ts.do(t, http.MethodGet, "/objects/b1/a", nil, nil).StatusCode; got != http.StatusInternalServerError {
		t.Errorf("GET with a missing blob = %d, want 500", got)
	}
	if got := testutil.ToFloat64(metrics.BlobMissingTotal) - before; got != 1 {
		t.Errorf("blob_missing_total delta = %v, want 1", got)
	}
}
