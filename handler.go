package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler serves the object API. It owns no state beyond the store and the
// size limit; everything durable lives behind Store.
type Handler struct {
	store         *Store
	maxObjectSize int64
}

func NewHandler(s *Store) *Handler {
	return &Handler{store: s, maxObjectSize: s.cfg.MaxObjectSize}
}

func (h *Handler) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("PUT /objects/{bucket}/{objectID}", instrument("PUT", http.HandlerFunc(h.put)))
	mux.Handle("GET /objects/{bucket}/{objectID}", instrument("GET", http.HandlerFunc(h.get)))
	mux.Handle("DELETE /objects/{bucket}/{objectID}", instrument("DELETE", http.HandlerFunc(h.delete)))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", h.health)
	return mux
}

// instrument wraps one route with the standard promhttp middleware rather
// than a hand-rolled one. The method label comes from the route, not the
// request, so it cannot be driven by a client.
func instrument(method string, next http.Handler) http.Handler {
	return promhttp.InstrumentHandlerDuration(
		requestDuration.MustCurryWith(prometheus.Labels{"method": method}),
		promhttp.InstrumentHandlerCounter(
			requestsTotal.MustCurryWith(prometheus.Labels{"method": method}),
			next))
}

// health reports ready when the metadata database answers.
//
// It deliberately does no filesystem work. The endpoint is unauthenticated
// and callers cannot be made to back off, so a write probe per request turns
// a trickle of traffic into heavy disk load for free. An unwritable data
// directory still surfaces immediately: PUTs fail with 507 and the sweeper
// errors, both of which are visible in /metrics.
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	var one int
	if err := h.store.Pool().QueryRow(r.Context(), `SELECT 1`).Scan(&one); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	bucket, objectID := r.PathValue("bucket"), r.PathValue("objectID")
	if err := validateNames(bucket, objectID); err != nil {
		h.fail(w, r, err)
		return
	}
	pre, ok := writePreconditions(r)
	if !ok {
		http.Error(w, "malformed precondition header", http.StatusBadRequest)
		return
	}
	// Advisory precondition check, before the size check and before a byte of
	// body is read, so a doomed 32 MiB upload is never transferred. It is
	// racy on its own; the authoritative check is inside Put's transaction.
	if err := h.precheck(r, bucket, objectID, pre); err != nil {
		h.fail(w, r, err)
		return
	}
	// Reject an oversize declared length before reading a byte of it.
	if r.ContentLength > h.maxObjectSize {
		http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
		return
	}
	// Catches chunked uploads that omitted or lied about a length.
	body := http.MaxBytesReader(w, r.Body, h.maxObjectSize)

	res, err := h.store.Put(r.Context(), bucket, objectID, body, pre)
	if err != nil {
		h.fail(w, r, err)
		return
	}

	putBytesTotal.Add(float64(res.Size))
	if res.Dedup {
		dedupHitsTotal.Inc()
	}

	w.Header().Set("ETag", etagOf(res.Hash))
	if res.Created {
		w.WriteHeader(http.StatusCreated)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	bucket, objectID := r.PathValue("bucket"), r.PathValue("objectID")
	if err := validateNames(bucket, objectID); err != nil {
		h.fail(w, r, err)
		return
	}

	f, hash, err := h.store.Get(r.Context(), bucket, objectID)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	defer f.Close()

	// Setting the header and handing off to ServeContent buys Range,
	// Content-Length, content sniffing and If-None-Match -> 304, including
	// "*", comma-separated lists and weak comparison, which is why the 304 is
	// not answered by hand. The cost is one open() even on a 304.
	w.Header().Set("ETag", etagOf(hash))
	// An object is opaque and is never parsed, so its own bytes must not be
	// allowed to choose their Content-Type. Letting ServeContent sniff would
	// let anyone who can PUT serve scripted HTML from this origin, and this
	// service has no authentication of its own. Setting the type also stops
	// ServeContent from sniffing at all.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, "", time.Time{}, f)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	bucket, objectID := r.PathValue("bucket"), r.PathValue("objectID")
	if err := validateNames(bucket, objectID); err != nil {
		h.fail(w, r, err)
		return
	}
	pre, ok := writePreconditions(r)
	if !ok {
		http.Error(w, "malformed precondition header", http.StatusBadRequest)
		return
	}
	if err := h.store.Delete(r.Context(), bucket, objectID, pre); err != nil {
		h.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writePreconditions parses the headers a write evaluates. GET's
// If-None-Match is not read here: http.ServeContent handles it.
func writePreconditions(r *http.Request) (Precondition, bool) {
	var pre Precondition
	if v := joinHeader(r, "If-Match"); v != "" {
		list, ok := parseTagList(v)
		if !ok {
			return pre, false
		}
		pre.IfMatch = list
	}
	if v := joinHeader(r, "If-None-Match"); v != "" {
		list, ok := parseTagList(v)
		if !ok {
			return pre, false
		}
		pre.IfNoneMatch = list
	}
	return pre, true
}

// joinHeader folds repeated header lines into one list value. RFC 9110 §5.3
// makes two If-Match lines equivalent to one comma-separated list, so
// reading only the first would turn a legitimate write into a spurious 412.
func joinHeader(r *http.Request, name string) string {
	return strings.Join(r.Header.Values(name), ", ")
}

// precheck is the advisory half of the precondition evaluation. A missing
// object is not an error here: Put decides 412-vs-201 authoritatively.
func (h *Handler) precheck(r *http.Request, bucket, objectID string, pre Precondition) error {
	if pre.IfMatch == nil && pre.IfNoneMatch == nil {
		return nil
	}
	hash, err := h.store.Head(r.Context(), bucket, objectID)
	switch {
	case errors.Is(err, ErrNotFound):
		return pre.check("", false)
	case err != nil:
		return err
	}
	return pre.check(hash, true)
}

func validateNames(bucket, objectID string) error {
	if err := validateBucket(bucket); err != nil {
		return err
	}
	return validateObjectID(objectID)
}

func etagOf(hash string) string { return `"` + hash + `"` }

// fail writes the status statusFor chose and logs what the operator needs.
func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := statusFor(err)
	// The request's own context is cancelled as soon as the connection goes
	// away, which catches aborts whose error value is some transport type we
	// did not enumerate.
	if r.Context().Err() != nil {
		status = 0
	} else if status == 0 {
		// The error is shaped like a vanished client, but this request's
		// context is still live, so the socket that broke was the database's,
		// not ours. Returning silently here would let net/http finish the
		// response as a 200 with an empty body — a success-shaped answer, and
		// a 200 in /metrics, for work that never happened.
		status = http.StatusServiceUnavailable
	}
	if status == 0 {
		// The client hung up. Nothing committed, the temp is gone, and there
		// is no one left to read a response.
		slog.Debug("client disconnected", "method", r.Method, "path", r.URL.Path)
		return
	}
	if status >= 500 {
		slog.Error("request failed", "method", r.Method, "path", r.URL.Path,
			"status", status, "err", err)
	}
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	http.Error(w, http.StatusText(status), status)
}

// statusFor maps a store error onto an HTTP status. It returns 0 when the
// client is gone and nothing should be written.
//
// ENOSPC gets its own code rather than a generic 500 because a 500 hides the
// cause from whoever is paged.
func statusFor(err error) int {
	var maxBytes *http.MaxBytesError
	var pgErr *pgconn.PgError
	var connErr *pgconn.ConnectError

	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrInvalidName):
		return http.StatusBadRequest
	case errors.Is(err, ErrPrecondition):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrBlobMissing):
		blobMissingTotal.Inc()
		return http.StatusInternalServerError
	case errors.As(err, &maxBytes):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, syscall.ENOSPC):
		return http.StatusInsufficientStorage
	case errors.As(err, &connErr):
		// The pool could not reach the database at all. pgx reports this as a
		// ConnectError, never a PgError, so it must be matched by type or it
		// falls through to a generic 500 with no Retry-After. Checked before
		// the client-gone cases below because a ConnectError is by
		// construction the database's socket, never the client's.
		return http.StatusServiceUnavailable
	case errors.Is(err, context.Canceled),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, io.ErrUnexpectedEOF):
		// The upload was aborted at the socket — a phone losing signal, a
		// cancelled curl. There is no one left to read a response, and it is
		// not a server error.
		return 0
	case errors.As(err, &pgErr):
		switch pgErr.Code {
		// 40001 serialization_failure, 55P03 lock_not_available, 57014
		// query_canceled: retry and it may well work. 08006/08003 are a
		// connection that broke or was already closed; 57P01 admin_shutdown
		// is what a clean `pg_ctl stop` sends every backend and 57P03
		// cannot_connect_now is a server still starting up. All transient,
		// none the client's fault.
		case "40001", "55P03", "57014", "08006", "08003", "57P01", "57P03":
			return http.StatusServiceUnavailable
		default:
			return http.StatusInternalServerError
		}
	default:
		return http.StatusInternalServerError
	}
}
