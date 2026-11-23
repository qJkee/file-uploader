package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"syscall"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
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
	mux.HandleFunc("PUT /objects/{bucket}/{objectID}", h.put)
	mux.HandleFunc("GET /objects/{bucket}/{objectID}", h.get)
	mux.HandleFunc("DELETE /objects/{bucket}/{objectID}", h.delete)
	return mux
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
	if v := r.Header.Get("If-Match"); v != "" {
		list, ok := parseTagList(v)
		if !ok {
			return pre, false
		}
		pre.IfMatch = list
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		list, ok := parseTagList(v)
		if !ok {
			return pre, false
		}
		pre.IfNoneMatch = list
	}
	return pre, true
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
// cause from whoever is paged; SQLITE_BUSY past busy_timeout is transient and
// deserves a retryable 503, not a 500.
func statusFor(err error) int {
	var maxBytes *http.MaxBytesError
	var sqliteErr *sqlite.Error

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
		return http.StatusInternalServerError
	case errors.As(err, &maxBytes):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, syscall.ENOSPC):
		return http.StatusInsufficientStorage
	case errors.Is(err, context.Canceled):
		return 0
	case errors.As(err, &sqliteErr) &&
		(sqliteErr.Code() == sqlite3.SQLITE_BUSY || sqliteErr.Code() == sqlite3.SQLITE_LOCKED):
		// shortcut: this branch has no test, because the driver's error value
		// is not constructible from outside its package. Upgrade with a real
		// lock-contention test if 503s ever show up in production.
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
