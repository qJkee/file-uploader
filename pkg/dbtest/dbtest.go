// Package dbtest gives every test package a private Postgres schema.
//
// It deliberately does not import pkg/store: that would make it unusable
// from store's own in-package tests, which need access to unexported
// internals and so cannot live in an external test package.
package dbtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// DatabaseURL is the live Postgres every test needs. It fails rather than
// skips: a silently skipped concurrency suite is how this port ships broken.
func DatabaseURL(tb testing.TB) string {
	tb.Helper()
	u := os.Getenv("OBJECTSTORE_TEST_DATABASE_URL")
	if u == "" {
		tb.Fatal("OBJECTSTORE_TEST_DATABASE_URL is unset; these tests need a live Postgres, e.g. " +
			"OBJECTSTORE_TEST_DATABASE_URL=postgres://objectstore:objectstore@localhost:15432/objectstore")
	}
	return u
}

// NewSchemaURL creates a private schema and returns a URL pointing every
// connection's search_path at it. pgx forwards unrecognised query parameters
// as startup RuntimeParams, so this covers every connection a pool opens and
// not merely the first.
func NewSchemaURL(tb testing.TB) string {
	tb.Helper()
	base := DatabaseURL(tb)
	name := fmt.Sprintf("test_%016x", rand.Uint64())
	quoted := pgx.Identifier{name}.Sanitize()

	ExecOnDatabase(tb, base, "CREATE SCHEMA "+quoted)
	tb.Cleanup(func() { ExecOnDatabase(tb, base, "DROP SCHEMA "+quoted+" CASCADE") })

	u, err := url.Parse(base)
	if err != nil {
		tb.Fatalf("parse %s: %v", base, err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// ExecOnDatabase runs one statement on a connection of its own, outside any
// Store's pool.
func ExecOnDatabase(tb testing.TB, databaseURL, sql string) {
	tb.Helper()
	ctx := context.WithoutCancel(tb.Context()) // cleanup runs after the test context is cancelled
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		tb.Fatalf("exec %q: %v", sql, err)
	}
}
