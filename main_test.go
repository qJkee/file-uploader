package main

import (
	"flag"
	"testing"
	"time"
)

func TestEnvFallback(t *testing.T) {
	newFlags := func() (*flag.FlagSet, *string, *time.Duration, *int64) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		addr := fs.String("addr", ":8080", "")
		interval := fs.Duration("sweep-interval", time.Minute, "")
		size := fs.Int64("max-object-size", 32<<20, "")
		return fs, addr, interval, size
	}

	t.Run("an unset flag takes the environment value", func(t *testing.T) {
		t.Setenv("OBJECTSTORE_ADDR", ":9999")
		t.Setenv("OBJECTSTORE_MAX_OBJECT_SIZE", "1024")
		fs, addr, _, size := newFlags()
		if err := fs.Parse(nil); err != nil {
			t.Fatal(err)
		}

		if err := envFallback(fs, "OBJECTSTORE_"); err != nil {
			t.Fatalf("envFallback: %v", err)
		}
		if *addr != ":9999" {
			t.Errorf("addr = %q, want %q", *addr, ":9999")
		}
		if *size != 1024 {
			t.Errorf("max-object-size = %d, want 1024", *size)
		}
	})

	t.Run("an explicit flag wins over the environment", func(t *testing.T) {
		t.Setenv("OBJECTSTORE_ADDR", ":9999")
		fs, addr, _, _ := newFlags()
		if err := fs.Parse([]string{"-addr", ":7777"}); err != nil {
			t.Fatal(err)
		}

		if err := envFallback(fs, "OBJECTSTORE_"); err != nil {
			t.Fatalf("envFallback: %v", err)
		}
		if *addr != ":7777" {
			t.Errorf("addr = %q, want the command line's %q", *addr, ":7777")
		}
	})

	t.Run("no environment variable keeps the default", func(t *testing.T) {
		fs, _, interval, _ := newFlags()
		if err := fs.Parse(nil); err != nil {
			t.Fatal(err)
		}

		if err := envFallback(fs, "OBJECTSTORE_"); err != nil {
			t.Fatalf("envFallback: %v", err)
		}
		if *interval != time.Minute {
			t.Errorf("sweep-interval = %v, want the default 1m", *interval)
		}
	})

	t.Run("an unparseable environment value is an error", func(t *testing.T) {
		t.Setenv("OBJECTSTORE_SWEEP_INTERVAL", "not-a-duration")
		fs, _, _, _ := newFlags()
		if err := fs.Parse(nil); err != nil {
			t.Fatal(err)
		}

		if err := envFallback(fs, "OBJECTSTORE_"); err == nil {
			t.Error("envFallback = nil, want an error naming the variable")
		}
	})
}
