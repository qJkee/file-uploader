package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("objectstore", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "listen address")
	data := fs.String("data", "", "data directory (required)")
	databaseURL := fs.String("database-url", "", "Postgres connection URL for the metadata database (required)")
	maxObjectSize := fs.Int64("max-object-size", 32<<20, "largest accepted object, in bytes")
	sweepInterval := fs.Duration("sweep-interval", time.Minute, "how often the sweeper runs")
	sweepBudget := fs.Duration("sweep-budget", 10*time.Second, "wall-clock cap on one sweep's reclaim loop")
	tmpMaxAge := fs.Duration("tmp-max-age", time.Hour, "age at which an abandoned upload is removed; must exceed the longest plausible upload")
	trashRetention := fs.Duration("trash-retention", 24*time.Hour, "how long reclaimed bytes survive in trash; 0 unlinks immediately")
	maxTrashBytes := fs.Int64("max-trash-bytes", 2<<30, "evict oldest trash past this many bytes; 0 disables the cap")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := envFallback(fs, "OBJECTSTORE_"); err != nil {
		return err
	}
	if *data == "" {
		return errors.New("-data is required")
	}
	if strings.TrimSpace(*databaseURL) == "" {
		return errors.New("-database-url is required")
	}

	// Captured before the server accepts anything, so the orphan walk can
	// tell a crash leftover from a blob an in-flight PUT is writing now.
	startedAt := time.Now()

	// Installed before Open so a Ctrl-C still works while we are waiting on
	// an unreachable database.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := Open(ctx, Config{
		DataDir:        *data,
		DatabaseURL:    *databaseURL,
		MaxObjectSize:  *maxObjectSize,
		SweepInterval:  *sweepInterval,
		SweepBudget:    *sweepBudget,
		TmpMaxAge:      *tmpMaxAge,
		TrashRetention: *trashRetention,
		MaxTrashBytes:  *maxTrashBytes,
	})
	if err != nil {
		return err
	}
	defer store.Close()

	go func() {
		n, err := store.SweepOrphans(ctx, startedAt)
		if err != nil {
			slog.Error("startup orphan walk failed", "err", err)
			return
		}
		if n > 0 {
			slog.Info("startup orphan walk reclaimed blobs", "count", n)
		}
	}()

	sweeperDone := make(chan struct{})
	go func() {
		defer close(sweeperDone)
		runSweeper(ctx, store)
	}()

	srv := &http.Server{
		Addr:    *addr,
		Handler: NewHandler(store).Mux(),
		// Slowloris protection. There is deliberately no WriteTimeout and no
		// ReadTimeout: either would abort a legitimate slow transfer of a
		// max-size object mid-flight. MaxBytesReader bounds uploads by size
		// instead, which is the right axis.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", *addr, "data", *data)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	stop() // restore default signal handling: a second SIGINT kills us
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	<-sweeperDone
	return err
}

func runSweeper(ctx context.Context, store *Store) {
	ticker := time.NewTicker(store.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := store.Sweep(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("sweep failed", "err", err)
				continue
			}
			if res.Reclaimed > 0 || res.TrashDeleted > 0 || res.TmpDeleted > 0 || res.Orphans > 0 {
				slog.Info("sweep",
					"candidates", res.Candidates, "reclaimed", res.Reclaimed,
					"trash_deleted", res.TrashDeleted, "tmp_deleted", res.TmpDeleted,
					"trash_files", res.TrashFiles, "trash_bytes", res.TrashBytes,
					"trash_evicted", res.TrashEvicted, "orphans", res.Orphans)
			}
		}
	}
}

// envFallback applies prefix+NAME for every flag not given on the command
// line, so containers can configure the service without a config file and
// without a config dependency. Flags stay the single source of truth.
func envFallback(fs *flag.FlagSet, prefix string) error {
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	var err error
	fs.VisitAll(func(f *flag.Flag) {
		if err != nil || explicit[f.Name] {
			return
		}
		key := prefix + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		value, ok := os.LookupEnv(key)
		if !ok {
			return
		}
		if setErr := fs.Set(f.Name, value); setErr != nil {
			err = fmt.Errorf("%s=%q: %w", key, value, setErr)
		}
	})
	return err
}
