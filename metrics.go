package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Every series here is a counter, histogram or an accumulated gauge — never a
// COUNT(*) — so a scrape stays O(1) regardless of how large the store grows.
//
// Labels stay on method and code. Never label by bucket or object ID: that is
// unbounded cardinality and the standard way to take down a Prometheus
// server.
var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "objectstore_requests_total",
		Help: "Object API requests by method and response code.",
	}, []string{"method", "code"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "objectstore_request_duration_seconds",
		Help:    "Object API request latency by method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	putBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_put_bytes_total",
		Help: "Bytes accepted by PUT, including deduplicated writes.",
	})

	dedupHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_dedup_hits_total",
		Help: "PUTs whose content was already stored in the bucket.",
	})

	// blobMissingTotal should be identically zero forever: it is the one
	// failure the whole locking design exists to prevent. Page on > 0.
	blobMissingTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_blob_missing_total",
		Help: "Reads where a row resolved but its blob file was gone.",
	})

	sweepRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "objectstore_sweep_runs_total",
		Help: "Sweeper runs by result.",
	}, []string{"result"})

	sweepDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "objectstore_sweep_duration_seconds",
		Help:    "Wall-clock time of one sweep.",
		Buckets: prometheus.DefBuckets,
	})

	// Alert on time() - this > 3 * sweep interval: the sweeper is wedged and
	// disk is leaking.
	sweepLastSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_sweep_last_success_timestamp_seconds",
		Help: "Unix time of the last successful sweep.",
	})

	sweepDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_sweep_deleted_total",
		Help: "Blobs reclaimed by the sweeper.",
	})

	sweepCandidates = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_sweep_candidates",
		Help: "Unreferenced blobs seen by the last sweep.",
	})

	// The one to watch on a disk-pressure alert: deleted data you are
	// deliberately still paying for. -trash-retention is the dial.
	trashFiles = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_trash_files",
		Help: "Files currently held in trash.",
	})

	trashBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_trash_bytes",
		Help: "Bytes currently held in trash.",
	})

	// Nonzero means -max-trash-bytes is dropping deleted data earlier than
	// -trash-retention promised, so the recovery window is shorter than it
	// looks. Worth alerting on: the cap is protecting the disk by shrinking
	// the safety net.
	trashEvictedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_trash_evicted_total",
		Help: "Trash entries removed by the size cap before their retention expired.",
	})

	// Not "startup": the walk runs once at boot and then periodically, since
	// a PUT that rolls back after renaming its blob into place leaks a file
	// the reclaim anti-join can never see.
	orphansReclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_orphans_reclaimed_total",
		Help: "Blobs with no blobs row moved to trash by the orphan walk.",
	})
)

// observeSweep records what the sweep actually did, whether or not it also
// returned an error. The three passes run independently so one unmovable
// file cannot stop the others — discarding their counts here would undo
// that, and sweepDeletedTotal is a counter, so a skipped increment is lost
// for good rather than caught up on the next pass.
func observeSweep(res SweepResult, err error, seconds float64) {
	sweepDuration.Observe(seconds)
	sweepCandidates.Set(float64(res.Candidates))
	sweepDeletedTotal.Add(float64(res.Reclaimed))
	// res.Orphans is not added here: SweepOrphans counts its own reclaims,
	// because the startup walk does not come through a sweep at all.
	trashEvictedTotal.Add(float64(res.TrashEvicted))
	trashFiles.Set(float64(res.TrashFiles))
	trashBytes.Set(float64(res.TrashBytes))

	if err != nil {
		sweepRunsTotal.WithLabelValues("error").Inc()
		return
	}
	sweepRunsTotal.WithLabelValues("success").Inc()
	sweepLastSuccess.SetToCurrentTime()
}
