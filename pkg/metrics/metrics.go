package metrics

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
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "objectstore_requests_total",
		Help: "Object API requests by method and response code.",
	}, []string{"method", "code"})

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "objectstore_request_duration_seconds",
		Help:    "Object API request latency by method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	PutBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_put_bytes_total",
		Help: "Bytes accepted by PUT, including deduplicated writes.",
	})

	DedupHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_dedup_hits_total",
		Help: "PUTs whose content was already stored in the bucket.",
	})

	// BlobMissingTotal should be identically zero forever: it is the one
	// failure the whole locking design exists to prevent. Page on > 0.
	BlobMissingTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_blob_missing_total",
		Help: "Reads where a row resolved but its blob file was gone.",
	})

	SweepRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "objectstore_sweep_runs_total",
		Help: "Sweeper runs by result.",
	}, []string{"result"})

	SweepDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "objectstore_sweep_duration_seconds",
		Help:    "Wall-clock time of one sweep.",
		Buckets: prometheus.DefBuckets,
	})

	// Alert on time() - this > 3 * sweep interval: the sweeper is wedged and
	// disk is leaking.
	SweepLastSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_sweep_last_success_timestamp_seconds",
		Help: "Unix time of the last successful sweep.",
	})

	SweepDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_sweep_deleted_total",
		Help: "Blobs reclaimed by the sweeper.",
	})

	SweepCandidates = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_sweep_candidates",
		Help: "Unreferenced blobs seen by the last sweep.",
	})

	// The one to watch on a disk-pressure alert: deleted data you are
	// deliberately still paying for. -trash-retention is the dial.
	TrashFiles = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_trash_files",
		Help: "Files currently held in trash.",
	})

	TrashBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "objectstore_trash_bytes",
		Help: "Bytes currently held in trash.",
	})

	// Nonzero means -max-trash-bytes is dropping deleted data earlier than
	// -trash-retention promised, so the recovery window is shorter than it
	// looks. Worth alerting on: the cap is protecting the disk by shrinking
	// the safety net.
	TrashEvictedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_trash_evicted_total",
		Help: "Trash entries removed by the size cap before their retention expired.",
	})

	// Not "startup": the walk runs once at boot and then periodically, since
	// a PUT that rolls back after renaming its blob into place leaks a file
	// the reclaim anti-join can never see.
	OrphansReclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objectstore_orphans_reclaimed_total",
		Help: "Blobs with no blobs row moved to trash by the orphan walk.",
	})
)
