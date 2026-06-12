// Package metrics exposes Prometheus metrics for the timberdb engine.
// Key metric: timberdb_sstable_skips_total / (timberdb_sstable_reads_total + timberdb_sstable_skips_total)
// is the time-range metadata skip ratio — the equivalent of a bloom filter hit rate.
// Target: >95% for bounded time-range queries.
package metrics

import (
	"net/http"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all instrumentation handles for a single engine instance.
// Backed by an isolated prometheus.Registry so multiple engines in the same
// process (common in tests) do not share or double-count metrics.
type Metrics struct {
	// Counters
	AppendsTotal        prometheus.Counter
	AppendBytesTotal    prometheus.Counter
	LateArrivalsTotal   prometheus.Counter
	WALWritesTotal      prometheus.Counter
	FlushedTotal        prometheus.Counter
	ScansTotal          prometheus.Counter
	ScanRecordsTotal    prometheus.Counter
	SSTReadsTotal       prometheus.Counter
	SSTSkipsTotal       prometheus.Counter
	CompactionsTotal    prometheus.Counter
	FilesExpiredTotal   prometheus.Counter
	BytesReclaimedTotal prometheus.Counter
	BytesFlushedTotal   prometheus.Counter
	BytesCompactedTotal prometheus.Counter

	// Gauges
	ActivePartitions   prometheus.Gauge
	SSTFilesTotal      prometheus.Gauge
	SSTBytesTotal      prometheus.Gauge
	RetentionHorizonTS prometheus.Gauge

	// Histograms
	ScanDuration       prometheus.Histogram
	CompactionDuration prometheus.Histogram

	reg *prometheus.Registry
}

// New creates a Metrics instance with all handles registered against a fresh
// isolated registry. Callers must not register these metrics with the global
// prometheus.DefaultRegisterer.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		AppendsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_appends_total",
			Help: "Total number of records appended.",
		}),
		AppendBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_append_bytes_total",
			Help: "Total bytes written by Append (timestamp + sourceID + payload).",
		}),
		LateArrivalsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_late_arrivals_total",
			Help: "Total records rejected due to arriving outside the late-arrival window (strict mode).",
		}),
		WALWritesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_wal_writes_total",
			Help: "Total records written to the WAL.",
		}),
		FlushedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_memtable_flushes_total",
			Help: "Total number of memtable-to-SSTable flushes completed.",
		}),
		ScansTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_scans_total",
			Help: "Total number of Scan calls.",
		}),
		ScanRecordsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_scan_records_total",
			Help: "Total records yielded across all scans.",
		}),
		SSTReadsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_sstable_reads_total",
			Help: "Total SSTable readers opened for a Scan (range overlapped).",
		}),
		SSTSkipsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_sstable_skips_total",
			Help: "Total SSTable readers skipped by time-range metadata pre-filter.",
		}),
		CompactionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_compactions_total",
			Help: "Total number of compaction runs.",
		}),
		FilesExpiredTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_files_expired_total",
			Help: "Total SSTable files deleted by the retention enforcer.",
		}),
		BytesReclaimedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_bytes_reclaimed_total",
			Help: "Total bytes freed by the retention enforcer.",
		}),
		BytesFlushedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_bytes_flushed_total",
			Help: "Total bytes written to SSTables during memtable flushes.",
		}),
		BytesCompactedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "timberdb_bytes_compacted_total",
			Help: "Total bytes written to SSTables during compaction merges.",
		}),
		ActivePartitions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "timberdb_active_partitions",
			Help: "Current number of partitions tracked by the router.",
		}),
		SSTFilesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "timberdb_sstable_files_total",
			Help: "Current number of live SSTable readers.",
		}),
		SSTBytesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "timberdb_sstable_bytes_total",
			Help: "Current total bytes of live SSTable files.",
		}),
		RetentionHorizonTS: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "timberdb_retention_horizon_timestamp",
			Help: "Unix nanoseconds of the current retention cutoff (0 when retention is disabled).",
		}),
		ScanDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "timberdb_scan_duration_seconds",
			Help:    "Time from Scan() call to iterator Close(), in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		CompactionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "timberdb_compaction_duration_seconds",
			Help:    "Time to complete a single compaction merge, in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		reg: reg,
	}

	reg.MustRegister(
		m.AppendsTotal,
		m.AppendBytesTotal,
		m.LateArrivalsTotal,
		m.WALWritesTotal,
		m.FlushedTotal,
		m.ScansTotal,
		m.ScanRecordsTotal,
		m.SSTReadsTotal,
		m.SSTSkipsTotal,
		m.CompactionsTotal,
		m.FilesExpiredTotal,
		m.BytesReclaimedTotal,
		m.BytesFlushedTotal,
		m.BytesCompactedTotal,
		m.ActivePartitions,
		m.SSTFilesTotal,
		m.SSTBytesTotal,
		m.RetentionHorizonTS,
		m.ScanDuration,
		m.CompactionDuration,
	)

	return m
}

// Handler returns an HTTP handler that serves the /metrics endpoint for this
// engine's isolated registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Gather returns all current metric families from the registry.
func (m *Metrics) Gather() ([]*dto.MetricFamily, error) {
	return m.reg.Gather()
}
