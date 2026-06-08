package integration_test

import (
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/metrics"
	"github.com/ngaddam369/timberdb/internal/record"
)

// gatherCounter extracts the current value of a counter or gauge by metric name
// from the engine's metrics registry.
func gatherCounter(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()
	families, err := m.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			mets := f.GetMetric()
			if len(mets) == 0 {
				return 0
			}
			switch f.GetType() {
			case dto.MetricType_COUNTER:
				return mets[0].GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				return mets[0].GetGauge().GetValue()
			}
		}
	}
	return 0
}

func TestMetricsIntegration(t *testing.T) {
	t.Run("counters_increment", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1 // flush on every append

		e := openEngine(t, dir, opts)
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		const n = 5
		for i := range n {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		// Give the background flusher time to complete.
		time.Sleep(200 * time.Millisecond)

		m := e.Metrics()
		assert.Equal(t, float64(n), gatherCounter(t, m, "timberdb_appends_total"))
		assert.Equal(t, float64(n), gatherCounter(t, m, "timberdb_wal_writes_total"))
		assert.GreaterOrEqual(t, gatherCounter(t, m, "timberdb_memtable_flushes_total"), float64(1))
		assert.Greater(t, gatherCounter(t, m, "timberdb_append_bytes_total"), float64(0))
	})

	t.Run("scan_reads_and_skips", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1

		e := openEngine(t, dir, opts)

		// Write records at the start of a future hour.
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		for i := range 3 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
			time.Sleep(100 * time.Millisecond) // wait for each flush
		}

		// Scan the last minute of the same partition window. The SSTable files have
		// MaxTimestamp ≈ base+2s, which is before the scan start (base+59min), so
		// the engine's meta pre-filter should skip them.
		scanStart := base.Add(59 * time.Minute)
		it, err := e.Scan(scanStart, scanStart.Add(time.Minute), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, it.Close()) })
		for it.Next() {
		}

		m := e.Metrics()
		assert.Greater(t, gatherCounter(t, m, "timberdb_sstable_skips_total"), float64(0))
		assert.Equal(t, float64(0), gatherCounter(t, m, "timberdb_sstable_reads_total"))
	})

	t.Run("compaction_counter", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1
		opts.MaxFilesPerPartition = 2
		opts.CompactionCheckInterval = 20 * time.Millisecond

		e := openEngine(t, dir, opts)
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		// Append 3 records with a flush gap between each so the flusher produces
		// three distinct SSTables. With MaxFilesPerPartition=2, the third SSTable
		// triggers compaction.
		for i := range 3 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
			time.Sleep(100 * time.Millisecond) // let the flusher process this record
		}
		time.Sleep(200 * time.Millisecond) // let the compactor run

		m := e.Metrics()
		assert.GreaterOrEqual(t, gatherCounter(t, m, "timberdb_compactions_total"), float64(1))
	})

	t.Run("retention_reclaimed", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1
		opts.MaxFilesPerPartition = 100
		opts.CompactionCheckInterval = time.Hour
		opts.LateArrivalWindow = 24 * time.Hour
		opts.RetentionDuration = time.Nanosecond
		opts.RetentionCheckInterval = 20 * time.Millisecond

		e := openEngine(t, dir, opts)

		now := time.Now()
		for i := range 3 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: now.Add(-time.Minute).Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}
		time.Sleep(300 * time.Millisecond)

		m := e.Metrics()
		assert.GreaterOrEqual(t, gatherCounter(t, m, "timberdb_files_expired_total"), float64(1))
		assert.Greater(t, gatherCounter(t, m, "timberdb_bytes_reclaimed_total"), float64(0))
	})
}
