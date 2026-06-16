// Package retention_test verifies TTL expiry correctness.
package retention_test

import (
	"path/filepath"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/metrics"
	"github.com/ngaddam369/timberdb/internal/record"
)

// gatherCounter extracts a counter value by name from an isolated metrics registry.
func gatherCounter(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()
	families, err := m.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name && f.GetType() == dto.MetricType_COUNTER {
			if mets := f.GetMetric(); len(mets) > 0 {
				return mets[0].GetCounter().GetValue()
			}
		}
	}
	return 0
}

// baseOpts returns options shared by all retention tests:
// - MemtableSizeBytes=1 so each append immediately flushes to an SSTable
// - LateArrivalWindow=24h so records written in the past are accepted
// - Fast retention check interval for tests
// - No compaction interference
func baseOpts(retentionDuration time.Duration) engine.Options {
	opts := engine.DefaultOptions()
	opts.MemtableSizeBytes = 1
	opts.LateArrivalWindow = 24 * time.Hour
	opts.RetentionDuration = retentionDuration
	opts.RetentionCheckInterval = 10 * time.Millisecond
	opts.CompactionCheckInterval = time.Hour
	opts.MaxFilesPerPartition = 100
	return opts
}

// waitExpired waits for the engine to both flush a record and then expire it via
// retention, verified through metrics rather than file system state (the file may be
// created and deleted between two Glob calls due to the fast retention interval).
func waitExpired(t *testing.T, m *metrics.Metrics) {
	t.Helper()
	require.Eventually(t, func() bool {
		return gatherCounter(t, m, "timberdb_memtable_flushes_total") >= 1
	}, 5*time.Second, 10*time.Millisecond, "at least one flush must complete before retention check")

	require.Eventually(t, func() bool {
		return gatherCounter(t, m, "timberdb_files_expired_total") >= 1
	}, 5*time.Second, 10*time.Millisecond, "retention must expire at least one SST file")
}

// TestRetentionPrecision verifies the exact TTL boundary semantics at the SSTable level.
//
// FIFO.PickExpired boundary: meta.MaxTimestamp < horizon → expired.
//   - MaxTimestamp == horizon - 1 → EXPIRED
//   - MaxTimestamp == horizon     → NOT expired
func TestRetentionPrecision(t *testing.T) {
	t.Run("expired_file_deleted", func(t *testing.T) {
		dir := t.TempDir()
		// RetentionDuration=1h means horizon = now-1h.
		// Records at now-2h have MaxTimestamp < horizon → expired.
		opts := baseOpts(time.Hour)
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		defer func() { _ = e.Close() }()

		m := e.Metrics()
		past := time.Now().Add(-2 * time.Hour)
		for i := range 5 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: past.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		waitExpired(t, m)

		ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
		require.NoError(t, err)
		assert.Empty(t, ssts, "all SST files must be deleted after retention sweep")
	})

	t.Run("near_horizon_not_deleted", func(t *testing.T) {
		dir := t.TempDir()
		// RetentionDuration=1h means horizon = now-1h.
		// Records at now-30min have MaxTimestamp > horizon → NOT expired.
		opts := baseOpts(time.Hour)
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		defer func() { _ = e.Close() }()

		recent := time.Now().Add(-30 * time.Minute)
		for i := range 5 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: recent.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		// Wait for flush to complete so we have SSTs to check.
		require.Eventually(t, func() bool {
			ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
			return len(ssts) >= 1
		}, 5*time.Second, 10*time.Millisecond, "SST must be flushed before checking retention")

		// Give the retention sweeper several extra cycles to (incorrectly) delete files.
		time.Sleep(100 * time.Millisecond)

		ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
		require.NoError(t, err)
		assert.NotEmpty(t, ssts, "SSTs with MaxTimestamp > retention horizon must not be deleted")

		// Records must still be scannable.
		it, err := e.Scan(recent, recent.Add(time.Minute), nil)
		require.NoError(t, err)
		var count int
		for it.Next() {
			count++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, 5, count, "5 non-expired records must be scannable")
	})

	t.Run("scan_expired_returns_empty_not_error", func(t *testing.T) {
		dir := t.TempDir()
		opts := baseOpts(time.Hour)
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		defer func() { _ = e.Close() }()

		past := time.Now().Add(-2 * time.Hour)
		for i := range 3 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: past.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		// Wait for flush + retention to remove all SSTs. Use an eventually-based scan
		// because records may still be in the memtable until a final flush cycle runs.
		require.Eventually(t, func() bool {
			ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
			if len(ssts) > 0 {
				return false // retention hasn't finished yet
			}
			it, scanErr := e.Scan(past, past.Add(time.Minute), nil)
			if scanErr != nil {
				return false
			}
			hasRecords := it.Next()
			_ = it.Close()
			return !hasRecords // true once both SSTs are gone and memtable is empty
		}, 10*time.Second, 10*time.Millisecond,
			"scan of expired range must return empty after all SSTs deleted and memtable flushed")

		// Final assertions on a clean scan.
		it, err := e.Scan(past, past.Add(time.Minute), nil)
		require.NoError(t, err, "Scan on expired range must not return an error")
		assert.False(t, it.Next(), "iterator over expired range must be empty")
		require.NoError(t, it.Err(), "iterator.Err() must be nil for empty expired range")
		require.NoError(t, it.Close())
	})

	t.Run("bytes_reclaimed_increments", func(t *testing.T) {
		dir := t.TempDir()
		opts := baseOpts(time.Hour)
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		defer func() { _ = e.Close() }()

		past := time.Now().Add(-2 * time.Hour)
		for i := range 5 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: past.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   make([]byte, 128), // non-trivial payload so bytes_reclaimed > 0
			}))
		}

		m := e.Metrics()
		require.Eventually(t, func() bool {
			return gatherCounter(t, m, "timberdb_files_expired_total") >= 1
		}, 5*time.Second, 10*time.Millisecond, "retention sweep did not run within timeout")

		assert.Greater(t, gatherCounter(t, m, "timberdb_bytes_reclaimed_total"), float64(0),
			"timberdb_bytes_reclaimed_total must be > 0 after expiring SST files")
	})
}
