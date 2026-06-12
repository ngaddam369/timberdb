package integration_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
)

func TestCompactionIntegration(t *testing.T) {
	t.Run("merge_triggered_and_scan", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1
		opts.MaxFilesPerPartition = 2
		opts.CompactionCheckInterval = 20 * time.Millisecond

		e := openEngine(t, dir, opts)
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		const n = 10
		for i := range n {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		// Poll until all records are visible (background flushes completed).
		require.Eventually(t, func() bool {
			it, err := e.Scan(base, base.Add(time.Hour), nil)
			if err != nil {
				return false
			}
			var count int
			for it.Next() {
				count++
			}
			_ = it.Close()
			return count == n
		}, 5*time.Second, 10*time.Millisecond, "records not all flushed within timeout")

		got := drainEngine(t, e, base, base.Add(time.Hour))
		require.Len(t, got, n)
		for i := 1; i < len(got); i++ {
			assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp)
		}
	})

	t.Run("retention_deletes_expired", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1
		opts.MaxFilesPerPartition = 100 // no compaction
		opts.CompactionCheckInterval = time.Hour
		opts.LateArrivalWindow = 24 * time.Hour // accept old records
		opts.RetentionDuration = time.Nanosecond
		opts.RetentionCheckInterval = 20 * time.Millisecond

		e := openEngine(t, dir, opts)

		// Records 1 minute in the past — well within the large late-arrival window,
		// and immediately expired under RetentionDuration = 1ns.
		now := time.Now()
		for i := range 5 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: now.Add(-time.Minute).Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		require.Eventually(t, func() bool {
			ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
			return len(ssts) >= 1
		}, 5*time.Second, 10*time.Millisecond, "flush must produce at least one SST before retention check")

		require.Eventually(t, func() bool {
			ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
			return len(ssts) == 0
		}, 5*time.Second, 10*time.Millisecond, "SSTs not deleted by retention within timeout")

		ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
		require.NoError(t, err)
		assert.Empty(t, ssts)
	})

	t.Run("reopen_after_compaction", func(t *testing.T) {
		dir := t.TempDir()
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1
		opts.MaxFilesPerPartition = 2
		opts.CompactionCheckInterval = 20 * time.Millisecond

		e := openEngine(t, dir, opts)
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		const n = 10
		for i := range n {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		require.Eventually(t, func() bool {
			it, err := e.Scan(base, base.Add(time.Hour), nil)
			if err != nil {
				return false
			}
			var count int
			for it.Next() {
				count++
			}
			_ = it.Close()
			return count == n
		}, 5*time.Second, 10*time.Millisecond, "records not all flushed within timeout")

		require.NoError(t, e.Close())

		// Reopen and verify manifest is consistent: all records are still scannable.
		e2, err := engine.Open(dir, opts)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, e2.Close()) })

		got := drainEngine(t, e2, base, base.Add(time.Hour))
		require.Len(t, got, n)
		for i := 1; i < len(got); i++ {
			assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp)
		}
	})

	t.Run("scan_expired_range_empty", func(t *testing.T) {
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
		base := now.Add(-time.Minute)
		for i := range 5 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		require.Eventually(t, func() bool {
			ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
			return len(ssts) >= 1
		}, 5*time.Second, 10*time.Millisecond, "flush must produce at least one SST before retention check")

		require.Eventually(t, func() bool {
			ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
			return len(ssts) == 0
		}, 5*time.Second, 10*time.Millisecond, "SSTs not deleted by retention within timeout")

		// After retention, scan should return empty without error.
		scanStart := base.Truncate(time.Hour)
		got := drainEngine(t, e, scanStart, scanStart.Add(time.Hour))
		assert.Empty(t, got)
	})
}
