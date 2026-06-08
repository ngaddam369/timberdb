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

		time.Sleep(200 * time.Millisecond)

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

		time.Sleep(300 * time.Millisecond)

		ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
		require.NoError(t, err)
		assert.Empty(t, ssts)
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

		time.Sleep(300 * time.Millisecond)

		// After retention, scan should return empty without error.
		scanStart := base.Truncate(time.Hour)
		got := drainEngine(t, e, scanStart, scanStart.Add(time.Hour))
		assert.Empty(t, got)
	})
}
