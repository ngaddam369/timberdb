package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
)

func TestAggregateIntegration(t *testing.T) {
	t.Run("columnar_sstable", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		const n = 120

		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1 // flush every record → v2 SSTables on disk
		opts.ColumnOriented = true
		opts.CompressionType = sstable.CompressionNone
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour

		func() {
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			for i := range n {
				require.NoError(t, e.Append(record.Record{
					Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   make([]byte, 128),
				}))
			}
		}()

		// Reopen — records are now in columnar (v2) SSTables.
		e := openEngine(t, dir, opts)
		buckets, err := e.Aggregate(base, base.Add(2*time.Minute), engine.AggregateOpts{
			BucketWidth: time.Minute,
			Fn:          engine.AggCount,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 2)
		assert.Equal(t, int64(60), buckets[0].Count)
		assert.Equal(t, int64(60), buckets[1].Count)
	})

	t.Run("row_sstable_fallback", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		const n = 120

		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1
		opts.ColumnOriented = false // row-oriented (v0)
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour

		func() {
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			for i := range n {
				require.NoError(t, e.Append(record.Record{
					Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   make([]byte, 128),
				}))
			}
		}()

		e := openEngine(t, dir, opts)
		buckets, err := e.Aggregate(base, base.Add(2*time.Minute), engine.AggregateOpts{
			BucketWidth: time.Minute,
			Fn:          engine.AggCount,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 2)
		assert.Equal(t, int64(60), buckets[0].Count)
		assert.Equal(t, int64(60), buckets[1].Count)
	})

	t.Run("mixed_memtable_and_sstable", func(t *testing.T) {
		// 60 records flushed to SST, 60 still in memtable — Aggregate must see both.
		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		opts := engine.DefaultOptions()
		opts.ColumnOriented = true
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour

		e := openEngine(t, dir, opts)

		// Write first 60 records and drain to SSTable.
		for i := range 60 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}
		require.Eventually(t, func() bool {
			it, err := e.Scan(base, base.Add(time.Minute), nil)
			if err != nil {
				return false
			}
			var cnt int
			for it.Next() {
				cnt++
			}
			_ = it.Close()
			return cnt == 60
		}, 5*time.Second, 10*time.Millisecond)

		// Write the remaining 60 records into the memtable (no flush triggered).
		opts.MemtableSizeBytes = 64 << 20 // large memtable, no auto-flush
		for i := 60; i < 120; i++ {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		buckets, err := e.Aggregate(base, base.Add(2*time.Minute), engine.AggregateOpts{
			BucketWidth: time.Minute,
			Fn:          engine.AggCount,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 2)
		assert.Equal(t, int64(60), buckets[0].Count)
		assert.Equal(t, int64(60), buckets[1].Count)
	})
}
