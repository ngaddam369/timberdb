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

func TestEngineZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	const n = 500
	payload := make([]byte, 256) // compressible zero-byte payload

	// Write with compression enabled, then close to flush memtable → SSTable.
	{
		opts := engine.DefaultOptions()
		opts.CompressionType = sstable.CompressionZstd
		opts.MemtableSizeBytes = 4 << 10 // 4 KiB — force multiple flushes
		opts.MetricsAddr = ""
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		for i := range n {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   payload,
			}))
		}
		require.NoError(t, e.Close())
	}

	// Reopen (no compression option needed — reader auto-detects v1) and scan.
	{
		opts := engine.DefaultOptions()
		opts.MetricsAddr = ""
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		t.Cleanup(func() { e.Close() })

		got := drainEngine(t, e, base, base.Add(n*time.Millisecond+time.Second))
		require.Len(t, got, n, "all records must survive close and reopen")
		for i := 1; i < len(got); i++ {
			assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp)
		}
	}
}

func TestEngineCompactionRewritesWithCompression(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	opts := engine.DefaultOptions()
	opts.CompressionType = sstable.CompressionZstd
	opts.MemtableSizeBytes = 1    // flush every record
	opts.MaxFilesPerPartition = 2 // compact aggressively
	opts.CompactionCheckInterval = 10 * time.Millisecond
	opts.RetentionCheckInterval = time.Hour
	opts.MetricsAddr = ""

	e := openEngine(t, dir, opts)

	const n = 20
	for i := range n {
		require.NoError(t, e.Append(record.Record{
			Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
			SourceID:  []byte("src"),
			Payload:   make([]byte, 128),
		}))
	}

	// Wait for all flushes and at least one compaction round to complete.
	require.Eventually(t, func() bool {
		it, err := e.Scan(base, base.Add(n*time.Second+time.Second), nil)
		if err != nil {
			return false
		}
		var count int
		for it.Next() {
			count++
		}
		_ = it.Close()
		return count == n
	}, 10*time.Second, 20*time.Millisecond, "records not all flushed within timeout")

	got := drainEngine(t, e, base, base.Add(n*time.Second+time.Second))
	require.Len(t, got, n, "all records must be visible after compaction")
	for i := 1; i < len(got); i++ {
		assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp)
	}
}
