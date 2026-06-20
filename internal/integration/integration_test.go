package integration_test

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/manifest"
	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
	"github.com/ngaddam369/timberdb/internal/wal"
)

// flushToSSTable freezes a partition's memtable, writes it to an SSTable at path,
// and registers the file in the manifest. Returns the SSTableMeta.
func flushToSSTable(t *testing.T, path string, p *partition.TimePartition, m *manifest.Manifest) sstable.SSTableMeta {
	t.Helper()
	opts := sstable.WriterOptions{
		BlockSizeBytes: sstable.DefaultWriterOptions().BlockSizeBytes,
		PartitionStart: p.Window.Start,
		PartitionEnd:   p.Window.End,
	}
	w, err := sstable.NewWriter(path, opts)
	require.NoError(t, err)

	it := p.SwapMemtable()
	for it.Next() {
		require.NoError(t, w.Add(it.Record()))
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	meta, err := w.Finish()
	require.NoError(t, err)

	require.NoError(t, m.Append(manifest.VersionEdit{
		AddedFiles: []manifest.FileEntry{{
			Path:           path,
			PartitionStart: meta.PartitionStart,
			PartitionEnd:   meta.PartitionEnd,
			MinTimestamp:   meta.MinTimestamp,
			MaxTimestamp:   meta.MaxTimestamp,
			RecordCount:    meta.RecordCount,
		}},
	}))
	return meta
}

// drainReader scans the full range of an SSTable and returns all records.
func drainReader(t *testing.T, r *sstable.Reader) []record.Record {
	t.Helper()
	meta := r.Meta()
	if meta.RecordCount == 0 {
		return nil
	}
	it, err := r.Scan(meta.MinTimestamp, meta.MaxTimestamp+1, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	var out []record.Record
	for it.Next() {
		out = append(out, it.Record())
	}
	require.NoError(t, it.Err())
	return out
}

// sortedPartitions returns r.All() sorted by window start for deterministic iteration.
func sortedPartitions(r *partition.Router) []*partition.TimePartition {
	ps := r.All()
	sort.Slice(ps, func(i, j int) bool {
		return ps[i].Window.Start < ps[j].Window.Start
	})
	return ps
}

// TestWriteFlushScanRoundTrip verifies the full write → flush → scan path for multiple partitions.
// Records are written to both the WAL and the partition memtables, then each partition is
// flushed to an SSTable registered in the manifest. The test confirms all records survive
// the round-trip with correct timestamps and ordering.
func TestWriteFlushScanRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal-0.wal"), wal.SyncAlways)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	m, err := manifest.Open(filepath.Join(dir, "MANIFEST"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	router := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)

	// Write 100 records into each of 3 consecutive hour-windows.
	base := time.Now().Add(time.Hour).Truncate(time.Hour)
	const (
		hours   = 3
		perHour = 100
	)
	var written []record.Record
	for h := range hours {
		for i := range perHour {
			rec := record.Record{
				// Use seconds so no record overflows into the next hour window (max offset = 99s).
				Timestamp: base.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
				SourceID:  fmt.Appendf(nil, "src-%d", i%5),
				Payload:   fmt.Appendf(nil, "h%d-r%d", h, i),
			}
			p, err := router.Route(rec.Timestamp)
			require.NoError(t, err)
			require.NoError(t, p.Append(rec))
			require.NoError(t, w.Append(rec))
			written = append(written, rec)
		}
	}

	// Flush each partition to a separate SSTable.
	partitions := sortedPartitions(router)
	require.Len(t, partitions, hours)

	var readers []*sstable.Reader
	for i, p := range partitions {
		p.Seal()
		path := filepath.Join(dir, fmt.Sprintf("part-%d.sst", i))
		flushToSSTable(t, path, p, m)
		r, err := sstable.NewReader(path, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Close() })
		readers = append(readers, r)
	}

	// Scan all SSTables and verify the total record count and sort order.
	var got []record.Record
	for _, r := range readers {
		got = append(got, drainReader(t, r)...)
	}
	require.Len(t, got, len(written))

	sort.Slice(written, func(i, j int) bool { return written[i].Timestamp < written[j].Timestamp })
	for i, want := range written {
		assert.Equal(t, want.Timestamp, got[i].Timestamp, "record %d timestamp mismatch", i)
	}
}

// TestReopenAfterFlush simulates crash-recovery: write + flush in one session, then reopen
// the manifest in a second session and verify that all records are scannable from the
// SSTables listed in the manifest.
func TestReopenAfterFlush(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")

	const totalRecords = 100

	// ── Session 1: write and flush ────────────────────────────────────────────
	func() {
		w, err := wal.Open(filepath.Join(dir, "wal-0.wal"), wal.SyncAlways)
		require.NoError(t, err)
		defer func() { require.NoError(t, w.Close()) }()

		m, err := manifest.Open(manifestPath)
		require.NoError(t, err)
		defer func() { require.NoError(t, m.Close()) }()

		router := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		for i := range totalRecords {
			rec := record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Minute).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}
			p, err := router.Route(rec.Timestamp)
			require.NoError(t, err)
			require.NoError(t, p.Append(rec))
			require.NoError(t, w.Append(rec))
		}

		for i, p := range sortedPartitions(router) {
			p.Seal()
			path := filepath.Join(dir, fmt.Sprintf("part-%d.sst", i))
			flushToSSTable(t, path, p, m)
		}
	}()

	// ── Session 2: manifest replay → scan SSTables ────────────────────────────
	m2, err := manifest.Open(manifestPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m2.Close()) })

	var liveFiles []manifest.FileEntry
	require.NoError(t, m2.Replay(func(e manifest.VersionEdit) {
		liveFiles = append(liveFiles, e.AddedFiles...)
		for _, del := range e.DeletedFiles {
			for i, lf := range liveFiles {
				if lf.Path == del.Path {
					liveFiles = append(liveFiles[:i], liveFiles[i+1:]...)
					break
				}
			}
		}
	}))
	require.NotEmpty(t, liveFiles, "manifest must have at least one live file after replay")

	var scanned int
	for _, fe := range liveFiles {
		r, err := sstable.NewReader(fe.Path, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Close() })
		scanned += len(drainReader(t, r))
	}
	assert.Equal(t, totalRecords, scanned, "all written records must be scannable after reopen")
}

// TestPartialFlushRecovery verifies that records written to the WAL but not yet flushed
// to an SSTable survive a crash. After reopening the WAL and replaying it, all records
// must be recoverable — without touching any SSTable.
func TestPartialFlushRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal-0.wal")

	const N = 500
	base := time.Now().Add(time.Hour).Truncate(time.Hour)

	// Write N records to WAL only — no SSTable flush.
	func() {
		w, err := wal.Open(walPath, wal.SyncAlways)
		require.NoError(t, err)
		defer func() { require.NoError(t, w.Close()) }()

		for i := range N {
			rec := record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   fmt.Appendf(nil, "p%d", i),
			}
			require.NoError(t, w.Append(rec))
		}
	}()

	// Reopen and replay — all N records must come back.
	w2, err := wal.Open(walPath, wal.SyncNever)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w2.Close()) })

	var got []record.Record
	require.NoError(t, w2.Replay(func(r record.Record) {
		got = append(got, r)
	}))
	require.Len(t, got, N, "all WAL-committed records must survive replay without SSTable flush")

	// Verify timestamps are monotonically increasing (WAL preserves write order).
	for i := 1; i < len(got); i++ {
		assert.Less(t, got[i-1].Timestamp, got[i].Timestamp, "timestamps must be in write order")
	}
}

// TestLateArrivalTolerantIntegration verifies that late-arriving records are silently
// accepted in Tolerant mode, routed to their natural time-window partition, and fully
// recoverable from the WAL. This exercises the WAL + Router integration for out-of-order data.
func TestLateArrivalTolerantIntegration(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal-0.wal")

	router := partition.NewRouter(time.Hour, 5*time.Minute, partition.Tolerant)
	w, err := wal.Open(walPath, wal.SyncAlways)
	require.NoError(t, err)

	now := time.Now()
	onTimeBase := now.Add(time.Hour).Truncate(time.Hour)
	lateBase := now.Add(-10 * time.Minute).UnixNano()

	const (
		onTimeCount = 10
		lateCount   = 5
	)

	// Write on-time records — routed to normal partitions.
	for i := range onTimeCount {
		rec := record.Record{
			Timestamp: onTimeBase.Add(time.Duration(i) * time.Minute).UnixNano(),
			SourceID:  []byte("src"),
			Payload:   []byte("on-time"),
		}
		p, err := router.Route(rec.Timestamp)
		require.NoError(t, err)
		require.NoError(t, p.Append(rec))
		require.NoError(t, w.Append(rec))
	}

	// Write late records — Tolerant mode must accept them without error.
	for i := range lateCount {
		rec := record.Record{
			Timestamp: lateBase + int64(i)*int64(time.Second),
			SourceID:  []byte("src"),
			Payload:   []byte("late"),
		}
		p, err := router.Route(rec.Timestamp)
		require.NoError(t, err, "Tolerant mode must not reject late records")
		require.NoError(t, p.Append(rec))
		require.NoError(t, w.Append(rec))
	}
	require.NoError(t, w.Close())

	// WAL must contain all records (on-time + late).
	w2, err := wal.Open(walPath, wal.SyncNever)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w2.Close()) })

	var walCount int
	require.NoError(t, w2.Replay(func(record.Record) { walCount++ }))
	assert.Equal(t, onTimeCount+lateCount, walCount, "WAL must contain all records including late arrivals")

	// In Tolerant mode, late records route to their natural time-window partition, so
	// router.All() includes both the on-time and late-arrival partitions. The total
	// record count across all partitions must equal onTimeCount + lateCount.
	var totalInMemtable int
	for _, p := range router.All() {
		it := p.Scan(math.MinInt64, math.MaxInt64, nil)
		for it.Next() {
			totalInMemtable++
		}
		require.NoError(t, it.Close())
	}
	assert.Equal(t, onTimeCount+lateCount, totalInMemtable, "all records must be visible in their natural-window partitions")
}
