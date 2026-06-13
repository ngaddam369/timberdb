package sstable

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/record"
)

// writeSSTable writes records to a temp SSTable and returns the path and meta.
func writeSSTable(t *testing.T, opts WriterOptions, records []record.Record) (string, SSTableMeta) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.sst")
	w, err := NewWriter(path, opts)
	require.NoError(t, err)
	for _, r := range records {
		require.NoError(t, w.Add(r))
	}
	meta, err := w.Finish()
	require.NoError(t, err)
	return path, meta
}

// collectScan drains an iterator into a slice.
func collectScan(t *testing.T, r *Reader, start, end int64, sourceID []byte) []record.Record {
	t.Helper()
	it, err := r.Scan(start, end, sourceID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, it.Close()) })
	var out []record.Record
	for it.Next() {
		out = append(out, it.Record())
	}
	require.NoError(t, it.Err())
	return out
}

func TestReaderMetaRoundTrip(t *testing.T) {
	recs := []record.Record{
		{Timestamp: 100, SourceID: []byte("s1"), Payload: []byte("a")},
		{Timestamp: 200, SourceID: []byte("s2"), Payload: []byte("b")},
	}
	opts := WriterOptions{BlockSizeBytes: defaultBlockSize, PartitionStart: 50, PartitionEnd: 300}
	path, written := writeSSTable(t, opts, recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got := r.Meta()
	assert.Equal(t, written.MinTimestamp, got.MinTimestamp)
	assert.Equal(t, written.MaxTimestamp, got.MaxTimestamp)
	assert.Equal(t, written.RecordCount, got.RecordCount)
	assert.Equal(t, written.PartitionStart, got.PartitionStart)
	assert.Equal(t, written.PartitionEnd, got.PartitionEnd)
	assert.Equal(t, written.TimeIndexOffset, got.TimeIndexOffset)
	assert.Equal(t, written.TimeIndexSize, got.TimeIndexSize)
}

func TestReaderScanFullRange(t *testing.T) {
	var recs []record.Record
	for i := range 100 {
		recs = append(recs, record.Record{
			Timestamp: int64(i+1) * 1000,
			SourceID:  []byte("src"),
			Payload:   []byte("data"),
		})
	}
	path, _ := writeSSTable(t, DefaultWriterOptions(), recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got := collectScan(t, r, 0, int64(101*1000), nil)
	assert.Equal(t, recs, got)
}

func TestReaderScanSubRange(t *testing.T) {
	var recs []record.Record
	for i := range 20 {
		recs = append(recs, record.Record{
			Timestamp: int64(i+1) * 1000,
			SourceID:  []byte("s"),
			Payload:   []byte("x"),
		})
	}
	path, _ := writeSSTable(t, DefaultWriterOptions(), recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	// Scan [5000, 15000) — expect records at 5000…14000.
	got := collectScan(t, r, 5000, 15000, nil)
	require.Len(t, got, 10)
	assert.Equal(t, int64(5000), got[0].Timestamp)
	assert.Equal(t, int64(14000), got[len(got)-1].Timestamp)
}

func TestReaderScanNoOverlap(t *testing.T) {
	// File covers [16000, 17000); query is [14000, 15000) — no overlap.
	recs := []record.Record{
		{Timestamp: 16000, SourceID: []byte("s"), Payload: []byte("a")},
		{Timestamp: 16500, SourceID: []byte("s"), Payload: []byte("b")},
	}
	opts := WriterOptions{BlockSizeBytes: defaultBlockSize, PartitionStart: 16000, PartitionEnd: 17000}
	path, _ := writeSSTable(t, opts, recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got := collectScan(t, r, 14000, 15000, nil)
	assert.Empty(t, got)
}

func TestReaderScanSourceFilter(t *testing.T) {
	recs := []record.Record{
		{Timestamp: 1000, SourceID: []byte("alpha"), Payload: []byte("1")},
		{Timestamp: 2000, SourceID: []byte("beta"), Payload: []byte("2")},
		{Timestamp: 3000, SourceID: []byte("alpha"), Payload: []byte("3")},
		{Timestamp: 4000, SourceID: []byte("beta"), Payload: []byte("4")},
	}
	path, _ := writeSSTable(t, DefaultWriterOptions(), recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	cases := []struct {
		name     string
		sourceID []byte
		wantLen  int
		wantTS   []int64
	}{
		{"filter alpha", []byte("alpha"), 2, []int64{1000, 3000}},
		{"filter beta", []byte("beta"), 2, []int64{2000, 4000}},
		{"no filter", nil, 4, []int64{1000, 2000, 3000, 4000}},
		{"unknown source", []byte("gamma"), 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := collectScan(t, r, 0, 5000, tc.sourceID)
			assert.Len(t, got, tc.wantLen)
			for i, ts := range tc.wantTS {
				assert.Equal(t, ts, got[i].Timestamp)
			}
		})
	}
}

func TestReaderScanMultiBlock(t *testing.T) {
	// Tiny blocks force many splits; verify complete sorted round-trip.
	opts := WriterOptions{BlockSizeBytes: 64}
	var recs []record.Record
	for i := range 200 {
		recs = append(recs, record.Record{
			Timestamp: int64(i+1) * 1000,
			SourceID:  []byte("src"),
			Payload:   make([]byte, 16),
		})
	}
	path, meta := writeSSTable(t, opts, recs)
	assert.Greater(t, meta.TimeIndexSize/timeIndexEntrySize, uint64(1))

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got := collectScan(t, r, 0, int64(201*1000), nil)
	require.Len(t, got, 200)
	for i, rec := range got {
		assert.Equal(t, int64(i+1)*1000, rec.Timestamp, "record %d out of order", i)
	}
}

func TestReaderScanSourceIndexSkip(t *testing.T) {
	// With IndexSources enabled, querying an absent source returns empty without scanning blocks.
	opts := WriterOptions{BlockSizeBytes: defaultBlockSize, IndexSources: true}
	recs := []record.Record{
		{Timestamp: 1000, SourceID: []byte("alpha"), Payload: []byte("a")},
		{Timestamp: 2000, SourceID: []byte("alpha"), Payload: []byte("b")},
	}
	path, _ := writeSSTable(t, opts, recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got := collectScan(t, r, 0, 3000, []byte("missing"))
	assert.Empty(t, got)
}

func TestReaderCorruptedFooterNoTimeIndex(t *testing.T) {
	// A footer with RecordCount>0 but TimeIndexSize=0 is structurally inconsistent
	// and must be rejected as corrupt rather than causing a nil-slice panic in Scan.
	recs := []record.Record{
		{Timestamp: 1000, SourceID: []byte("s"), Payload: []byte("a")},
	}
	path, _ := writeSSTable(t, DefaultWriterOptions(), recs)

	// Patch TimeIndexSize (footer offset 24, 8 bytes) to zero.
	info, err := os.Stat(path)
	require.NoError(t, err)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, 8), info.Size()-footerSize+24)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = NewReader(path)
	assert.ErrorIs(t, err, ErrInvalidMagic)
}

func TestReaderInvalidMagic(t *testing.T) {
	cases := []struct {
		name    string
		content []byte
	}{
		{"empty file", []byte{}},
		{"too short", make([]byte, footerSize-1)},
		{"wrong magic", make([]byte, footerSize)}, // all-zero footer has wrong magic
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.sst")
			require.NoError(t, os.WriteFile(path, tc.content, 0644))
			_, err := NewReader(path)
			assert.ErrorIs(t, err, ErrInvalidMagic)
		})
	}
}

func TestScanBlockAllocsZero(t *testing.T) {
	// 200 records with tiny blocks forces many block reads; mmap must make them allocation-free.
	opts := WriterOptions{BlockSizeBytes: 64}
	var recs []record.Record
	for i := range 200 {
		recs = append(recs, record.Record{
			Timestamp: int64(i+1) * 1000,
			SourceID:  []byte("src"),
			Payload:   make([]byte, 16),
		})
	}
	path, _ := writeSSTable(t, opts, recs)
	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	allocs := testing.AllocsPerRun(20, func() {
		it, err2 := r.Scan(0, int64(201*1000), nil)
		if err2 != nil {
			t.Fatal(err2)
		}
		for it.Next() {
			_ = it.View()
		}
		_ = it.Close()
	})
	if allocs > 5 {
		t.Errorf("expected ≤ 5 allocs for 200-record scan, got %.0f", allocs)
	}
}

func TestConcurrentScanSameFile(t *testing.T) {
	var recs []record.Record
	for i := range 100 {
		recs = append(recs, record.Record{
			Timestamp: int64(i+1) * 1000,
			SourceID:  []byte("src"),
			Payload:   []byte("payload"),
		})
	}
	path, _ := writeSSTable(t, DefaultWriterOptions(), recs)

	r1, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r1.Close() })

	r2, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r2.Close() })

	results := make([][]record.Record, 2)
	var wg sync.WaitGroup
	for i, r := range []*Reader{r1, r2} {
		wg.Add(1)
		go func(idx int, rd *Reader) {
			defer wg.Done()
			results[idx] = collectScan(t, rd, 0, int64(101*1000), nil)
		}(i, r)
	}
	wg.Wait()

	assert.Len(t, results[0], 100)
	assert.Len(t, results[1], 100)
}

func TestReaderEmptySSTable(t *testing.T) {
	path, _ := writeSSTable(t, DefaultWriterOptions(), nil)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got := collectScan(t, r, 0, 9999, nil)
	assert.Empty(t, got)
}

func TestScanIteratorView(t *testing.T) {
	recs := []record.Record{
		{Timestamp: 100, SourceID: []byte("s1"), Payload: []byte("payload-one")},
		{Timestamp: 200, SourceID: []byte("s2"), Payload: []byte("payload-two")},
		{Timestamp: 300, SourceID: []byte("s3"), Payload: []byte("payload-three")},
		{Timestamp: 400, SourceID: []byte("s4"), Payload: []byte("payload-four")},
		{Timestamp: 500, SourceID: []byte("s5"), Payload: []byte("payload-five")},
	}
	path, _ := writeSSTable(t, DefaultWriterOptions(), recs)
	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	it, err := r.Scan(0, 9999, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, it.Close()) })

	var i int
	for it.Next() {
		view := it.View()
		assert.Equal(t, recs[i].Timestamp, view.Timestamp, "view timestamp mismatch at %d", i)
		assert.Equal(t, recs[i].SourceID, view.SourceID, "view SourceID mismatch at %d", i)
		assert.Equal(t, recs[i].Payload, view.Payload, "view Payload mismatch at %d", i)

		rec := it.Record()
		assert.Equal(t, recs[i].Timestamp, rec.Timestamp, "Record() timestamp mismatch at %d", i)
		assert.Equal(t, recs[i].SourceID, rec.SourceID, "Record() SourceID mismatch at %d", i)
		assert.Equal(t, recs[i].Payload, rec.Payload, "Record() Payload mismatch at %d", i)
		i++
	}
	require.NoError(t, it.Err())
	assert.Equal(t, len(recs), i, "expected all records to be visited")
}
