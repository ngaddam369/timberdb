package sstable

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/record"
)

func collectAllRecords(t *testing.T, r *Reader) []record.Record {
	t.Helper()
	it, err := r.Scan(math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	var recs []record.Record
	for it.Next() {
		recs = append(recs, it.View().Clone())
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return recs
}

func TestReaderV1ZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(100)
	path := buildSSTable(t, dir, "zstd.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	got := collectAllRecords(t, r)
	require.Len(t, got, len(recs))
	for i, want := range recs {
		assert.Equal(t, want.Timestamp, got[i].Timestamp)
		assert.Equal(t, want.Payload, got[i].Payload)
	}
}

func TestReaderV1SnappyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(100)
	path := buildSSTable(t, dir, "snappy.sst",
		WriterOptions{CompressionType: CompressionSnappy, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	got := collectAllRecords(t, r)
	require.Len(t, got, len(recs))
	for i, want := range recs {
		assert.Equal(t, want.Timestamp, got[i].Timestamp)
		assert.Equal(t, want.Payload, got[i].Payload)
	}
}

func TestReaderV0BackwardCompat(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(50)
	path := buildSSTable(t, dir, "v0.sst",
		WriterOptions{BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	got := collectAllRecords(t, r)
	assert.Len(t, got, len(recs), "v0 reader must handle existing uncompressed files")
}

func TestReaderMixedV0V1Scan(t *testing.T) {
	dir := t.TempDir()

	// v0 SSTable: records at timestamps 0..49
	v0Recs := make([]record.Record, 50)
	for i := range 50 {
		v0Recs[i] = record.Record{
			Timestamp: int64(i * 1000),
			SourceID:  []byte("src"),
			Payload:   []byte("v0 payload"),
		}
	}
	v0Path := buildSSTable(t, dir, "v0.sst", WriterOptions{BlockSizeBytes: 512}, v0Recs)

	// v1 SSTable (zstd): records at timestamps 50000..99000
	v1Recs := make([]record.Record, 50)
	for i := range 50 {
		v1Recs[i] = record.Record{
			Timestamp: int64(50_000 + i*1000),
			SourceID:  []byte("src"),
			Payload:   []byte("v1 payload"),
		}
	}
	v1Path := buildSSTable(t, dir, "v1.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		v1Recs)

	// Open both readers and scan independently, simulating how the engine merges them.
	r0, err := NewReader(v0Path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r0.Close() })

	r1, err := NewReader(v1Path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r1.Close() })

	got0 := collectAllRecords(t, r0)
	got1 := collectAllRecords(t, r1)
	assert.Len(t, got0, 50, "v0 SSTable should return 50 records")
	assert.Len(t, got1, 50, "v1 SSTable should return 50 records")
	assert.Equal(t, int64(0), got0[0].Timestamp)
	assert.Equal(t, int64(50_000), got1[0].Timestamp)
}

// TestScanDecompAllocCount verifies that scanning a compressed SSTable reuses
// the decompression buffer across block boundaries instead of allocating once per block.
// 200 records at BlockSizeBytes=512 produces ~100 blocks; without reuse allocs/op ≈ 102,
// with reuse allocs/op ≈ 2.
func TestScanDecompAllocCount(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(200)
	path := buildSSTable(t, dir, "alloc.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	// Warm-up: pre-size any internal state before counting.
	_ = collectAllRecords(t, r)

	allocs := testing.AllocsPerRun(10, func() {
		it, err := r.Scan(math.MinInt64, math.MaxInt64, nil)
		require.NoError(t, err)
		for it.Next() {
			_ = it.View()
		}
		_ = it.Close()
	})
	// Without buffer reuse: ~1 alloc per block (≈102 for 100 blocks).
	// With buffer reuse: ≤ 5 without -race; ≤ 20 with -race (race detector shadow allocs).
	assert.LessOrEqual(t, allocs, float64(20),
		"want ≤ 20 allocs/scan (buffer reuse), got %.1f", allocs)
}

func TestReaderV2RoundTrip(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(30)
	path := buildSSTable(t, dir, "v2.sst",
		WriterOptions{ColumnOriented: true, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	assert.True(t, r.IsColumnar(), "reader must detect v2 (columnar) file")

	got := collectAllRecords(t, r)
	require.Len(t, got, len(recs))
	for i, want := range recs {
		assert.Equal(t, want.Timestamp, got[i].Timestamp, "record %d timestamp", i)
		assert.Equal(t, want.SourceID, got[i].SourceID, "record %d sourceID", i)
		assert.Equal(t, want.Payload, got[i].Payload, "record %d payload", i)
	}
}

func TestReaderV2WithCompressionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(30)
	path := buildSSTable(t, dir, "v2z.sst",
		WriterOptions{ColumnOriented: true, CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	assert.True(t, r.IsColumnar())

	got := collectAllRecords(t, r)
	assert.Len(t, got, len(recs))
}

func TestMixedVersionPartitionScan(t *testing.T) {
	dir := t.TempDir()

	v0Recs := []record.Record{
		{Timestamp: 1000, SourceID: []byte("s"), Payload: []byte("v0-a")},
		{Timestamp: 1001, SourceID: []byte("s"), Payload: []byte("v0-b")},
	}
	v1Recs := []record.Record{
		{Timestamp: 2000, SourceID: []byte("s"), Payload: []byte("v1-a")},
		{Timestamp: 2001, SourceID: []byte("s"), Payload: []byte("v1-b")},
	}
	v2Recs := []record.Record{
		{Timestamp: 3000, SourceID: []byte("s"), Payload: []byte("v2-a")},
		{Timestamp: 3001, SourceID: []byte("s"), Payload: []byte("v2-b")},
	}

	pathV0 := buildSSTable(t, dir, "v0.sst", WriterOptions{}, v0Recs)
	pathV1 := buildSSTable(t, dir, "v1.sst", WriterOptions{CompressionType: CompressionZstd}, v1Recs)
	pathV2 := buildSSTable(t, dir, "v2.sst", WriterOptions{ColumnOriented: true}, v2Recs)

	r0, err := NewReader(pathV0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r0.Close() })

	r1, err := NewReader(pathV1, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r1.Close() })

	r2, err := NewReader(pathV2, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r2.Close() })

	assert.False(t, r0.IsColumnar(), "v0 must not be columnar")
	assert.False(t, r1.IsColumnar(), "v1 must not be columnar")
	assert.True(t, r2.IsColumnar(), "v2 must be columnar")

	var got []record.Record
	for _, r := range []*Reader{r0, r1, r2} {
		it, err := r.Scan(1000, 4000, nil)
		require.NoError(t, err)
		for it.Next() {
			got = append(got, it.Record())
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
	}

	want := append(append(v0Recs, v1Recs...), v2Recs...)
	assert.Equal(t, want, got)
}

func TestReaderConcurrentCompressed(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(200)
	path := buildSSTable(t, dir, "concurrent.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	counts := make([]int, goroutines)

	for i := range goroutines {
		wg.Go(func() {
			it, err := r.Scan(math.MinInt64, math.MaxInt64, nil)
			if err != nil {
				errs[i] = err
				return
			}
			for it.Next() {
				counts[i]++
			}
			if err := it.Err(); err != nil {
				errs[i] = err
			}
			_ = it.Close()
		})
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d error", i)
	}
	for i, c := range counts {
		assert.Equal(t, len(recs), c, "goroutine %d count mismatch", i)
	}
}

func TestReaderBlockCacheHit(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(100)
	path := buildSSTable(t, dir, "cached.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)

	cache := NewBlockCache(8 << 20) // 8 MiB
	r, err := NewReader(path, cache)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	collectFull := func() []record.Record {
		it, err := r.Scan(math.MinInt64, math.MaxInt64, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, it.Close()) }()
		var out []record.Record
		for it.Next() {
			out = append(out, it.View().Clone())
		}
		require.NoError(t, it.Err())
		return out
	}

	// Cold scan: all blocks are cache misses.
	cold := collectFull()
	require.Len(t, cold, len(recs))
	entries, _ := cache.Metrics()
	assert.Equal(t, len(r.timeIndex), entries, "every block must be cached after first scan")

	// Warm scan: cache hits must produce identical records.
	warm := collectFull()
	require.Len(t, warm, len(recs))
	for i := range cold {
		assert.Equal(t, cold[i].Timestamp, warm[i].Timestamp, "record %d timestamp mismatch on warm scan", i)
		assert.Equal(t, cold[i].Payload, warm[i].Payload, "record %d payload mismatch on warm scan", i)
	}
}
