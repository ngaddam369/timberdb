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

	r, err := NewReader(path)
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

	r, err := NewReader(path)
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

	r, err := NewReader(path)
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
	r0, err := NewReader(v0Path)
	require.NoError(t, err)
	t.Cleanup(func() { r0.Close() })

	r1, err := NewReader(v1Path)
	require.NoError(t, err)
	t.Cleanup(func() { r1.Close() })

	got0 := collectAllRecords(t, r0)
	got1 := collectAllRecords(t, r1)
	assert.Len(t, got0, 50, "v0 SSTable should return 50 records")
	assert.Len(t, got1, 50, "v1 SSTable should return 50 records")
	assert.Equal(t, int64(0), got0[0].Timestamp)
	assert.Equal(t, int64(50_000), got1[0].Timestamp)
}

func TestReaderConcurrentCompressed(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(200)
	path := buildSSTable(t, dir, "concurrent.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)

	r, err := NewReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	counts := make([]int, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
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
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d error", i)
	}
	for i, c := range counts {
		assert.Equal(t, len(recs), c, "goroutine %d count mismatch", i)
	}
}
