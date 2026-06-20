package sstable

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/record"
)

// compressRecords builds a small deterministic record slice for compression tests.
func compressRecords(n int) []record.Record {
	recs := make([]record.Record, n)
	for i := range n {
		recs[i] = record.Record{
			Timestamp: int64(1_000_000 + i*1000),
			SourceID:  []byte("sensor"),
			// Highly compressible zero-byte payload.
			Payload: make([]byte, 256),
		}
	}
	return recs
}

// buildSSTable writes records to dir/name.sst with opts and returns the path.
func buildSSTable(t *testing.T, dir, name string, opts WriterOptions, recs []record.Record) string {
	t.Helper()
	path := filepath.Join(dir, name)
	w, err := NewWriter(path, opts)
	require.NoError(t, err)
	for _, r := range recs {
		require.NoError(t, w.Add(r))
	}
	_, err = w.Finish()
	require.NoError(t, err)
	return path
}

func TestWriterV1HeaderPresent(t *testing.T) {
	dir := t.TempDir()
	path := buildSSTable(t, dir, "v1.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 4096},
		compressRecords(10))

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	var first8 [8]byte
	_, err = f.ReadAt(first8[:], 0)
	require.NoError(t, err)

	got := binary.LittleEndian.Uint64(first8[:])
	assert.Equal(t, headerMagic, got, "v1 file must start with headerMagic")
}

func TestWriterV0NoHeader(t *testing.T) {
	dir := t.TempDir()
	path := buildSSTable(t, dir, "v0.sst",
		WriterOptions{BlockSizeBytes: 4096},
		compressRecords(10))

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	var first8 [8]byte
	_, err = f.ReadAt(first8[:], 0)
	require.NoError(t, err)

	got := binary.LittleEndian.Uint64(first8[:])
	assert.NotEqual(t, headerMagic, got, "v0 file must NOT start with headerMagic")
}

func TestWriterBlocksCompressed(t *testing.T) {
	dir := t.TempDir()
	recs := compressRecords(50)

	pathZ := buildSSTable(t, dir, "zstd.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 512},
		recs)
	pathNone := buildSSTable(t, dir, "none.sst",
		WriterOptions{BlockSizeBytes: 512},
		recs)

	siZ, err := os.Stat(pathZ)
	require.NoError(t, err)
	siNone, err := os.Stat(pathNone)
	require.NoError(t, err)

	assert.Less(t, siZ.Size(), siNone.Size(),
		"zstd-compressed SSTable must be smaller than uncompressed for zero-byte payloads")
}

func TestWriterV1OffsetStartsAfterHeader(t *testing.T) {
	dir := t.TempDir()
	path := buildSSTable(t, dir, "v1.sst",
		WriterOptions{CompressionType: CompressionZstd, BlockSizeBytes: 4096},
		compressRecords(10))

	r, err := NewReader(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	assert.GreaterOrEqual(t, r.Meta().TimeIndexOffset, uint64(headerSize),
		"time index offset must account for the 16-byte file header")
}

func TestWriterV2HeaderPresent(t *testing.T) {
	dir := t.TempDir()
	path := buildSSTable(t, dir, "v2.sst",
		WriterOptions{ColumnOriented: true, BlockSizeBytes: 4096},
		compressRecords(10))

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	var hdr [headerSize]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)

	assert.Equal(t, headerMagic, binary.LittleEndian.Uint64(hdr[0:8]),
		"v2 file must start with headerMagic")
	assert.Equal(t, uint16(2), binary.LittleEndian.Uint16(hdr[8:10]),
		"v2 file header version must be 2")
}

func TestWriterV2WithCompressionHeader(t *testing.T) {
	dir := t.TempDir()
	path := buildSSTable(t, dir, "v2z.sst",
		WriterOptions{ColumnOriented: true, CompressionType: CompressionZstd, BlockSizeBytes: 4096},
		compressRecords(10))

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	var hdr [headerSize]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)

	assert.Equal(t, uint16(2), binary.LittleEndian.Uint16(hdr[8:10]),
		"columnar+compressed file must have version 2")
	assert.Equal(t, uint16(CompressionZstd), binary.LittleEndian.Uint16(hdr[10:12]),
		"compression type must be stored in flags")
}
