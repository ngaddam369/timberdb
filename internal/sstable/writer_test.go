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

// makeWriter creates a Writer in t.TempDir() and registers cleanup.
func makeWriter(t *testing.T, opts WriterOptions) (*Writer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.sst")
	w, err := NewWriter(path, opts)
	require.NoError(t, err)
	return w, path
}

func TestWriterFinishEmpty(t *testing.T) {
	w, path := makeWriter(t, DefaultWriterOptions())
	meta, err := w.Finish()
	require.NoError(t, err)

	assert.Equal(t, path, meta.Path)
	assert.Equal(t, uint64(0), meta.RecordCount)
	assert.Equal(t, int64(0), meta.MinTimestamp)
	assert.Equal(t, int64(0), meta.MaxTimestamp)

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(footerSize), info.Size(), "empty SSTable should be exactly one footer")
}

func TestWriterSingleBlock(t *testing.T) {
	opts := DefaultWriterOptions()
	opts.PartitionStart = 1000
	opts.PartitionEnd = 5000
	w, path := makeWriter(t, opts)

	records := []record.Record{
		{Timestamp: 1000, SourceID: []byte("s1"), Payload: []byte("a")},
		{Timestamp: 2000, SourceID: []byte("s2"), Payload: []byte("b")},
		{Timestamp: 3000, SourceID: []byte("s1"), Payload: []byte("c")},
	}
	for _, r := range records {
		require.NoError(t, w.Add(r))
	}
	meta, err := w.Finish()
	require.NoError(t, err)

	assert.Equal(t, path, meta.Path)
	assert.Equal(t, int64(1000), meta.MinTimestamp)
	assert.Equal(t, int64(3000), meta.MaxTimestamp)
	assert.Equal(t, uint64(3), meta.RecordCount)
	assert.Equal(t, int64(1000), meta.PartitionStart)
	assert.Equal(t, int64(5000), meta.PartitionEnd)
	assert.Equal(t, uint64(1), meta.TimeIndexSize/timeIndexEntrySize, "one data block expected")
}

func TestWriterMultiBlock(t *testing.T) {
	// Use a tiny block size to force multiple blocks.
	opts := WriterOptions{BlockSizeBytes: 64}
	w, _ := makeWriter(t, opts)

	var ts int64
	for range 50 {
		ts += 1000
		require.NoError(t, w.Add(record.Record{
			Timestamp: ts,
			SourceID:  []byte("src"),
			Payload:   make([]byte, 20), // 20-byte payload forces block splits
		}))
	}
	meta, err := w.Finish()
	require.NoError(t, err)

	assert.Equal(t, uint64(50), meta.RecordCount)
	assert.Greater(t, meta.TimeIndexSize/timeIndexEntrySize, uint64(1), "should have more than one block")
}

func TestWriterOutOfOrder(t *testing.T) {
	cases := []struct {
		name    string
		records []record.Record
	}{
		{
			"timestamp decreases",
			[]record.Record{
				{Timestamp: 2000, SourceID: []byte("s1")},
				{Timestamp: 1000, SourceID: []byte("s1")},
			},
		},
		{
			"same timestamp, source ID decreases",
			[]record.Record{
				{Timestamp: 1000, SourceID: []byte("s2")},
				{Timestamp: 1000, SourceID: []byte("s1")},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := makeWriter(t, DefaultWriterOptions())
			require.NoError(t, w.Add(tc.records[0]))
			err := w.Add(tc.records[1])
			assert.ErrorIs(t, err, ErrOutOfOrder)
		})
	}
}

func TestWriterDuplicateRecord(t *testing.T) {
	// Two records with identical (Timestamp, SourceID) are not out-of-order —
	// the writer uses a strict less-than check, so equal keys are accepted.
	w, _ := makeWriter(t, DefaultWriterOptions())
	r := record.Record{Timestamp: 1000, SourceID: []byte("s1"), Payload: []byte("a")}
	require.NoError(t, w.Add(r))
	r2 := record.Record{Timestamp: 1000, SourceID: []byte("s1"), Payload: []byte("b")}
	require.NoError(t, w.Add(r2), "duplicate (ts, sourceID) must be accepted")
	meta, err := w.Finish()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), meta.RecordCount)
}

func TestWriterFooterBytes(t *testing.T) {
	opts := WriterOptions{BlockSizeBytes: defaultBlockSize, PartitionStart: 100, PartitionEnd: 200}
	w, path := makeWriter(t, opts)

	require.NoError(t, w.Add(record.Record{Timestamp: 111, SourceID: []byte("s"), Payload: []byte("p")}))
	require.NoError(t, w.Add(record.Record{Timestamp: 222, SourceID: []byte("s"), Payload: []byte("p")}))
	_, err := w.Finish()
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), footerSize)

	footer := raw[len(raw)-footerSize:]
	assert.Equal(t, int64(111), int64(binary.LittleEndian.Uint64(footer[0:])), "min_timestamp")
	assert.Equal(t, int64(222), int64(binary.LittleEndian.Uint64(footer[8:])), "max_timestamp")
	assert.Equal(t, uint64(2), binary.LittleEndian.Uint64(footer[48:]), "record_count")
	assert.Equal(t, int64(100), int64(binary.LittleEndian.Uint64(footer[56:])), "partition_start")
	assert.Equal(t, int64(200), int64(binary.LittleEndian.Uint64(footer[64:])), "partition_end")
	assert.Equal(t, footerMagic, binary.LittleEndian.Uint64(footer[72:]), "magic")
}

func TestWriterSourceIndex(t *testing.T) {
	opts := WriterOptions{BlockSizeBytes: defaultBlockSize, IndexSources: true}
	w, path := makeWriter(t, opts)

	records := []record.Record{
		{Timestamp: 1000, SourceID: []byte("alpha"), Payload: []byte("1")},
		{Timestamp: 2000, SourceID: []byte("beta"), Payload: []byte("2")},
		{Timestamp: 3000, SourceID: []byte("alpha"), Payload: []byte("3")},
	}
	for _, r := range records {
		require.NoError(t, w.Add(r))
	}
	meta, err := w.Finish()
	require.NoError(t, err)

	assert.Greater(t, meta.SrcIndexSize, uint64(0), "source index should be non-empty")

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	srcBlock := raw[meta.SrcIndexOffset : meta.SrcIndexOffset+meta.SrcIndexSize]

	// Parse the source index and verify the two source IDs are present.
	srcCounts := make(map[string]uint64)
	off := 0
	for off < len(srcBlock) {
		idLen := int(binary.LittleEndian.Uint32(srcBlock[off:]))
		off += 4
		srcID := string(srcBlock[off : off+idLen])
		off += idLen
		off += 8 // skip firstBlockOffset
		count := binary.LittleEndian.Uint64(srcBlock[off:])
		off += 8
		srcCounts[srcID] = count
	}

	assert.Equal(t, uint64(2), srcCounts["alpha"])
	assert.Equal(t, uint64(1), srcCounts["beta"])
}
