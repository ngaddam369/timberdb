package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"log/slog"
	"os"
	"sort"

	"github.com/ngaddam369/timberdb/internal/record"
)

const (
	// footerSize is the fixed byte length of the SSTable footer.
	// Layout (80 bytes, all little-endian):
	//   [0:8]   min_timestamp      int64
	//   [8:16]  max_timestamp      int64
	//   [16:24] time_index_offset  uint64
	//   [24:32] time_index_size    uint64
	//   [32:40] src_index_offset   uint64
	//   [40:48] src_index_size     uint64
	//   [48:56] record_count       uint64
	//   [56:64] partition_start    int64
	//   [64:72] partition_end      int64
	//   [72:80] magic              uint64  ("TIMBERDB")
	footerSize = 80

	// footerMagic is the 8-byte sentinel at the end of every valid SSTable footer.
	footerMagic = uint64(0x54494D4245524442) // "TIMBERDB"

	// defaultBlockSize is the target encoded size for each data block.
	defaultBlockSize = 32 * 1024 // 32 KB

	// timeIndexEntrySize is the fixed byte size of one time index entry:
	//   min_timestamp(8B) | block_offset(8B) | block_size(4B)
	timeIndexEntrySize = 20
)

// ErrOutOfOrder is returned by Add when a record violates (Timestamp, SourceID) sort order.
var ErrOutOfOrder = errors.New("sstable: record added out of (Timestamp, SourceID) order")

// WriterOptions controls SSTable file creation.
type WriterOptions struct {
	// BlockSizeBytes is the target encoded size per data block. Default: 32 KB.
	BlockSizeBytes int
	// IndexSources builds the optional per-source index block.
	IndexSources bool
	// PartitionStart and PartitionEnd are the inclusive/exclusive window bounds (Unix ns).
	PartitionStart int64
	PartitionEnd   int64
}

// DefaultWriterOptions returns WriterOptions with sensible defaults.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{BlockSizeBytes: defaultBlockSize}
}

// SSTableMeta is the decoded footer of a finished SSTable file.
// It is returned by Finish and stored by the manifest for each live SSTable.
type SSTableMeta struct {
	Path            string
	MinTimestamp    int64
	MaxTimestamp    int64
	TimeIndexOffset uint64
	TimeIndexSize   uint64
	SrcIndexOffset  uint64
	SrcIndexSize    uint64
	RecordCount     uint64
	PartitionStart  int64
	PartitionEnd    int64
}

// timeIndexEntry records the on-disk position and timestamp bound of one data block.
type timeIndexEntry struct {
	minTimestamp int64
	offset       uint64
	size         uint32
}

// srcEntry tracks the first block offset and total record count for one source ID.
type srcEntry struct {
	firstBlockOffset uint64
	recordCount      uint64
}

// Writer streams sorted records into an SSTable file.
// Records must be supplied in (Timestamp, SourceID) order via Add.
// Call Finish exactly once after all Add calls; the Writer is unusable after that.
// Writer is not safe for concurrent use.
type Writer struct {
	opts WriterOptions
	f    *os.File
	bw   *bufio.Writer

	offset      uint64 // bytes written to disk so far
	recordCount uint64
	minTS       int64
	maxTS       int64
	hasRecords  bool

	// pending holds records for the block currently being accumulated.
	pending     []record.Record
	pendingSize int // estimated encoded size of pending records

	// order-check state
	lastTS    int64
	lastSrcID []byte

	// indexes built during writing
	timeIndex []timeIndexEntry
	srcIndex  map[string]*srcEntry // nil when IndexSources is false
	newSrcIDs []string             // source IDs first seen in the current pending block
}

// NewWriter creates an SSTable writer that writes to path.
// The file is created or truncated on open.
func NewWriter(path string, opts WriterOptions) (*Writer, error) {
	if opts.BlockSizeBytes <= 0 {
		opts.BlockSizeBytes = defaultBlockSize
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		opts: opts,
		f:    f,
		bw:   bufio.NewWriterSize(f, 256*1024),
	}
	if opts.IndexSources {
		w.srcIndex = make(map[string]*srcEntry)
	}
	return w, nil
}

// Add appends r to the SSTable. Returns ErrOutOfOrder if r breaks sort order.
// A data block is flushed automatically when the pending buffer reaches BlockSizeBytes.
func (w *Writer) Add(r record.Record) error {
	if w.hasRecords {
		if r.Timestamp < w.lastTS ||
			(r.Timestamp == w.lastTS && string(r.SourceID) < string(w.lastSrcID)) {
			return ErrOutOfOrder
		}
	}

	if !w.hasRecords {
		w.minTS = r.Timestamp
		w.hasRecords = true
	}
	w.maxTS = r.Timestamp
	w.recordCount++
	w.lastTS = r.Timestamp
	w.lastSrcID = r.SourceID

	if w.opts.IndexSources {
		key := string(r.SourceID)
		if _, seen := w.srcIndex[key]; !seen {
			w.srcIndex[key] = &srcEntry{}
			w.newSrcIDs = append(w.newSrcIDs, key)
		}
		w.srcIndex[key].recordCount++
	}

	w.pending = append(w.pending, r)
	w.pendingSize += 8 + 4 + len(r.SourceID) + 4 + len(r.Payload)
	if w.pendingSize >= w.opts.BlockSizeBytes {
		return w.flushBlock()
	}
	return nil
}

// flushBlock encodes the pending records into a data block and appends it to the file.
func (w *Writer) flushBlock() error {
	if len(w.pending) == 0 {
		return nil
	}

	blockOffset := w.offset

	for _, key := range w.newSrcIDs {
		w.srcIndex[key].firstBlockOffset = blockOffset
	}
	w.newSrcIDs = w.newSrcIDs[:0]

	encoded := encodeBlock(w.pending)
	if _, err := w.bw.Write(encoded); err != nil {
		return err
	}

	w.timeIndex = append(w.timeIndex, timeIndexEntry{
		minTimestamp: w.pending[0].Timestamp,
		offset:       blockOffset,
		size:         uint32(len(encoded)),
	})

	w.offset += uint64(len(encoded))
	w.pending = w.pending[:0]
	w.pendingSize = 0
	return nil
}

// Finish flushes any remaining records, writes the time index, source index (if enabled),
// and footer, then fsyncs the file. Returns SSTableMeta for registration in the manifest.
// The Writer must not be used after Finish.
func (w *Writer) Finish() (SSTableMeta, error) {
	var fileClosed bool
	defer func() {
		if !fileClosed {
			if cerr := w.f.Close(); cerr != nil {
				slog.Error("sstable: close writer file on error", "err", cerr)
			}
		}
	}()

	if err := w.flushBlock(); err != nil {
		return SSTableMeta{}, err
	}

	// Write time index.
	timeIdxOffset := w.offset
	timeIdxBuf := make([]byte, len(w.timeIndex)*timeIndexEntrySize)
	for i, e := range w.timeIndex {
		off := i * timeIndexEntrySize
		binary.LittleEndian.PutUint64(timeIdxBuf[off:], uint64(e.minTimestamp))
		binary.LittleEndian.PutUint64(timeIdxBuf[off+8:], e.offset)
		binary.LittleEndian.PutUint32(timeIdxBuf[off+16:], e.size)
	}
	if _, err := w.bw.Write(timeIdxBuf); err != nil {
		return SSTableMeta{}, err
	}
	timeIdxSize := uint64(len(timeIdxBuf))
	w.offset += timeIdxSize

	// Write source index (optional). Entries are sorted by source ID for determinism.
	srcIdxOffset := w.offset
	var srcIdxSize uint64
	if w.opts.IndexSources && len(w.srcIndex) > 0 {
		keys := make([]string, 0, len(w.srcIndex))
		for k := range w.srcIndex {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			e := w.srcIndex[key]
			idBytes := []byte(key)
			entry := make([]byte, 4+len(idBytes)+8+8)
			binary.LittleEndian.PutUint32(entry[0:4], uint32(len(idBytes)))
			copy(entry[4:], idBytes)
			off := 4 + len(idBytes)
			binary.LittleEndian.PutUint64(entry[off:], e.firstBlockOffset)
			binary.LittleEndian.PutUint64(entry[off+8:], e.recordCount)
			if _, err := w.bw.Write(entry); err != nil {
				return SSTableMeta{}, err
			}
			srcIdxSize += uint64(len(entry))
		}
	}
	w.offset += srcIdxSize

	// Write footer.
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:], uint64(w.minTS))
	binary.LittleEndian.PutUint64(footer[8:], uint64(w.maxTS))
	binary.LittleEndian.PutUint64(footer[16:], timeIdxOffset)
	binary.LittleEndian.PutUint64(footer[24:], timeIdxSize)
	binary.LittleEndian.PutUint64(footer[32:], srcIdxOffset)
	binary.LittleEndian.PutUint64(footer[40:], srcIdxSize)
	binary.LittleEndian.PutUint64(footer[48:], w.recordCount)
	binary.LittleEndian.PutUint64(footer[56:], uint64(w.opts.PartitionStart))
	binary.LittleEndian.PutUint64(footer[64:], uint64(w.opts.PartitionEnd))
	binary.LittleEndian.PutUint64(footer[72:], footerMagic)

	if _, err := w.bw.Write(footer[:]); err != nil {
		return SSTableMeta{}, err
	}

	if err := w.bw.Flush(); err != nil {
		return SSTableMeta{}, err
	}
	if err := w.f.Sync(); err != nil {
		return SSTableMeta{}, err
	}

	path := w.f.Name()
	fileClosed = true
	if err := w.f.Close(); err != nil {
		return SSTableMeta{}, err
	}

	return SSTableMeta{
		Path:            path,
		MinTimestamp:    w.minTS,
		MaxTimestamp:    w.maxTS,
		TimeIndexOffset: timeIdxOffset,
		TimeIndexSize:   timeIdxSize,
		SrcIndexOffset:  srcIdxOffset,
		SrcIndexSize:    srcIdxSize,
		RecordCount:     w.recordCount,
		PartitionStart:  w.opts.PartitionStart,
		PartitionEnd:    w.opts.PartitionEnd,
	}, nil
}
