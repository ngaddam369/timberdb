package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/ngaddam369/timberdb/internal/record"
)

// ErrInvalidMagic is returned when a file's footer magic does not match,
// indicating the file is not a valid SSTable.
var ErrInvalidMagic = errors.New("sstable: invalid magic — not a valid SSTable file")

// Reader reads records from an immutable SSTable file.
// Multiple goroutines may call Scan concurrently.
// Close must not be called concurrently with any other method.
type Reader struct {
	f         *os.File
	meta      SSTableMeta
	timeIndex []timeIndexEntry
	srcIndex  map[string]srcEntry // nil when no source index was written
}

// NewReader opens path, validates the footer, and loads the time index into memory.
// The file stays open until Close is called.
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// On any error after this point, close the file before returning.
	// The success flag is set only when we hand ownership to the Reader.
	success := false
	defer func() {
		if !success {
			if cerr := f.Close(); cerr != nil {
				slog.Error("sstable: cleanup close failed", "err", cerr)
			}
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < footerSize {
		return nil, ErrInvalidMagic
	}

	var footerBuf [footerSize]byte
	if _, err := f.ReadAt(footerBuf[:], info.Size()-footerSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint64(footerBuf[72:]) != footerMagic {
		return nil, ErrInvalidMagic
	}

	meta := SSTableMeta{
		Path:            path,
		MinTimestamp:    int64(binary.LittleEndian.Uint64(footerBuf[0:])),
		MaxTimestamp:    int64(binary.LittleEndian.Uint64(footerBuf[8:])),
		TimeIndexOffset: binary.LittleEndian.Uint64(footerBuf[16:]),
		TimeIndexSize:   binary.LittleEndian.Uint64(footerBuf[24:]),
		SrcIndexOffset:  binary.LittleEndian.Uint64(footerBuf[32:]),
		SrcIndexSize:    binary.LittleEndian.Uint64(footerBuf[40:]),
		RecordCount:     binary.LittleEndian.Uint64(footerBuf[48:]),
		PartitionStart:  int64(binary.LittleEndian.Uint64(footerBuf[56:])),
		PartitionEnd:    int64(binary.LittleEndian.Uint64(footerBuf[64:])),
	}

	if meta.RecordCount > 0 && meta.TimeIndexSize == 0 {
		return nil, ErrInvalidMagic
	}

	timeIndex, err := loadTimeIndex(f, meta)
	if err != nil {
		return nil, err
	}

	srcIndex, err := loadSrcIndex(f, meta)
	if err != nil {
		return nil, err
	}

	success = true
	return &Reader{f: f, meta: meta, timeIndex: timeIndex, srcIndex: srcIndex}, nil
}

// Meta returns the decoded footer metadata for this file.
// Callers may inspect MinTimestamp and MaxTimestamp to skip the file entirely
// before calling Scan.
func (r *Reader) Meta() SSTableMeta {
	return r.meta
}

// Scan returns an iterator over records with Timestamp in [start, end),
// optionally filtered to sourceID (pass nil to return all sources).
// The iterator is not safe for concurrent use, but multiple iterators from
// the same Reader may run concurrently.
func (r *Reader) Scan(start, end int64, sourceID []byte) (record.Iterator, error) {
	if r.meta.RecordCount == 0 || r.meta.MaxTimestamp < start || r.meta.MinTimestamp >= end {
		return emptyIter{}, nil
	}

	// Source index short-circuit: if the requested source is not in this file, skip it.
	if len(sourceID) > 0 && r.srcIndex != nil {
		if _, ok := r.srcIndex[string(sourceID)]; !ok {
			return emptyIter{}, nil
		}
	}

	// Binary search: find the last block whose minTimestamp <= start.
	// That block might straddle the start boundary, so we must read it.
	startBlock := sort.Search(len(r.timeIndex), func(i int) bool {
		return r.timeIndex[i].minTimestamp > start
	})
	if startBlock > 0 {
		startBlock--
	}

	records, err := r.readBlock(startBlock)
	if err != nil {
		return nil, err
	}

	return &scanIterator{
		r:        r,
		start:    start,
		end:      end,
		sourceID: sourceID,
		blockIdx: startBlock,
		records:  records,
	}, nil
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.f.Close()
}

// readBlock reads and decodes the data block at index idx in the time index.
func (r *Reader) readBlock(idx int) ([]record.Record, error) {
	e := r.timeIndex[idx]
	buf := make([]byte, e.size)
	if _, err := r.f.ReadAt(buf, int64(e.offset)); err != nil {
		return nil, err
	}
	return decodeBlock(buf)
}

// loadTimeIndex reads the time index block and parses it into a slice of entries.
func loadTimeIndex(f *os.File, meta SSTableMeta) ([]timeIndexEntry, error) {
	if meta.TimeIndexSize == 0 {
		return nil, nil
	}
	buf := make([]byte, meta.TimeIndexSize)
	if _, err := f.ReadAt(buf, int64(meta.TimeIndexOffset)); err != nil {
		return nil, err
	}
	count := len(buf) / timeIndexEntrySize
	entries := make([]timeIndexEntry, count)
	for i := range count {
		off := i * timeIndexEntrySize
		entries[i] = timeIndexEntry{
			minTimestamp: int64(binary.LittleEndian.Uint64(buf[off:])),
			offset:       binary.LittleEndian.Uint64(buf[off+8:]),
			size:         binary.LittleEndian.Uint32(buf[off+16:]),
		}
	}
	return entries, nil
}

// loadSrcIndex reads the source index block and returns a lookup map, or nil if absent.
func loadSrcIndex(f *os.File, meta SSTableMeta) (map[string]srcEntry, error) {
	if meta.SrcIndexSize == 0 {
		return nil, nil
	}
	buf := make([]byte, meta.SrcIndexSize)
	if _, err := f.ReadAt(buf, int64(meta.SrcIndexOffset)); err != nil {
		return nil, err
	}
	index := make(map[string]srcEntry)
	off := 0
	for off < len(buf) {
		if off+4 > len(buf) {
			return nil, fmt.Errorf("sstable: corrupt source index")
		}
		idLen := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
		if off+idLen+16 > len(buf) {
			return nil, fmt.Errorf("sstable: corrupt source index")
		}
		srcID := string(buf[off : off+idLen])
		off += idLen
		index[srcID] = srcEntry{
			firstBlockOffset: binary.LittleEndian.Uint64(buf[off:]),
			recordCount:      binary.LittleEndian.Uint64(buf[off+8:]),
		}
		off += 16
	}
	return index, nil
}

// scanIterator iterates over records in a time-range and optional source filter.
type scanIterator struct {
	r        *Reader
	start    int64
	end      int64
	sourceID []byte

	blockIdx int
	records  []record.Record
	pos      int
	current  record.Record
	err      error
}

// Next advances to the next matching record. Returns false when exhausted or
// when a read error occurs; check Err() to distinguish the two.
func (it *scanIterator) Next() bool {
	for {
		for it.pos < len(it.records) {
			r := it.records[it.pos]
			it.pos++
			if r.Timestamp >= it.end {
				return false
			}
			if r.Timestamp < it.start {
				continue
			}
			if len(it.sourceID) > 0 && !bytes.Equal(r.SourceID, it.sourceID) {
				continue
			}
			it.current = r
			return true
		}

		it.blockIdx++
		if it.blockIdx >= len(it.r.timeIndex) {
			return false
		}
		if it.r.timeIndex[it.blockIdx].minTimestamp >= it.end {
			return false
		}

		records, err := it.r.readBlock(it.blockIdx)
		if err != nil {
			it.err = err
			return false
		}
		it.records = records
		it.pos = 0
	}
}

// Record returns the record at the current iterator position.
func (it *scanIterator) Record() record.Record {
	return it.current
}

// Close is a no-op; the Reader owns the file lifetime.
func (it *scanIterator) Close() error {
	return nil
}

// Err returns the first read error encountered during iteration, or nil.
func (it *scanIterator) Err() error { return it.err }

// emptyIter is a record.Iterator that is immediately exhausted.
type emptyIter struct{}

func (emptyIter) Next() bool            { return false }
func (emptyIter) Record() record.Record { return record.Record{} }
func (emptyIter) Close() error          { return nil }
func (emptyIter) Err() error            { return nil }
