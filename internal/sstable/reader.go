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
	f               *os.File
	meta            SSTableMeta
	timeIndex       []timeIndexEntry
	srcIndex        map[string]srcEntry // nil when no source index was written
	mmap            []byte              // nil on Windows or when there are no data blocks
	compressionType CompressionType     // CompressionNone for v0 files
	columnar        bool                // true for version-2 (column-oriented) files
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

	r := &Reader{f: f, meta: meta, timeIndex: timeIndex, srcIndex: srcIndex}

	// Detect v1 format: if the file is large enough to hold a header plus at least
	// one data block, check whether the first 8 bytes equal headerMagic.
	if meta.TimeIndexOffset >= headerSize {
		var first8 [8]byte
		if _, err := f.ReadAt(first8[:], 0); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint64(first8[:]) == headerMagic {
			var hdr [headerSize]byte
			if _, err := f.ReadAt(hdr[:], 0); err != nil {
				return nil, err
			}
			version := binary.LittleEndian.Uint16(hdr[8:10])
			flags := binary.LittleEndian.Uint16(hdr[10:12])
			r.compressionType = CompressionType(flags & 0xFF)
			if version >= 2 {
				r.columnar = true
			}
		}
	}

	if err := r.initMmap(); err != nil {
		return nil, err
	}
	success = true
	return r, nil
}

// Meta returns the decoded footer metadata for this file.
// Callers may inspect MinTimestamp and MaxTimestamp to skip the file entirely
// before calling Scan.
func (r *Reader) Meta() SSTableMeta {
	return r.meta
}

// IsColumnar reports whether this is a version-2 (column-oriented) SSTable.
// When true, the Aggregate path can use timestamps-only block decoding, skipping payloads.
func (r *Reader) IsColumnar() bool {
	return r.columnar
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

	if r.columnar {
		recs, decompBuf, err := r.readColBlock(startBlock, nil)
		if err != nil {
			return nil, err
		}
		return &colScanIterator{
			r:         r,
			start:     start,
			end:       end,
			sourceID:  sourceID,
			blockIdx:  startBlock,
			blockRecs: recs,
			decompBuf: decompBuf,
		}, nil
	}

	blockData, blockRem, err := r.readBlockRaw(startBlock, nil)
	if err != nil {
		return nil, err
	}

	return &scanIterator{
		r:         r,
		start:     start,
		end:       end,
		sourceID:  sourceID,
		blockIdx:  startBlock,
		blockData: blockData,
		decompBuf: blockData, // seed reuse buffer; nil for uncompressed (harmless)
		blockOff:  4,         // skip 4-byte RecordCount header
		blockRem:  blockRem,
	}, nil
}

// Close unmaps the data-block region and closes the underlying file.
func (r *Reader) Close() error {
	if err := r.closeMmap(); err != nil {
		if cerr := r.f.Close(); cerr != nil {
			slog.Error("sstable: close file after munmap error", "err", cerr)
		}
		return err
	}
	return r.f.Close()
}

// readBlockRaw reads and CRC-validates the block at idx. Returns the validated payload
// (sans CRC), the record count from the header, and any error.
// decompBuf is an optional hint buffer for compressed files: when its capacity is sufficient
// to hold the decompressed block the backing array is reused, avoiding an allocation.
func (r *Reader) readBlockRaw(idx int, decompBuf []byte) (payload []byte, count int, err error) {
	e := r.timeIndex[idx]
	var buf []byte
	if r.mmap != nil {
		buf = r.mmap[e.offset : e.offset+uint64(e.size)]
	} else {
		b := make([]byte, e.size)
		if _, err := r.f.ReadAt(b, int64(e.offset)); err != nil {
			return nil, 0, err
		}
		buf = b
	}
	if r.compressionType != CompressionNone {
		decompressed, derr := decompress(r.compressionType, buf, decompBuf)
		if derr != nil {
			return nil, 0, ErrBlockCorrupt
		}
		buf = decompressed
	}
	payload, err = validateBlock(buf)
	if err != nil {
		return nil, 0, err
	}
	if len(payload) < 4 {
		return nil, 0, ErrBlockCorrupt
	}
	count = int(binary.LittleEndian.Uint32(payload[:4]))
	return payload, count, nil
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
// blockData holds the validated block payload (sans CRC); SourceID and Payload in
// current are zero-copy slices into blockData.
// For compressed files, decompBuf is reused across block boundaries: each new block
// decompresses into decompBuf's backing array when capacity is sufficient, replacing
// the previous block's data. RecordViews are valid only until the next call to Next.
type scanIterator struct {
	r        *Reader
	start    int64
	end      int64
	sourceID []byte

	blockIdx  int
	blockData []byte // validated block payload (record count header + records)
	decompBuf []byte // reuse hint for decompress; nil for uncompressed files
	blockOff  int    // parse cursor (starts at 4, after the 4-byte count header)
	blockRem  int    // records left to parse in blockData
	current   record.RecordView
	err       error
}

// Next advances to the next matching record. Returns false when exhausted or
// when a read error occurs; check Err() to distinguish the two.
func (it *scanIterator) Next() bool {
	for {
		for it.blockRem > 0 {
			view, err := parseRecordAt(it.blockData, &it.blockOff)
			if err != nil {
				it.err = err
				return false
			}
			it.blockRem--
			if view.Timestamp >= it.end {
				return false
			}
			if view.Timestamp < it.start {
				continue
			}
			if len(it.sourceID) > 0 && !bytes.Equal(view.SourceID, it.sourceID) {
				continue
			}
			it.current = view
			return true
		}

		it.blockIdx++
		if it.blockIdx >= len(it.r.timeIndex) {
			return false
		}
		if it.r.timeIndex[it.blockIdx].minTimestamp >= it.end {
			return false
		}

		payload, count, err := it.r.readBlockRaw(it.blockIdx, it.decompBuf)
		if err != nil {
			it.err = err
			return false
		}
		it.blockData = payload
		it.decompBuf = payload // update for next block
		it.blockOff = 4
		it.blockRem = count
	}
}

// View returns a zero-copy view of the current record. The view's SourceID and Payload
// slices are valid until the next call to Next.
func (it *scanIterator) View() record.RecordView { return it.current }

// Record returns a fully owned copy of the current record.
func (it *scanIterator) Record() record.Record { return it.current.Clone() }

// Close is a no-op; the Reader owns the file lifetime.
func (it *scanIterator) Close() error { return nil }

// Err returns the first read error encountered during iteration, or nil.
func (it *scanIterator) Err() error { return it.err }

// emptyIter is a record.Iterator that is immediately exhausted.
type emptyIter struct{}

func (emptyIter) Next() bool              { return false }
func (emptyIter) Record() record.Record   { return record.Record{} }
func (emptyIter) View() record.RecordView { return record.RecordView{} }
func (emptyIter) Close() error            { return nil }
func (emptyIter) Err() error              { return nil }

// readColBlock reads the columnar block at idx, decompresses if needed, and decodes
// it into a slice of records. Unlike readBlockRaw it returns owned copies of all fields.
// decompBuf is an optional hint buffer for compressed files: when its capacity is
// sufficient to hold the decompressed block the backing array is reused, avoiding an
// allocation. The caller should store the second return value and pass it on the next call.
func (r *Reader) readColBlock(idx int, decompBuf []byte) ([]record.Record, []byte, error) {
	e := r.timeIndex[idx]
	var buf []byte
	if r.mmap != nil {
		buf = r.mmap[e.offset : e.offset+uint64(e.size)]
	} else {
		b := make([]byte, e.size)
		if _, err := r.f.ReadAt(b, int64(e.offset)); err != nil {
			return nil, decompBuf, err
		}
		buf = b
	}
	if r.compressionType != CompressionNone {
		decompressed, derr := decompress(r.compressionType, buf, decompBuf)
		if derr != nil {
			return nil, decompBuf, ErrBlockCorrupt
		}
		buf = decompressed
		decompBuf = decompressed
	}
	recs, err := decodeColBlock(buf)
	return recs, decompBuf, err
}

// ScanTimestamps returns the timestamps of all records in the block at idx that
// fall within [start, end), without decoding sourceIDs or payloads.
// Only valid for columnar (version-2) files.
func (r *Reader) ScanTimestamps(idx int, start, end int64) ([]int64, error) {
	e := r.timeIndex[idx]
	var buf []byte
	if r.mmap != nil {
		buf = r.mmap[e.offset : e.offset+uint64(e.size)]
	} else {
		b := make([]byte, e.size)
		if _, err := r.f.ReadAt(b, int64(e.offset)); err != nil {
			return nil, err
		}
		buf = b
	}
	if r.compressionType != CompressionNone {
		decompressed, derr := decompress(r.compressionType, buf, nil)
		if derr != nil {
			return nil, ErrBlockCorrupt
		}
		buf = decompressed
	}
	all, err := decodeColBlockTimestamps(buf)
	if err != nil {
		return nil, err
	}
	filtered := all[:0]
	for _, ts := range all {
		if ts >= start && ts < end {
			filtered = append(filtered, ts)
		}
	}
	return filtered, nil
}

// TimeIndexLen returns the number of entries in the time index (one per data block).
func (r *Reader) TimeIndexLen() int { return len(r.timeIndex) }

// TimeIndexEntry returns the min-timestamp for the block at position i.
func (r *Reader) TimeIndexEntry(i int) (minTimestamp int64) {
	return r.timeIndex[i].minTimestamp
}

// colScanIterator iterates over records in a columnar (version-2) SSTable.
// It decodes one block at a time, keeping decoded records in memory until the block
// is exhausted, then loads the next matching block.
// For compressed files, decompBuf is reused across block boundaries to avoid a
// per-block decompression allocation (mirrors the row-path scanIterator behaviour).
type colScanIterator struct {
	r        *Reader
	start    int64
	end      int64
	sourceID []byte

	blockIdx  int
	blockRecs []record.Record
	recIdx    int
	decompBuf []byte // reuse hint for decompress; nil for uncompressed files
	current   record.RecordView
	err       error
}

// Next advances to the next matching record.
func (it *colScanIterator) Next() bool {
	for {
		for it.recIdx < len(it.blockRecs) {
			r := it.blockRecs[it.recIdx]
			it.recIdx++
			if r.Timestamp >= it.end {
				return false
			}
			if r.Timestamp < it.start {
				continue
			}
			if len(it.sourceID) > 0 && !bytes.Equal(r.SourceID, it.sourceID) {
				continue
			}
			it.current = record.RecordView(r)
			return true
		}

		it.blockIdx++
		if it.blockIdx >= len(it.r.timeIndex) {
			return false
		}
		if it.r.timeIndex[it.blockIdx].minTimestamp >= it.end {
			return false
		}
		recs, buf, err := it.r.readColBlock(it.blockIdx, it.decompBuf)
		if err != nil {
			it.err = err
			return false
		}
		it.blockRecs = recs
		it.decompBuf = buf
		it.recIdx = 0
	}
}

// View returns a zero-copy view of the current record. Slices are valid until the
// next call to Next that crosses a block boundary.
func (it *colScanIterator) View() record.RecordView { return it.current }

// Record returns a fully owned copy of the current record.
func (it *colScanIterator) Record() record.Record { return it.current.Clone() }

// Close is a no-op; the Reader owns the file lifetime.
func (it *colScanIterator) Close() error { return nil }

// Err returns the first read error encountered during iteration, or nil.
func (it *colScanIterator) Err() error { return it.err }
