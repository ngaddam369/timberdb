// Package sstable implements immutable on-disk SSTable files.
// Each SSTable covers a sealed time partition segment and is never modified after creation.
//
// File layout: Data Blocks | Time Index Block | Source Index Block (optional) | Footer (64B)
// Footer stores min_timestamp and max_timestamp — replaces bloom filters for time-range skipping.
package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/ngaddam369/timberdb/internal/record"
)

// ErrBlockCorrupt is returned when a data block fails CRC verification.
var ErrBlockCorrupt = errors.New("sstable: block CRC mismatch")

// Block wire format:
//
//	RecordCount(4B) | Record[0] | Record[1] | ... | CRC32(4B)
//
// Each record:
//
//	Timestamp(8B) | SrcIDLen(4B) | SourceID | PayloadLen(4B) | Payload
//
// CRC32 covers everything preceding it: RecordCount + all record bytes.

// encodeBlock serialises records into a self-verifying block.
// Records must already be in (Timestamp, SourceID) sort order — callers are responsible.
func encodeBlock(records []record.Record) []byte {
	size := 4 // RecordCount
	for _, r := range records {
		size += 8 + 4 + len(r.SourceID) + 4 + len(r.Payload)
	}
	size += 4 // CRC32

	buf := make([]byte, 0, size)

	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(records)))
	buf = append(buf, hdr[:]...)

	var tmp [8]byte
	for _, r := range records {
		binary.LittleEndian.PutUint64(tmp[:8], uint64(r.Timestamp))
		buf = append(buf, tmp[:8]...)

		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r.SourceID)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, r.SourceID...)

		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r.Payload)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, r.Payload...)
	}

	h := crc32.NewIEEE()
	h.Write(buf)
	binary.LittleEndian.PutUint32(tmp[:4], h.Sum32())
	buf = append(buf, tmp[:4]...)

	return buf
}

// validateBlock verifies the CRC of a raw block and returns the payload slice (everything
// except the trailing 4-byte CRC) or ErrBlockCorrupt. Does not allocate.
func validateBlock(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return nil, ErrBlockCorrupt
	}
	payload := data[:len(data)-4]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	h := crc32.NewIEEE()
	h.Write(payload)
	if h.Sum32() != storedCRC {
		return nil, ErrBlockCorrupt
	}
	return payload, nil
}

// parseRecordAt reads one record from payload starting at *off, returning a RecordView
// whose SourceID and Payload point directly into payload. *off is advanced past the record.
func parseRecordAt(payload []byte, off *int) (record.RecordView, error) {
	o := *off
	if o+12 > len(payload) {
		return record.RecordView{}, ErrBlockCorrupt
	}
	ts := int64(binary.LittleEndian.Uint64(payload[o : o+8]))
	o += 8

	srcIDLen := int(binary.LittleEndian.Uint32(payload[o : o+4]))
	o += 4

	if o+srcIDLen+4 > len(payload) {
		return record.RecordView{}, ErrBlockCorrupt
	}
	srcID := payload[o : o+srcIDLen]
	o += srcIDLen

	payloadLen := int(binary.LittleEndian.Uint32(payload[o : o+4]))
	o += 4

	if o+payloadLen > len(payload) {
		return record.RecordView{}, ErrBlockCorrupt
	}
	p := payload[o : o+payloadLen]
	o += payloadLen

	*off = o
	return record.RecordView{Timestamp: ts, SourceID: srcID, Payload: p}, nil
}

// Columnar block wire format:
//
//	RecordCount(4B) | SrcIDsSectionSize(4B) | PayloadsSectionSize(4B) |
//	TimestampsSection (RecordCount × 8B) |
//	SourceIDsSection (SrcIDsSectionSize bytes) |
//	PayloadsSection (PayloadsSectionSize bytes) |
//	CRC32(4B)
//
// The fixed-width timestamps section starts at offset 12 and is RecordCount×8 bytes long,
// making it addressable without parsing the variable-length sections.

// encodeColBlock serialises records into a columnar self-verifying block.
// Records must already be in (Timestamp, SourceID) sort order — callers are responsible.
func encodeColBlock(records []record.Record) []byte {
	srcIDsSize := 0
	payloadsSize := 0
	for _, r := range records {
		srcIDsSize += 4 + len(r.SourceID)
		payloadsSize += 4 + len(r.Payload)
	}

	buf := make([]byte, 0, 12+len(records)*8+srcIDsSize+payloadsSize+4)

	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(records)))
	buf = append(buf, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(srcIDsSize))
	buf = append(buf, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(payloadsSize))
	buf = append(buf, tmp[:4]...)

	for _, r := range records {
		binary.LittleEndian.PutUint64(tmp[:8], uint64(r.Timestamp))
		buf = append(buf, tmp[:8]...)
	}
	for _, r := range records {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r.SourceID)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, r.SourceID...)
	}
	for _, r := range records {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r.Payload)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, r.Payload...)
	}

	h := crc32.NewIEEE()
	h.Write(buf)
	binary.LittleEndian.PutUint32(tmp[:4], h.Sum32())
	buf = append(buf, tmp[:4]...)
	return buf
}

// decodeColBlock deserialises a columnar block produced by encodeColBlock.
// Returns ErrBlockCorrupt if the CRC does not match or the data is malformed.
func decodeColBlock(data []byte) ([]record.Record, error) {
	if len(data) < 16 { // minimum: 12B header + 4B CRC, zero records
		return nil, ErrBlockCorrupt
	}
	payload := data[:len(data)-4]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	h := crc32.NewIEEE()
	h.Write(payload)
	if h.Sum32() != storedCRC {
		return nil, ErrBlockCorrupt
	}

	count := int(binary.LittleEndian.Uint32(payload[0:4]))
	srcIDsSize := int(binary.LittleEndian.Uint32(payload[4:8]))

	tsStart := 12
	tsEnd := tsStart + count*8
	if tsEnd > len(payload) {
		return nil, ErrBlockCorrupt
	}
	srcIDsEnd := tsEnd + srcIDsSize
	if srcIDsEnd > len(payload) {
		return nil, ErrBlockCorrupt
	}

	records := make([]record.Record, 0, count)

	off := tsEnd
	srcIDs := make([][]byte, count)
	for i := range count {
		if off+4 > srcIDsEnd {
			return nil, ErrBlockCorrupt
		}
		l := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4
		if off+l > srcIDsEnd {
			return nil, ErrBlockCorrupt
		}
		b := make([]byte, l)
		copy(b, payload[off:off+l])
		srcIDs[i] = b
		off += l
	}

	off = srcIDsEnd
	for i := range count {
		ts := int64(binary.LittleEndian.Uint64(payload[tsStart+i*8:]))
		if off+4 > len(payload) {
			return nil, ErrBlockCorrupt
		}
		l := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4
		if off+l > len(payload) {
			return nil, ErrBlockCorrupt
		}
		p := make([]byte, l)
		copy(p, payload[off:off+l])
		off += l
		records = append(records, record.Record{Timestamp: ts, SourceID: srcIDs[i], Payload: p})
	}

	return records, nil
}

// decodeColBlockTimestamps extracts only the timestamps column from a columnar block.
// It verifies the full-block CRC but does not allocate or parse SourceIDs or Payloads.
func decodeColBlockTimestamps(data []byte) ([]int64, error) {
	if len(data) < 16 {
		return nil, ErrBlockCorrupt
	}
	payload := data[:len(data)-4]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	h := crc32.NewIEEE()
	h.Write(payload)
	if h.Sum32() != storedCRC {
		return nil, ErrBlockCorrupt
	}

	count := int(binary.LittleEndian.Uint32(payload[0:4]))
	tsEnd := 12 + count*8
	if tsEnd > len(payload) {
		return nil, ErrBlockCorrupt
	}

	timestamps := make([]int64, count)
	for i := range count {
		timestamps[i] = int64(binary.LittleEndian.Uint64(payload[12+i*8:]))
	}
	return timestamps, nil
}

// decodeBlock deserialises a block produced by encodeBlock.
// Returns ErrBlockCorrupt if the CRC does not match or the data is malformed.
func decodeBlock(data []byte) ([]record.Record, error) {
	if len(data) < 8 { // 4 RecordCount + 4 CRC minimum
		return nil, ErrBlockCorrupt
	}

	payload := data[:len(data)-4]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])

	h := crc32.NewIEEE()
	h.Write(payload)
	if h.Sum32() != storedCRC {
		return nil, ErrBlockCorrupt
	}

	count := int(binary.LittleEndian.Uint32(payload[:4]))
	records := make([]record.Record, 0, count)

	off := 4
	for range count {
		if off+12 > len(payload) {
			return nil, ErrBlockCorrupt
		}
		ts := int64(binary.LittleEndian.Uint64(payload[off : off+8]))
		off += 8

		srcIDLen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4

		if off+srcIDLen+4 > len(payload) {
			return nil, ErrBlockCorrupt
		}
		srcID := make([]byte, srcIDLen)
		copy(srcID, payload[off:off+srcIDLen])
		off += srcIDLen

		payloadLen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4

		if off+payloadLen > len(payload) {
			return nil, ErrBlockCorrupt
		}
		p := make([]byte, payloadLen)
		copy(p, payload[off:off+payloadLen])
		off += payloadLen

		records = append(records, record.Record{
			Timestamp: ts,
			SourceID:  srcID,
			Payload:   p,
		})
	}

	return records, nil
}
