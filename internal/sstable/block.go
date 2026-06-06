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
