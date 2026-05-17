// Package wal implements the write-ahead log for crash durability.
// Every write lands in the WAL before the memtable. On crash and
// restart, the WAL is replayed to rebuild all active partition memtables.
//
// Record format (binary, sequential):
//
//	CRC32(4B) | Len(4B) | Timestamp(8B) | SrcIDLen(4B) | SourceID | PayloadLen(4B) | Payload
//
// WAL files are named: wal-{partition_window}-{seq}.wal
package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/ngaddam369/timberdb/internal/record"
)

// WALSyncMode controls when the WAL is fsynced to disk.
type WALSyncMode int

const (
	SyncAlways   WALSyncMode = iota // fsync after every Append
	SyncPeriodic                    // fsync on a background ticker (default 200ms)
	SyncNever                       // let the OS decide; highest throughput, lowest durability
)

// maxRecordBodySize is a sanity cap when reading from a potentially corrupt WAL.
const maxRecordBodySize = 10 * 1024 * 1024 // 10 MB

// WAL is a sequential, append-only binary log. All methods are safe for concurrent use.
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	bw       *bufio.Writer
	syncMode WALSyncMode
	ticker   *time.Ticker
	done     chan struct{}
	wg       sync.WaitGroup
}

// Open opens or creates a WAL file at path with the given sync mode.
// Callers should call Replay immediately after Open to rebuild in-memory state
// from any prior run before calling Append.
func Open(path string, syncMode WALSyncMode) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		file:     f,
		bw:       bufio.NewWriterSize(f, 64*1024),
		syncMode: syncMode,
	}
	if syncMode == SyncPeriodic {
		w.ticker = time.NewTicker(200 * time.Millisecond)
		w.done = make(chan struct{})
		w.wg.Go(w.periodicSync)
	}
	return w, nil
}

func (w *WAL) periodicSync() {
	for {
		select {
		case <-w.ticker.C:
			w.mu.Lock()
			if err := w.bw.Flush(); err != nil {
				slog.Error("wal periodic flush failed", "err", err)
			}
			if err := w.file.Sync(); err != nil {
				slog.Error("wal periodic sync failed", "err", err)
			}
			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}

// Append encodes r and appends it to the WAL. Behavior on disk flush is
// determined by the WALSyncMode given to Open.
func (w *WAL) Append(r record.Record) error {
	buf := encodeRecord(r)
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.bw.Write(buf); err != nil {
		return err
	}
	if w.syncMode == SyncAlways {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}
	return nil
}

// Replay reads all complete records from the WAL and calls fn for each.
// A partial last record (detected via CRC mismatch or unexpected EOF) is
// silently truncated — this is the expected result of a crash mid-write.
// Replay must be called before any Append calls.
func (w *WAL) Replay(fn func(record.Record)) (retErr error) {
	// Flush any buffered bytes so the reader sees everything on disk.
	w.mu.Lock()
	err := w.bw.Flush()
	w.mu.Unlock()
	if err != nil {
		return err
	}

	f, err := os.Open(w.file.Name())
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = cerr
		}
	}()

	br := bufio.NewReader(f)
	for {
		var header [8]byte
		if _, err := io.ReadFull(br, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end or truncated header = partial write
			}
			return err
		}

		bodyLen := int(binary.LittleEndian.Uint32(header[4:8]))
		if bodyLen > maxRecordBodySize {
			return nil // corrupt length field, stop replay
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(br, body); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil // partial body = partial write
			}
			return err
		}

		// CRC covers Len(4B) || body.
		h := crc32.NewIEEE()
		h.Write(header[4:8])
		h.Write(body)
		if h.Sum32() != binary.LittleEndian.Uint32(header[0:4]) {
			return nil // CRC mismatch = partial last record, silently stop
		}

		fn(decodeBody(body))
	}
}

// Rotate fsyncs and closes the current WAL file, then opens newPath for subsequent writes.
// Called after a successful SSTable flush to seal the current WAL segment.
func (w *WAL) Rotate(newPath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.bw = bufio.NewWriterSize(f, 64*1024)
	return nil
}

// Close flushes all buffered bytes, fsyncs, and closes the WAL file.
func (w *WAL) Close() error {
	if w.ticker != nil {
		w.ticker.Stop()
		close(w.done)
		w.wg.Wait()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// encodeRecord serialises r into:
//
//	CRC32(4B) | Len(4B) | Timestamp(8B) | SrcIDLen(4B) | SourceID | PayloadLen(4B) | Payload
//
// CRC is computed over Len(4B) || body.
func encodeRecord(r record.Record) []byte {
	srcIDLen := len(r.SourceID)
	payloadLen := len(r.Payload)
	// body = Timestamp(8) + SrcIDLen(4) + SourceID + PayloadLen(4) + Payload
	bodyLen := 8 + 4 + srcIDLen + 4 + payloadLen
	buf := make([]byte, 8+bodyLen) // CRC32(4) + Len(4) + body

	binary.LittleEndian.PutUint32(buf[4:8], uint32(bodyLen))

	body := buf[8:]
	binary.LittleEndian.PutUint64(body[0:8], uint64(r.Timestamp))
	binary.LittleEndian.PutUint32(body[8:12], uint32(srcIDLen))
	copy(body[12:12+srcIDLen], r.SourceID)
	off := 12 + srcIDLen
	binary.LittleEndian.PutUint32(body[off:off+4], uint32(payloadLen))
	copy(body[off+4:], r.Payload)

	h := crc32.NewIEEE()
	h.Write(buf[4:]) // Len + body
	binary.LittleEndian.PutUint32(buf[0:4], h.Sum32())
	return buf
}

// decodeBody decodes a body slice (everything after the 8-byte header) into a Record.
func decodeBody(body []byte) record.Record {
	ts := int64(binary.LittleEndian.Uint64(body[0:8]))
	srcIDLen := int(binary.LittleEndian.Uint32(body[8:12]))
	srcID := make([]byte, srcIDLen)
	copy(srcID, body[12:12+srcIDLen])
	off := 12 + srcIDLen
	payloadLen := int(binary.LittleEndian.Uint32(body[off : off+4]))
	off += 4
	payload := make([]byte, payloadLen)
	copy(payload, body[off:off+payloadLen])
	return record.Record{Timestamp: ts, SourceID: srcID, Payload: payload}
}
