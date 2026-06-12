// Package manifest maintains a durable, append-only log of VersionEdit records.
// It tracks which SSTable files exist, their partition and time-range metadata,
// and reconstructs full engine state on startup.
package manifest

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// maxEditBodySize is a sanity cap when reading from a potentially corrupt manifest.
const maxEditBodySize = 64 * 1024 * 1024 // 64 MB

// VersionEdit describes one atomic change to the set of live SSTable files.
// An edit is durable only after it has been fsynced to the manifest.
// The crash-safe protocol is: write SSTables → append VersionEdit (fsync) → delete old files.
type VersionEdit struct {
	Seq          uint64      `json:"seq"`
	AddedFiles   []FileEntry `json:"added,omitempty"`
	DeletedFiles []FileEntry `json:"deleted,omitempty"`
}

// FileEntry is the manifest's durable record of one SSTable file.
type FileEntry struct {
	Path           string `json:"path"`
	PartitionStart int64  `json:"partition_start"`
	PartitionEnd   int64  `json:"partition_end"`
	MinTimestamp   int64  `json:"min_ts"`
	MaxTimestamp   int64  `json:"max_ts"`
	RecordCount    uint64 `json:"record_count"`
}

// Record format (binary header + JSON body):
//
//	CRC32(4B) | Len(4B) | JSON(Len bytes)
//
// CRC covers Len(4B) + JSON body — same pattern as the WAL.

// Manifest is an append-only log of VersionEdit records.
// All methods are safe for concurrent use.
type Manifest struct {
	mu  sync.Mutex
	f   *os.File
	bw  *bufio.Writer
	seq uint64
}

// Open opens or creates the manifest at path.
// Call Replay immediately after Open to rebuild state from any prior run,
// before calling Append.
func Open(path string) (*Manifest, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Manifest{
		f:  f,
		bw: bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// Append encodes edit and appends it to the manifest, then fsyncs.
// The Seq field is set automatically; callers do not need to populate it.
func (m *Manifest) Append(edit VersionEdit) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := m.seq + 1
	edit.Seq = next

	body, err := json.Marshal(edit)
	if err != nil {
		return err
	}

	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(body)))
	h := crc32.NewIEEE()
	h.Write(hdr[4:8])
	h.Write(body)
	binary.LittleEndian.PutUint32(hdr[0:4], h.Sum32())

	if _, err := m.bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := m.bw.Write(body); err != nil {
		return err
	}
	if err := m.bw.Flush(); err != nil {
		return err
	}
	if err := m.f.Sync(); err != nil {
		return err
	}
	m.seq = next // advance only after successful fsync
	return nil
}

// Replay reads all complete VersionEdits from the manifest and calls fn for each,
// in written order. A partial last record (CRC mismatch or unexpected EOF) is
// silently truncated — the expected result of a crash mid-write.
// Replay must be called before any Append calls; it sets the internal sequence
// counter so subsequent edits continue the sequence correctly.
func (m *Manifest) Replay(fn func(VersionEdit)) (retErr error) {
	m.mu.Lock()
	if err := m.bw.Flush(); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	f, err := os.Open(m.f.Name())
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = cerr
		}
	}()

	br := bufio.NewReader(f)
	var maxSeq uint64
	var validBytes int64
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		bodyLen := int(binary.LittleEndian.Uint32(hdr[4:8]))
		if bodyLen > maxEditBodySize {
			break // corrupt length field
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(br, body); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		h := crc32.NewIEEE()
		h.Write(hdr[4:8])
		h.Write(body)
		if h.Sum32() != binary.LittleEndian.Uint32(hdr[0:4]) {
			break // CRC mismatch — partial last record
		}

		var edit VersionEdit
		if err := json.Unmarshal(body, &edit); err != nil {
			break // corrupt JSON body
		}

		fn(edit)
		if edit.Seq > maxSeq {
			maxSeq = edit.Seq
		}
		validBytes += int64(8 + bodyLen)
	}

	m.mu.Lock()
	m.seq = maxSeq
	// Truncate any corrupt tail so the next Append starts at the right offset.
	if err := m.f.Truncate(validBytes); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()
	return nil
}

// Close flushes, fsyncs, and closes the manifest file.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.bw.Flush(); err != nil {
		return err
	}
	if err := m.f.Sync(); err != nil {
		return err
	}
	return m.f.Close()
}
