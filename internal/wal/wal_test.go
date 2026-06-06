package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/record"
)

// fixture records used across several tests.
var fixtureRecords = []record.Record{
	{Timestamp: 1_000, SourceID: []byte("src-1"), Payload: []byte("payload-1")},
	{Timestamp: 2_000, SourceID: []byte("src-2"), Payload: []byte("payload-2")},
	{Timestamp: 3_000, SourceID: []byte("src-1"), Payload: []byte("payload-3")},
}

func writeAndClose(t *testing.T, path string, mode WALSyncMode, records []record.Record) {
	t.Helper()
	w, err := Open(path, mode)
	require.NoError(t, err)
	for _, r := range records {
		require.NoError(t, w.Append(r))
	}
	require.NoError(t, w.Close())
}

func replayAll(t *testing.T, path string) []record.Record {
	t.Helper()
	w, err := Open(path, SyncNever)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	var got []record.Record
	require.NoError(t, w.Replay(func(r record.Record) { got = append(got, r) }))
	return got
}

// TestWALAppendReplay verifies round-trip append→close→reopen→replay for all sync modes.
func TestWALAppendReplay(t *testing.T) {
	tests := []struct {
		name     string
		syncMode WALSyncMode
	}{
		{"SyncAlways", SyncAlways},
		{"SyncPeriodic", SyncPeriodic},
		{"SyncNever", SyncNever},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.wal")
			writeAndClose(t, path, tc.syncMode, fixtureRecords)

			got := replayAll(t, path)

			require.Len(t, got, len(fixtureRecords))
			for i, want := range fixtureRecords {
				assert.Equal(t, want.Timestamp, got[i].Timestamp, "record %d Timestamp", i)
				assert.Equal(t, want.SourceID, got[i].SourceID, "record %d SourceID", i)
				assert.Equal(t, want.Payload, got[i].Payload, "record %d Payload", i)
			}
		})
	}
}

// TestWALReplayPartialWrite verifies that a partial final record is silently truncated.
func TestWALReplayPartialWrite(t *testing.T) {
	tests := []struct {
		name        string
		truncateBy  int64
		wantRecords int
	}{
		{"truncate_1_byte", 1, 9_999},
		{"truncate_half_record", 20, 9_999},
		{"truncate_full_header", 8, 9_999},
	}

	const N = 10_000
	records := make([]record.Record, N)
	for i := range N {
		records[i] = record.Record{
			Timestamp: int64(i * 1_000),
			SourceID:  []byte("source-1"),
			Payload:   []byte("some payload data for record"),
		}
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "partial.wal")
			writeAndClose(t, path, SyncAlways, records)

			info, err := os.Stat(path)
			require.NoError(t, err)
			require.NoError(t, os.Truncate(path, info.Size()-tc.truncateBy))

			got := replayAll(t, path)

			require.Len(t, got, tc.wantRecords, "expected all complete records to survive")
			for i, r := range got {
				assert.Equal(t, records[i].Timestamp, r.Timestamp, "record %d Timestamp", i)
				assert.Equal(t, records[i].SourceID, r.SourceID, "record %d SourceID", i)
			}
		})
	}
}

// TestWALReplayEmptyFile verifies that an empty WAL replays zero records without error.
func TestWALReplayEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")

	w, err := Open(path, SyncAlways)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	var count int
	require.NoError(t, w.Replay(func(record.Record) { count++ }))
	assert.Equal(t, 0, count)
}

// TestWALReplayMalformedBodySilentStop verifies that a body with internally
// inconsistent field lengths stops replay gracefully — no panic, no error.
func TestWALReplayMalformedBodySilentStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	// Write one valid record.
	w, err := Open(path, SyncAlways)
	require.NoError(t, err)
	require.NoError(t, w.Append(record.Record{Timestamp: 42, SourceID: []byte("s"), Payload: []byte("p")}))
	require.NoError(t, w.Close())

	// Manually craft a record whose CRC is valid but whose srcIDLen overflows the body.
	// Body layout: Timestamp(8B) | srcIDLen(4B) | payloadLen(4B) = 16 bytes.
	// srcIDLen=9999 claims 9999 bytes of SourceID, but the body is only 16 bytes.
	body := make([]byte, 16)
	binary.LittleEndian.PutUint64(body[0:8], uint64(999))
	binary.LittleEndian.PutUint32(body[8:12], 9999) // overflows
	binary.LittleEndian.PutUint32(body[12:16], 0)

	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(body)))
	h := crc32.NewIEEE()
	h.Write(hdr[4:8])
	h.Write(body)
	binary.LittleEndian.PutUint32(hdr[0:4], h.Sum32())

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	_, err = f.Write(body)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Replay must return exactly 1 record (the valid one) without panicking.
	got := replayAll(t, path)
	require.Len(t, got, 1)
	assert.Equal(t, int64(42), got[0].Timestamp)
}

// TestWALRotate verifies that after Rotate each file contains only its own records.
func TestWALRotate(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "wal-1.wal")
	path2 := filepath.Join(dir, "wal-2.wal")

	w, err := Open(path1, SyncAlways)
	require.NoError(t, err)

	require.NoError(t, w.Append(record.Record{Timestamp: 1_000, SourceID: []byte("src"), Payload: []byte("before")}))
	require.NoError(t, w.Rotate(path2))
	require.NoError(t, w.Append(record.Record{Timestamp: 2_000, SourceID: []byte("src"), Payload: []byte("after")}))
	require.NoError(t, w.Close())

	tests := []struct {
		path    string
		wantTS  int64
		wantLen int
	}{
		{path1, 1_000, 1},
		{path2, 2_000, 1},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			got := replayAll(t, tc.path)
			require.Len(t, got, tc.wantLen)
			assert.Equal(t, tc.wantTS, got[0].Timestamp)
		})
	}
}
