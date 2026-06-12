// Package memtable implements an in-memory sorted buffer for a single time partition.
// Records are stored in a sorted slice (not a skiplist) — within a partition window
// records arrive mostly in order, making sorted-slice insertion cache-friendly and cheap.
package memtable

import (
	"bytes"
	"math"
	"sort"
	"sync"

	"github.com/ngaddam369/timberdb/internal/record"
)

// Memtable is an in-memory sorted buffer for a single time partition.
// All methods are safe for concurrent use.
type Memtable struct {
	mu        sync.RWMutex
	records   []record.Record // sorted by (Timestamp, SourceID)
	sizeBytes int64
}

// New returns an empty Memtable.
func New() *Memtable {
	return &Memtable{}
}

// Append inserts r into the sorted buffer, maintaining (Timestamp, SourceID) ascending order
// via binary-search insertion.
func (m *Memtable) Append(r record.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := insertionPoint(m.records, r)
	m.records = append(m.records, record.Record{})
	copy(m.records[i+1:], m.records[i:])
	m.records[i] = r
	m.sizeBytes += recordSize(r)
}

// snapshot returns a point-in-time copy of m.records under RLock.
func (m *Memtable) snapshot() []record.Record {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := make([]record.Record, len(m.records))
	copy(s, m.records)
	return s
}

// Scan returns an Iterator over records with Timestamp in [start, end), optionally
// filtered to sourceID (nil means all sources). The snapshot is isolated from
// subsequent Append calls.
func (m *Memtable) Scan(start, end int64, sourceID []byte) record.Iterator {
	snapshot := m.snapshot()
	startIdx := sort.Search(len(snapshot), func(i int) bool {
		return snapshot[i].Timestamp >= start
	})
	return &iterator{
		records:  snapshot,
		pos:      startIdx - 1,
		end:      end,
		sourceID: sourceID,
	}
}

// ApproximateSize returns the cumulative byte size of all records
// (8B timestamp + len(SourceID) + len(Payload)). Used as a flush trigger.
func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

// Freeze returns a sorted Iterator over a point-in-time snapshot of the buffer.
// The snapshot is isolated from subsequent Append calls and is used during SSTable flush.
func (m *Memtable) Freeze() record.Iterator {
	return &iterator{records: m.snapshot(), pos: -1, end: math.MaxInt64}
}

// iterator is a forward-only cursor over a sorted record slice.
type iterator struct {
	records  []record.Record
	pos      int
	end      int64
	sourceID []byte
	current  record.Record
}

func (it *iterator) Next() bool {
	for {
		it.pos++
		if it.pos >= len(it.records) {
			return false
		}
		r := it.records[it.pos]
		if r.Timestamp >= it.end {
			return false
		}
		if it.sourceID != nil && !bytes.Equal(r.SourceID, it.sourceID) {
			continue
		}
		it.current = r
		return true
	}
}

func (it *iterator) Record() record.Record { return it.current }
func (it *iterator) Close() error          { return nil }
func (it *iterator) Err() error            { return nil }

// insertionPoint returns the index at which r should be inserted to maintain
// (Timestamp, SourceID) ascending order.
func insertionPoint(records []record.Record, r record.Record) int {
	return sort.Search(len(records), func(i int) bool {
		if records[i].Timestamp != r.Timestamp {
			return records[i].Timestamp > r.Timestamp
		}
		return bytes.Compare(records[i].SourceID, r.SourceID) >= 0
	})
}

// recordSize returns the byte contribution of a single record toward ApproximateSize.
func recordSize(r record.Record) int64 {
	return int64(8 + len(r.SourceID) + len(r.Payload))
}
