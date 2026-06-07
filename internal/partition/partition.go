// Package partition manages time-window partitions.
// Each partition owns its memtable, flushed SSTables, and WAL segment.
// Partition lifecycle: OPEN → SEALED → DELETED.
package partition

import (
	"sync"
	"time"

	"github.com/ngaddam369/timberdb/internal/memtable"
	"github.com/ngaddam369/timberdb/internal/record"
)

// State represents the lifecycle state of a TimePartition.
type State int

const (
	StateOpen    State = iota // accepting writes; memtable active
	StateSealed               // no new writes; memtable flushed to SSTable
	StateDeleted              // all SSTables removed by the TTL enforcer
)

// PartitionWindow is the half-open time range [Start, End) that defines a partition boundary.
type PartitionWindow struct {
	Start int64 // Unix nanoseconds, inclusive
	End   int64 // Unix nanoseconds, exclusive
}

// Overlaps reports whether [start, end) shares any range with [w.Start, w.End).
func (w PartitionWindow) Overlaps(start, end int64) bool {
	return w.Start < end && w.End > start
}

// TimePartition owns all in-memory state for a single time window.
// All methods are safe for concurrent use.
type TimePartition struct {
	Window  PartitionWindow
	WALPath string
	mu      sync.RWMutex
	state   State
	mem     *memtable.Memtable
}

// NewPartition creates a new open partition for the given window.
func NewPartition(window PartitionWindow, walPath string) *TimePartition {
	return &TimePartition{
		Window:  window,
		WALPath: walPath,
		state:   StateOpen,
		mem:     memtable.New(),
	}
}

// Append writes r to the memtable. Returns ErrPartitionSealed if the partition
// is no longer accepting writes.
func (p *TimePartition) Append(r record.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != StateOpen {
		return ErrPartitionSealed
	}
	p.mem.Append(r)
	return nil
}

// Scan returns an Iterator over records in [start, end), optionally filtered by sourceID.
func (p *TimePartition) Scan(start, end int64, sourceID []byte) record.Iterator {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mem.Scan(start, end, sourceID)
}

// MemtableSize returns the approximate byte size of the active in-memory buffer.
// Used by the engine to decide when to trigger a flush.
func (p *TimePartition) MemtableSize() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mem.ApproximateSize()
}

// SwapMemtable atomically snapshots the current memtable and replaces it with a
// fresh empty one so the partition continues accepting writes after a flush.
func (p *TimePartition) SwapMemtable() record.Iterator {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := p.mem.Freeze()
	p.mem = memtable.New()
	return snap
}

// Seal transitions the partition from Open to Sealed, refusing further writes.
// A no-op if the partition is already sealed or deleted.
func (p *TimePartition) Seal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == StateOpen {
		p.state = StateSealed
	}
}

// MarkDeleted transitions a sealed partition to Deleted once all its SSTables are removed.
func (p *TimePartition) MarkDeleted() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = StateDeleted
}

// State returns the current lifecycle state of the partition.
func (p *TimePartition) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// IsSealable reports whether the wall clock has advanced past Window.End + lateArrivalWindow,
// meaning no further on-time writes can arrive for this partition.
func (p *TimePartition) IsSealable(now time.Time, lateArrivalWindow time.Duration) bool {
	return now.UnixNano() > p.Window.End+lateArrivalWindow.Nanoseconds()
}

// IsExpired reports whether the entire partition window is older than the retention horizon,
// making it eligible for bulk deletion by the TTL enforcer.
func (p *TimePartition) IsExpired(retentionHorizon int64) bool {
	return p.Window.End < retentionHorizon
}
