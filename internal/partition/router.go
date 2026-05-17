package partition

import (
	"errors"
	"sync"
	"time"
)

// LateArrivalMode controls behaviour when a record's timestamp falls outside
// the configured late-arrival window.
type LateArrivalMode int

const (
	// Strict rejects late-arriving records with ErrLateArrival.
	Strict LateArrivalMode = iota
	// Tolerant routes late-arriving records to a dedicated late-arrival partition
	// rather than rejecting them.
	Tolerant
)

// ErrLateArrival is returned in Strict mode when a record's timestamp is outside
// the configured late-arrival window.
var ErrLateArrival = errors.New("timberdb: record timestamp is outside the late arrival window")

// ErrPartitionSealed is returned when a write targets a partition that is no longer open.
var ErrPartitionSealed = errors.New("timberdb: partition is sealed")

// Router maps incoming record timestamps to the correct TimePartition, creating new
// partitions as time advances. It also returns the overlapping partition set for range scans.
// All methods are safe for concurrent use.
type Router struct {
	mu                sync.RWMutex
	partitions        map[PartitionWindow]*TimePartition
	windowDuration    time.Duration
	lateArrivalWindow time.Duration
	lateArrivalMode   LateArrivalMode
	latePartition     *TimePartition // non-nil only in Tolerant mode
}

// NewRouter creates a Router with the given time-window duration and late-arrival policy.
func NewRouter(windowDuration, lateArrivalWindow time.Duration, mode LateArrivalMode) *Router {
	r := &Router{
		partitions:        make(map[PartitionWindow]*TimePartition),
		windowDuration:    windowDuration,
		lateArrivalWindow: lateArrivalWindow,
		lateArrivalMode:   mode,
	}
	if mode == Tolerant {
		r.latePartition = NewPartition(PartitionWindow{}, "late.wal")
	}
	return r
}

// Route returns the TimePartition for the given timestamp, creating it if necessary.
// Returns ErrLateArrival when the timestamp is outside the late-arrival window and
// the mode is Strict.
func (r *Router) Route(timestamp int64) (*TimePartition, error) {
	horizon := time.Now().UnixNano() - r.lateArrivalWindow.Nanoseconds()
	if timestamp < horizon {
		if r.lateArrivalMode == Strict {
			return nil, ErrLateArrival
		}
		return r.latePartition, nil
	}

	win := r.windowFor(timestamp)

	r.mu.RLock()
	p, ok := r.partitions[win]
	r.mu.RUnlock()
	if ok {
		return p, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok = r.partitions[win]; ok { // re-check after acquiring write lock
		return p, nil
	}
	p = NewPartition(win, "")
	r.partitions[win] = p
	return p, nil
}

// Overlapping returns all partitions whose window overlaps [start, end).
func (r *Router) Overlapping(start, end int64) []*TimePartition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*TimePartition
	for win, p := range r.partitions {
		if win.Overlaps(start, end) {
			out = append(out, p)
		}
	}
	return out
}

// SealExpired seals every open partition whose window has closed past the late-arrival window.
func (r *Router) SealExpired(now time.Time) {
	r.mu.RLock()
	var toSeal []*TimePartition
	for _, p := range r.partitions {
		if p.State() == StateOpen && p.IsSealable(now, r.lateArrivalWindow) {
			toSeal = append(toSeal, p)
		}
	}
	r.mu.RUnlock()
	for _, p := range toSeal {
		p.Seal()
	}
}

// All returns every partition tracked by this router. Used during startup and shutdown.
func (r *Router) All() []*TimePartition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*TimePartition, 0, len(r.partitions))
	for _, p := range r.partitions {
		out = append(out, p)
	}
	return out
}

// AddPartition registers an existing partition with the router. Used during WAL replay
// on startup to restore the partition map without re-routing individual records.
func (r *Router) AddPartition(p *TimePartition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitions[p.Window] = p
}

// windowFor computes the PartitionWindow that contains ts.
func (r *Router) windowFor(ts int64) PartitionWindow {
	dur := r.windowDuration.Nanoseconds()
	start := (ts / dur) * dur
	return PartitionWindow{Start: start, End: start + dur}
}
