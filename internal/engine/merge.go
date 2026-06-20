package engine

import (
	"bytes"

	"github.com/ngaddam369/timberdb/internal/record"
)

// heapItem holds the timestamp and sourceID of the record currently at the head of
// iters[index], along with the iterator index. Only ts and sourceID are stored (not a
// full RecordView) so that the heap never holds byte slices into a scanIterator's
// decompression buffer across a block boundary.
// sourceID is a zero-copy slice that remains valid while the item sits in the heap,
// because the iterator is only advanced after the item is popped.
type heapItem struct {
	ts       int64
	sourceID []byte
	index    int
}

type minHeap []heapItem

func (h minHeap) Less(i, j int) bool {
	if h[i].ts != h[j].ts {
		return h[i].ts < h[j].ts
	}
	return bytes.Compare(h[i].sourceID, h[j].sourceID) < 0
}

func (h *minHeap) push(item heapItem) {
	*h = append(*h, item)
	h.up(len(*h) - 1)
}

func (h *minHeap) pop() heapItem {
	n := len(*h) - 1
	(*h)[0], (*h)[n] = (*h)[n], (*h)[0]
	h.down(0, n)
	item := (*h)[n]
	*h = (*h)[:n]
	return item
}

func (h *minHeap) init() {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *minHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		j = i
	}
}

func (h *minHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		i = j
	}
}

// mergeIterator merges N sorted record.Iterators into a single sorted stream.
// View() delegates directly to iters[curIdx] so no RecordView slices are held
// across Next calls, enabling decompBuf reuse in scanIterator.
type mergeIterator struct {
	iters  []record.Iterator
	h      minHeap
	curIdx int
	err    error
	closed bool
}

func newMergeIterator(iters []record.Iterator) *mergeIterator {
	m := &mergeIterator{iters: iters, curIdx: -1}
	m.h.init()
	for i, it := range iters {
		if it.Next() {
			v := it.View()
			m.h.push(heapItem{ts: v.Timestamp, sourceID: v.SourceID, index: i})
		} else if err := it.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
	return m
}

func (m *mergeIterator) Next() bool {
	if m.err != nil {
		return false
	}
	// Advance the previously-current iterator and enqueue its next record.
	if m.curIdx >= 0 {
		it := m.iters[m.curIdx]
		if it.Next() {
			v := it.View()
			m.h.push(heapItem{ts: v.Timestamp, sourceID: v.SourceID, index: m.curIdx})
		} else if err := it.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
	if len(m.h) == 0 {
		return false
	}
	m.curIdx = m.h.pop().index
	return true
}

func (m *mergeIterator) Record() record.Record {
	if m.curIdx < 0 {
		return record.Record{}
	}
	return m.iters[m.curIdx].View().Clone()
}

func (m *mergeIterator) View() record.RecordView {
	if m.curIdx < 0 {
		return record.RecordView{}
	}
	return m.iters[m.curIdx].View()
}

func (m *mergeIterator) Err() error { return m.err }

func (m *mergeIterator) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	var firstErr error
	for _, it := range m.iters {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
