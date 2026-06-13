package engine

import (
	"bytes"

	"github.com/ngaddam369/timberdb/internal/record"
)

type heapItem struct {
	view  record.RecordView
	index int
}

type minHeap []heapItem

func (h minHeap) Less(i, j int) bool {
	if h[i].view.Timestamp != h[j].view.Timestamp {
		return h[i].view.Timestamp < h[j].view.Timestamp
	}
	return bytes.Compare(h[i].view.SourceID, h[j].view.SourceID) < 0
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

type mergeIterator struct {
	iters  []record.Iterator
	h      minHeap
	cur    record.RecordView
	err    error
	closed bool
}

func newMergeIterator(iters []record.Iterator) *mergeIterator {
	m := &mergeIterator{iters: iters}
	m.h.init()
	for i, it := range iters {
		if it.Next() {
			m.h.push(heapItem{view: it.View(), index: i})
		} else if err := it.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
	return m
}

func (m *mergeIterator) Next() bool {
	if m.err != nil || len(m.h) == 0 {
		return false
	}
	item := m.h.pop()
	m.cur = item.view
	it := m.iters[item.index]
	if it.Next() {
		m.h.push(heapItem{view: it.View(), index: item.index})
	} else if err := it.Err(); err != nil && m.err == nil {
		m.err = err
	}
	return true
}

func (m *mergeIterator) Record() record.Record   { return m.cur.Clone() }
func (m *mergeIterator) View() record.RecordView { return m.cur }
func (m *mergeIterator) Err() error              { return m.err }

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
