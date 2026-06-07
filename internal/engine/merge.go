package engine

import (
	"bytes"
	"container/heap"

	"github.com/ngaddam369/timberdb/internal/record"
)

type heapItem struct {
	rec   record.Record
	index int
}

type minHeap []heapItem

func (h minHeap) Len() int      { return len(h) }
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h minHeap) Less(i, j int) bool {
	if h[i].rec.Timestamp != h[j].rec.Timestamp {
		return h[i].rec.Timestamp < h[j].rec.Timestamp
	}
	return bytes.Compare(h[i].rec.SourceID, h[j].rec.SourceID) < 0
}
func (h *minHeap) Push(x any) {
	item, ok := x.(heapItem)
	if !ok {
		panic("mergeIterator: pushed non-heapItem")
	}
	*h = append(*h, item)
}
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type mergeIterator struct {
	iters  []record.Iterator
	h      minHeap
	cur    record.Record
	closed bool
}

func newMergeIterator(iters []record.Iterator) *mergeIterator {
	m := &mergeIterator{iters: iters}
	heap.Init(&m.h)
	for i, it := range iters {
		if it.Next() {
			heap.Push(&m.h, heapItem{rec: it.Record(), index: i})
		}
	}
	return m
}

func (m *mergeIterator) Next() bool {
	if len(m.h) == 0 {
		return false
	}
	raw := heap.Pop(&m.h)
	item, ok := raw.(heapItem)
	if !ok {
		return false
	}
	m.cur = item.rec
	if m.iters[item.index].Next() {
		heap.Push(&m.h, heapItem{rec: m.iters[item.index].Record(), index: item.index})
	}
	return true
}

func (m *mergeIterator) Record() record.Record { return m.cur }

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
