package compaction

import (
	"bytes"
	"container/heap"
	"errors"
	"log/slog"
	"math"
	"os"

	"github.com/ngaddam369/timberdb/internal/manifest"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
)

// Merge performs a k-way merge of readers into a new SSTable at outputPath.
// On success the manifest is updated atomically (AddedFiles: output, DeletedFiles:
// inputs) and the input files are deleted from disk. The caller retains ownership
// of the input readers and must close them after Merge returns.
//
// Crash safety: if the process dies before the manifest write, the output file is
// an orphan and input files remain live (unchanged manifest). If it dies after the
// manifest write but before the input deletes, both old and new files exist on disk;
// startup orphan detection removes the files listed as deleted in the manifest.
func Merge(
	readers []*sstable.Reader,
	outputPath string,
	wopts sstable.WriterOptions,
	m *manifest.Manifest,
) (*sstable.Reader, error) {
	inputMetas := make([]sstable.SSTableMeta, len(readers))
	for i, r := range readers {
		inputMetas[i] = r.Meta()
	}

	// Open a full-range scan iterator for each input reader.
	iters := make([]record.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.Scan(math.MinInt64, math.MaxInt64, nil)
		if err != nil {
			for _, open := range iters {
				if cerr := open.Close(); cerr != nil {
					slog.Error("compaction: close iter on scan error", "err", cerr)
				}
			}
			return nil, err
		}
		iters = append(iters, it)
	}

	// Seed the min-heap with the first record from each non-empty iterator.
	h := &mergeHeap{}
	heap.Init(h)
	for _, it := range iters {
		if it.Next() {
			heap.Push(h, mergeItem{rec: it.Record(), iter: it})
		} else {
			if cerr := it.Close(); cerr != nil {
				slog.Error("compaction: close empty iter", "err", cerr)
			}
		}
	}

	drainHeap := func() {
		for h.Len() > 0 {
			raw := heap.Pop(h)
			item, ok := raw.(mergeItem)
			if ok {
				if cerr := item.iter.Close(); cerr != nil {
					slog.Error("compaction: close heap iter on error", "err", cerr)
				}
			}
		}
	}

	w, err := sstable.NewWriter(outputPath, wopts)
	if err != nil {
		drainHeap()
		return nil, err
	}

	for h.Len() > 0 {
		raw := heap.Pop(h)
		item, ok := raw.(mergeItem)
		if !ok {
			drainHeap()
			return nil, errors.New("compaction: unexpected heap item type")
		}
		if err := w.Add(item.rec); err != nil {
			if cerr := item.iter.Close(); cerr != nil {
				slog.Error("compaction: close iter on Add error", "err", cerr)
			}
			drainHeap()
			return nil, err
		}
		if item.iter.Next() {
			heap.Push(h, mergeItem{rec: item.iter.Record(), iter: item.iter})
		} else {
			if cerr := item.iter.Close(); cerr != nil {
				slog.Error("compaction: close exhausted iter", "err", cerr)
			}
		}
	}

	meta, err := w.Finish()
	if err != nil {
		return nil, err
	}

	// Build the atomic manifest edit: add the merged file, remove all inputs.
	deletedFiles := make([]manifest.FileEntry, len(inputMetas))
	for i, im := range inputMetas {
		deletedFiles[i] = manifest.FileEntry{
			Path:           im.Path,
			PartitionStart: im.PartitionStart,
			PartitionEnd:   im.PartitionEnd,
			MinTimestamp:   im.MinTimestamp,
			MaxTimestamp:   im.MaxTimestamp,
			RecordCount:    im.RecordCount,
		}
	}
	if err := m.Append(manifest.VersionEdit{
		AddedFiles: []manifest.FileEntry{{
			Path:           meta.Path,
			PartitionStart: meta.PartitionStart,
			PartitionEnd:   meta.PartitionEnd,
			MinTimestamp:   meta.MinTimestamp,
			MaxTimestamp:   meta.MaxTimestamp,
			RecordCount:    meta.RecordCount,
		}},
		DeletedFiles: deletedFiles,
	}); err != nil {
		return nil, err
	}

	// Manifest is durable — safe to remove input files.
	for _, im := range inputMetas {
		if err := os.Remove(im.Path); err != nil && !os.IsNotExist(err) {
			slog.Error("compaction: remove input file", "path", im.Path, "err", err)
		}
	}

	return sstable.NewReader(outputPath)
}

// mergeItem is one slot in the k-way merge heap.
type mergeItem struct {
	rec  record.Record
	iter record.Iterator
}

// mergeHeap is a min-heap of mergeItems ordered by (Timestamp, SourceID).
type mergeHeap []mergeItem

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	a, b := h[i].rec, h[j].rec
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	return bytes.Compare(a.SourceID, b.SourceID) < 0
}

func (h *mergeHeap) Push(x any) {
	item, ok := x.(mergeItem)
	if !ok {
		panic("compaction: mergeHeap.Push received non-mergeItem")
	}
	*h = append(*h, item)
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
