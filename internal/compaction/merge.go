package compaction

import (
	"bytes"
	"fmt"
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
	cache *sstable.BlockCache,
) (*sstable.Reader, error) {
	inputMetas := make([]sstable.SSTableMeta, len(readers))
	for i, r := range readers {
		inputMetas[i] = r.Meta()
	}

	// drainAndCloseIters closes all iterators, logging but not masking errors.
	drainAndCloseIters := func(its []record.Iterator) {
		for _, it := range its {
			if cerr := it.Close(); cerr != nil {
				slog.Error("compaction: close iter on error", "err", cerr)
			}
		}
	}

	// Open a full-range scan iterator for each input reader.
	iters := make([]record.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.Scan(math.MinInt64, math.MaxInt64, nil)
		if err != nil {
			drainAndCloseIters(iters)
			return nil, fmt.Errorf("compaction: open scan iterator: %w", err)
		}
		iters = append(iters, it)
	}

	// Seed the min-heap with the first record from each non-empty iterator.
	h := &mergeHeap{}
	h.init()
	for _, it := range iters {
		if it.Next() {
			v := it.View()
			h.push(mergeItem{ts: v.Timestamp, sourceID: v.SourceID, iter: it})
		} else {
			if err := it.Err(); err != nil {
				drainAndCloseIters(iters)
				return nil, fmt.Errorf("compaction: seed iterator: %w", err)
			}
			if cerr := it.Close(); cerr != nil {
				slog.Error("compaction: close empty iter", "err", cerr)
			}
		}
	}

	drainHeap := func() {
		for len(*h) > 0 {
			item := h.pop()
			if cerr := item.iter.Close(); cerr != nil {
				slog.Error("compaction: close heap iter on error", "err", cerr)
			}
		}
	}

	w, err := sstable.NewWriter(outputPath, wopts)
	if err != nil {
		drainHeap()
		return nil, err
	}

	for len(*h) > 0 {
		item := h.pop()
		if err := w.Add(item.iter.View().Clone()); err != nil {
			if cerr := item.iter.Close(); cerr != nil {
				slog.Error("compaction: close iter on Add error", "err", cerr)
			}
			drainHeap()
			return nil, err
		}
		if item.iter.Next() {
			v := item.iter.View()
			h.push(mergeItem{ts: v.Timestamp, sourceID: v.SourceID, iter: item.iter})
		} else {
			if err := item.iter.Err(); err != nil {
				if cerr := item.iter.Close(); cerr != nil {
					slog.Error("compaction: close errored iter", "err", cerr)
				}
				drainHeap()
				return nil, fmt.Errorf("compaction: merge iterator: %w", err)
			}
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

	return sstable.NewReader(outputPath, cache)
}

// mergeItem is one slot in the k-way merge heap.
// Only ts and sourceID are stored for ordering; the full view is read via iter.View()
// after pop, before the iterator is advanced — keeping the heap free of byte-slice aliases.
type mergeItem struct {
	ts       int64
	sourceID []byte
	iter     record.Iterator
}

// mergeHeap is a min-heap of mergeItems ordered by (Timestamp, SourceID).
type mergeHeap []mergeItem

func (h mergeHeap) Less(i, j int) bool {
	if h[i].ts != h[j].ts {
		return h[i].ts < h[j].ts
	}
	return bytes.Compare(h[i].sourceID, h[j].sourceID) < 0
}

func (h *mergeHeap) push(item mergeItem) {
	*h = append(*h, item)
	h.up(len(*h) - 1)
}

func (h *mergeHeap) pop() mergeItem {
	n := len(*h) - 1
	(*h)[0], (*h)[n] = (*h)[n], (*h)[0]
	h.down(0, n)
	item := (*h)[n]
	*h = (*h)[:n]
	return item
}

func (h *mergeHeap) init() {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *mergeHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		j = i
	}
}

func (h *mergeHeap) down(i0, n int) {
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
