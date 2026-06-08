package compaction

import (
	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/sstable"
)

// FIFOStrategy implements Strategy with a first-in-first-out policy.
// It merges all files in a partition when the count exceeds MaxFilesPerPartition,
// and expires files whose MaxTimestamp predates the retention horizon.
// FIFO is the correct primary strategy for time-partitioned append-only data:
// oldest data expires first as a unit, so near-1× write amplification is achievable.
type FIFOStrategy struct {
	MaxFilesPerPartition int
}

// Name returns the strategy identifier.
func (f *FIFOStrategy) Name() string { return "fifo" }

// PickMerge returns a MergeJob covering all files when len(files) exceeds
// MaxFilesPerPartition, or nil if no merge is warranted.
func (f *FIFOStrategy) PickMerge(win partition.PartitionWindow, files []sstable.SSTableMeta) *MergeJob {
	if len(files) <= f.MaxFilesPerPartition {
		return nil
	}
	inputs := make([]sstable.SSTableMeta, len(files))
	copy(inputs, files)
	return &MergeJob{Window: win, Inputs: inputs}
}

// PickExpired returns all files whose MaxTimestamp is strictly less than horizon.
func (f *FIFOStrategy) PickExpired(files []sstable.SSTableMeta, horizon int64) []sstable.SSTableMeta {
	var expired []sstable.SSTableMeta
	for _, meta := range files {
		if meta.MaxTimestamp < horizon {
			expired = append(expired, meta)
		}
	}
	return expired
}
