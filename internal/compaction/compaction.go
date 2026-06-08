// Package compaction manages SSTable lifecycle.
// Two responsibilities:
//  1. Intra-partition compaction — merge multiple small SSTables within a partition.
//  2. TTL expiry — delete entire SSTable files older than the retention horizon (O(1), no tombstones).
package compaction

import (
	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/sstable"
)

// MergeJob describes a compaction to perform: all Inputs are merged into a single SSTable.
type MergeJob struct {
	Window partition.PartitionWindow
	Inputs []sstable.SSTableMeta
}

// Strategy decides when to compact and which files have expired.
type Strategy interface {
	// Name identifies the strategy for logging and metrics.
	Name() string
	// PickMerge returns a MergeJob when the partition exceeds its file-count budget,
	// or nil if no merge is needed.
	PickMerge(win partition.PartitionWindow, files []sstable.SSTableMeta) *MergeJob
	// PickExpired returns the subset of files whose MaxTimestamp is strictly below horizon.
	PickExpired(files []sstable.SSTableMeta, horizon int64) []sstable.SSTableMeta
}
