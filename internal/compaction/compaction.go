// Package compaction manages SSTable lifecycle.
// Two responsibilities:
//  1. Intra-partition compaction — merge multiple small SSTables within a partition.
//  2. TTL expiry — delete entire SSTable files older than the retention horizon (O(1), no tombstones).
package compaction
