// Package engine is the top-level coordinator. It wires together the WAL,
// PartitionRouter, Memtable, SSTable flush, Manifest, Compaction, and Metrics
// into the public Append/Scan/Close API.
package engine
