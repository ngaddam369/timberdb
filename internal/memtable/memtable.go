// Package memtable implements an in-memory sorted buffer for a single time partition.
// Records are stored in a sorted slice (not a skiplist) — within a partition window
// records arrive mostly in order, making sorted-slice insertion cache-friendly and cheap.
package memtable
