// Package partition manages time-window partitions.
// Each partition owns its memtable, flushed SSTables, and WAL segment.
// Partition lifecycle: OPEN → SEALED → DELETED.
package partition
