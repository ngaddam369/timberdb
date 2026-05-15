// Package wal implements the write-ahead log for crash durability.
// Every write lands in the WAL before the memtable. On crash and
// restart, the WAL is replayed to rebuild all active partition memtables.
//
// Record format (binary, sequential):
//
//	CRC32(4B) | Len(4B) | Timestamp(8B) | SrcIDLen(4B) | SourceID | PayloadLen(4B) | Payload
//
// WAL files are named: wal-{partition_window}-{seq}.wal
package wal
