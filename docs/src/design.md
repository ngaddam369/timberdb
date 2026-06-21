# Design

This page explains the key design decisions in timberdb — what was chosen, what was deliberately left out, and why.

---

## Write amplification

Write amplification (WA) measures how many bytes are written to disk for each byte of user data. Lower is better.

**timberdb's WA story:**

Every record is written exactly twice under normal operation:
1. To the WAL (for crash safety)
2. To the SSTable during a memtable flush

After the flush, the WAL segment is deleted. There is no accumulation of WAL files. Because compaction is FIFO within each partition (N files → 1 file), there is no multi-level write amplification — a compacted partition writes each record one additional time, but this happens at most once per partition lifetime.

Base WA = 2× (WAL + flush). With FIFO compaction, total WA ≤ 3× in the worst case.

In practice, with `CompressionZstd`, the bytes written to SSTable are far smaller than the raw input. The benchmark payload — 512 bytes of a repeated character — achieves a **storage amplification of 0.011×** (90:1 compression), making the effective on-disk footprint much smaller than the data ingested.

---

## Partition lifecycle

Every partition passes through three states:

```
OPEN  →  SEALED  →  DELETED
```

- **OPEN** — the current partition for a given time window. Accepts new `Append` writes. Has an active memtable and possibly one or more SSTable files from previous flushes.
- **SEALED** — the partition's time window has closed (wall clock has advanced past its end). No new records can be written. Eligible for compaction and retention.
- **DELETED** — all SSTable files removed by the TTL sweeper. The partition no longer exists in the manifest.

Transitions are tracked in the manifest. On recovery, the manifest is replayed to determine which partitions exist and which files belong to each.

---

## Crash safety

timberdb's crash-safety invariant: **no record that was durably acknowledged (fsync'd via WAL) is ever lost, and no phantom data from an incomplete write can appear after recovery.**

This is enforced by three rules:

1. **WAL before data** — every record is written to the WAL (and fsync'd, with `SyncAlways`) before it reaches the memtable. If the process crashes before the SSTable flush, the WAL replay on startup reconstructs the in-memory state.

2. **Manifest before delete** — the old WAL segment is deleted only after the manifest durably records the new SSTable. If the process crashes mid-flush, the partial SSTable file (which is not yet in the manifest) is treated as an orphan and removed on startup. The WAL is replayed instead.

3. **Orphan cleanup on startup** — on `Open`, the engine reads the manifest, identifies all referenced files, and removes any SSTable or WAL file on disk that is not referenced. This prevents phantom data from appearing after a partial write that was not committed to the manifest.

---

## Why no bloom filter

Many LSM engines use bloom filters to skip SSTable files that don't contain a queried key.

timberdb doesn't use bloom filters because:

- **The router already skips whole partitions** — a scan of `[start, end)` never touches partitions outside that window. This is a coarser but usually sufficient skip.
- **The time index skips blocks within a file** — each SSTable has a time index with one entry per block. Binary search on the time index skips all blocks before `start` and stops at the first block past `end`. Bloom filters would only help if blocks that overlap the time range contained no relevant records, which is unusual in a time-ordered store.
- **Bloom filters add write overhead** — building a bloom filter per SSTable costs CPU and memory during flush. For a store where the time index already gives efficient range skipping, this cost is not justified.

---

## Why no tombstones and no Delete()

timberdb has no `Delete` operation and no tombstone mechanism.

In a general-purpose LSM engine, deleting a key requires writing a tombstone record that shadows the old value until compaction removes both. This adds complexity: tombstones must be tracked across compaction rounds, queries must filter them, and they complicate TTL semantics.

timberdb sidesteps all of this by being **append-only by design**:

- Log and event data is inherently immutable — you don't delete individual log lines.
- Bulk expiry is handled at partition granularity via `RetentionDuration` — when a partition's time window expires, all of its files are removed atomically. No tombstone management required.
- If selective data removal is needed, it must be done at the application level (e.g. by filtering payloads during a scan and rewriting to a new database).

---

## Why no Get()

timberdb has no `Get(key)` or point-lookup operation.

Supporting `Get` would require a per-key index (restart points in blocks, plus a bloom filter per file, plus a level-by-level scan for non-time-ordered data). This is exactly what general-purpose LSM engines like RocksDB and Pebble implement, and it is incompatible with timberdb's time-partitioned design where the only identity of a record is its `(timestamp, sourceID)` pair.

If you need point lookups, the right tool is a KV store. timberdb is optimized for range scans — `Scan(start, end)` is its primitive, not `Get`.

---

## Why sorted slice over skiplist for the Memtable

Many LSM engines use a skiplist for the memtable because skiplists support O(log n) insertions in any order.

timberdb uses a **sorted slice with binary-search insertion** instead, for these reasons:

- **Records arrive mostly in chronological order** — for real-time log streams, nearly every `Append` lands at the end of the slice. Binary search degenerates to O(1) in this case.
- **Cache efficiency** — a sorted slice is a contiguous array in memory. Iterating it for a flush scan is a single sequential pass with excellent cache locality. Skiplist iteration requires pointer chasing.
- **No per-node allocation** — the slice grows by amortized doubling. Skiplists allocate one node per record, adding GC pressure proportional to the number of in-memory records.

The worst case for binary-search insertion is O(n) per insert (when records arrive in reverse order). In practice this doesn't occur for time-ordered workloads.

---

## Why mmap for SSTable reads

SSTable block reads use `mmap` (on Linux/Unix) rather than `pread`/`ReadAt` for two reasons:

1. **No per-block syscall** — once the file is mapped, accessing a block is a memory read, not a syscall. For sequential scans that touch many blocks, this eliminates thousands of `read` calls per scan.
2. **Kernel-managed prefetch** — the `MADV_SEQUENTIAL` hint tells the kernel to read-ahead aggressively. The kernel's I/O scheduler is often better at batching disk reads than an application-level prefetch would be.

The mmap is implemented via `golang.org/x/sys/unix` — there is no CGo. On Windows, `mmap` falls back to `ReadAt` transparently.

---

## Why one WAL per engine (not per partition)

A single WAL is shared across all partitions:

- **Simpler rotation** — WAL rotation happens on each memtable flush. With per-partition WALs, you'd need to track which WAL segment belongs to which partition flush and coordinate deletion of each segment independently. A single WAL makes the rotation sequence straightforward: flush partition → rotate WAL → write SSTable → write manifest → delete old WAL.
- **Sequential write stream** — a single WAL file means all writes are sequentially appended to one file. Per-partition WALs would interleave writes across multiple files, making them less sequential on spinning disks and more complex to manage.

The tradeoff is that if the engine has many open partitions, a single WAL flush may cover records from several partitions. This is fine — on replay, the WAL router reconstructs each partition's memtable independently.

---

## Out of scope

The following capabilities are explicitly not part of timberdb's design:

| Feature | Why excluded |
|---|---|
| General key-value Get() | Requires a per-key index; conflicts with time-partition design |
| Float / numeric time-series | No columnar float storage, no downsampling or interpolation |
| Full-text search | Payloads are opaque bytes; no inverted index |
| Multi-node replication | Single-process embedded library only |
| SQL / query engine | No query planner, no filter pushdown |
| CGo | All code is pure Go; `golang.org/x/sys/unix` is used for mmap (pure Go, not CGo) |
