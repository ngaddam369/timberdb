# Configuration

All configuration is passed to `engine.Open` via an `engine.Options` struct. Start from `engine.DefaultOptions()` and override only what you need:

```go
opts := engine.DefaultOptions()
opts.CompressionType = engine.CompressionZstd
opts.RetentionDuration = 7 * 24 * time.Hour
e, err := engine.Open("./mydb", opts)
```

---

## Full options reference

### PartitionDuration

```go
PartitionDuration time.Duration  // default: 1h
```

The time window covered by each partition. Records whose timestamps fall within the same window are stored together. Smaller windows give finer retention granularity but create more files; larger windows reduce file count but mean retention can only delete whole partitions.

**Guidance:** The default 1-hour window works well for most workloads. Use 24h for low-volume data where hourly cleanup granularity is not needed, or 5–15 min for very high-throughput workloads where you want faster TTL precision.

---

### LateArrivalWindow

```go
LateArrivalWindow time.Duration  // default: 5m
```

How far behind wall clock a record's timestamp may be before it is considered a late arrival. A record with `Timestamp < now - LateArrivalWindow` is either rejected or routed to a late partition depending on `LateArrivalMode`.

**Guidance:** Keep this ≥ the maximum expected clock skew or buffer delay in your pipeline. For real-time log shipping, the default 5 minutes is usually sufficient.

---

### LateArrivalMode

```go
LateArrivalMode engine.LateArrivalMode  // default: LateArrivalReject
```

Controls what happens when a record's timestamp falls outside the `LateArrivalWindow`.

| Value | Behaviour |
|---|---|
| `LateArrivalReject` | Returns `ErrLateArrival` to the caller (default) |
| `LateArrivalAccept` | Silently routes the record to a dedicated late-arrival partition |

**Guidance:** Use `LateArrivalReject` when late records indicate a bug in the producer. Use `LateArrivalAccept` when clock skew or buffering is expected and you still want the data.

---

### MemtableSizeBytes

```go
MemtableSizeBytes int64  // default: 64 MiB
```

The approximate memtable size that triggers a background flush to SSTable. Larger values batch more records per flush (better compression, fewer files) at the cost of more memory and potential data loss on unclean shutdown since the WAL segment is removed only after a successful flush.

**Guidance:** 64 MiB is a good default. Increase for workloads with large payloads or low flush frequency. Decrease (e.g. 4 MiB) when you need tighter durability windows or when running with `SyncPeriodic`.

---

### WALSyncMode

```go
WALSyncMode engine.WALSyncMode  // default: SyncAlways
```

Controls when the WAL is fsynced to disk.

| Value | Behaviour | Durability | Throughput |
|---|---|---|---|
| `SyncAlways` | fsync after every `Append` | Strongest: no data loss on crash | Lowest |
| `SyncPeriodic` | fsync on a 200ms background ticker | Up to 200ms of data loss on crash | High |
| `SyncNever` | Let the OS decide | OS-dependent; data may be lost on power failure | Highest |

**Guidance:** Use `SyncAlways` for any workload where data loss is unacceptable. Use `SyncPeriodic` for high-throughput logging where losing up to 200ms of events is acceptable. `SyncNever` is only appropriate for scratch or cache-like data.

---

### BlockSizeBytes

```go
BlockSizeBytes int  // default: 32 KiB
```

The target uncompressed size for SSTable data blocks. Larger blocks mean fewer index entries (better compression ratio) but higher read amplification when only a few records are needed. Smaller blocks improve point-in-time access at the cost of a larger time index.

**Guidance:** The default 32 KiB is appropriate for most payloads. Increase to 64–128 KiB for workloads with large payloads and sequential access patterns.

---

### IndexSources

```go
IndexSources bool  // default: false
```

When true, an additional per-source index is written inside each SSTable. This index allows source-filtered scans (`ScanOptions.SourceID` set) to skip entire blocks that contain no records from the target source.

**Guidance:** Enable when your workload frequently scans a single source from a partition that contains many sources. The write overhead is small but nonzero — only enable it if source-filtered scan performance matters.

---

### MaxFilesPerPartition

```go
MaxFilesPerPartition int  // default: 10
```

The number of SSTable files in a single partition that triggers background compaction. Each flush creates one SSTable; compaction merges N files into 1. Lower values keep the file count tighter (fewer open file descriptors, better scan amplification) at the cost of more compaction I/O.

**Guidance:** The default 10 is conservative. Increase to 20–50 for write-heavy workloads where compaction overhead matters; lower to 3–5 for read-heavy workloads where scan amplification is critical.

---

### RetentionDuration

```go
RetentionDuration time.Duration  // default: 0 (disabled)
```

The maximum age of data before it is eligible for deletion. When the oldest timestamp in a partition is older than `now - RetentionDuration`, the entire partition (all its SSTable files) is deleted. A value of 0 disables retention entirely.

```go
opts.RetentionDuration = 30 * 24 * time.Hour  // keep 30 days
```

**Guidance:** Set this whenever disk space is bounded. Retention granularity is one full partition (`PartitionDuration`), so the actual data deleted may be up to `PartitionDuration` newer than the cutoff.

---

### RetentionCheckInterval

```go
RetentionCheckInterval time.Duration  // default: 1h
```

How often the retention sweeper runs in the background. Lower values give tighter enforcement of `RetentionDuration` at the cost of slightly more frequent I/O.

**Guidance:** 1h is appropriate for daily-scale retention. If `RetentionDuration` is set to minutes or hours, lower this proportionally.

---

### CompactionCheckInterval

```go
CompactionCheckInterval time.Duration  // default: 30s
```

How often the compactor background sweeps all partitions for merge eligibility. Compaction is also triggered immediately after every memtable flush via an internal signal, so this interval only catches partitions that didn't compact immediately after flush.

---

### CompressionType

```go
CompressionType engine.CompressionType  // default: CompressionNone
```

The block compression algorithm applied when writing new SSTables.

| Value | Algorithm | Best for |
|---|---|---|
| `CompressionNone` | No compression | CPU-constrained writes; already-compressed payloads |
| `CompressionZstd` | Zstandard | Best compression ratio; recommended for text/JSON |
| `CompressionSnappy` | Snappy | Faster compression at a lower ratio |

Compression runs at flush time, not on the hot write path — it does not add latency to individual `Append` calls.

> **Note:** `CompressionType` affects whether the block cache is used. Only compressed blocks (v1/v2 SSTables) are cached. If `CompressionNone` is set, `BlockCacheBytes` has no effect.

---

### ColumnOriented

```go
ColumnOriented bool  // default: false
```

When true, SSTables are written in a columnar v2 format that stores timestamps, source IDs, and payloads in separate sections of each block. This enables `Aggregate()` to read only the timestamps column per block, skipping all payload data — dramatically reducing I/O for count and rate aggregations.

> **Note:** `ColumnOriented` and `CompressionType` work together. Setting both writes columnar blocks that are also compressed.

**Guidance:** Enable when your workload makes frequent `Aggregate()` calls on large time ranges. The write format is backward-compatible — existing v0/v1 files are still readable after enabling this option.

---

### BlockCacheBytes

```go
BlockCacheBytes int64  // default: 64 MiB
```

The maximum number of bytes of decompressed SSTable block data held in an LRU cache shared across all open SSTable readers for this engine instance. When a block is read for the first time, it is decompressed and stored in the cache; subsequent reads of the same block are served from cache without decompressing again.

Setting `BlockCacheBytes = 0` disables the cache entirely.

> **Note:** Only compressed blocks (from SSTables written with `CompressionZstd` or `CompressionSnappy`) are cached. Uncompressed blocks are not cached regardless of this setting.

**Guidance:** The default 64 MiB works well for most workloads. Increase for workloads with large working sets and repeated scans of the same time range. Set to 0 only when memory is extremely constrained and most scans are one-time reads.

---

## Defaults summary

| Option | Default |
|---|---|
| `PartitionDuration` | 1h |
| `LateArrivalWindow` | 5m |
| `LateArrivalMode` | `LateArrivalReject` |
| `MemtableSizeBytes` | 64 MiB |
| `WALSyncMode` | `SyncAlways` |
| `BlockSizeBytes` | 32 KiB |
| `IndexSources` | false |
| `MaxFilesPerPartition` | 10 |
| `RetentionDuration` | 0 (disabled) |
| `RetentionCheckInterval` | 1h |
| `CompactionCheckInterval` | 30s |
| `CompressionType` | `CompressionNone` |
| `ColumnOriented` | false |
| `BlockCacheBytes` | 64 MiB |
