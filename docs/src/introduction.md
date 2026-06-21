# Introduction

timberdb is a time-partitioned, TTL-native LSM storage engine for append-only, mostly-chronological workloads.

```
Append(record)  →  WAL (fsync)  →  Memtable  →  SSTable flush
                                                        │
Scan(start,end) →  Router  →  SST skip-by-time  →  MergeIterator
                                                        │
                             TTL sweeper  →  os.Remove (expired SSTs)
```

## What it is

timberdb stores **timestamped records** — each record is a triple of `(timestamp int64, sourceID []byte, payload []byte)`. Records are written in chronological order, stored in time-partitioned SSTables, and read back through range scans. The engine handles compression, block caching, background compaction, and TTL-based expiry transparently.

| Property | Value |
|---|---|
| Module | `github.com/ngaddam369/timberdb` |
| Language | Go 1.26+ |
| Storage format | LSM (WAL → Memtable → SSTable) |
| Partitioning | Time-windowed (default: 1-hour partitions) |
| Compression | None / Zstd / Snappy (configurable) |
| Durability | WAL with configurable fsync policy |

## Good fit for

- **System logs** — syslog, application logs, structured log streams
- **Sensor / telemetry streams** — IoT readings, metrics samples, trace spans
- **Audit trails** — immutable append-only event histories
- **Any workload where records arrive mostly in time order** and are queried by time range

## Not designed for

- **General key-value storage** — there is no `Get(key)` operation; lookups require a time-range scan
- **Numeric time-series** — no float columns, downsampling, interpolation, or moving averages
- **Full-text search** — payloads are opaque bytes; timberdb does not index their contents
- **Distributed / multi-node** — single-process embedded library only; no replication or consensus
- **Query engine** — no SQL, no filter pushdown, no aggregation beyond count and rate

## Quick start

```bash
go get github.com/ngaddam369/timberdb
```

See [Getting Started](getting-started.md) for a runnable example.
