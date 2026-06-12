# timberdb

Time-partitioned, TTL-native LSM storage engine for append-only time-ordered workloads.

```
Append(record)  →  WAL (fsync)  →  Memtable  →  SSTable flush
                                                        │
Scan(start,end) →  Router  →  SST skip-by-time  →  MergeIter
                                                        │
                             TTL sweeper  →  os.Remove (expired SSTs)
```

## Quick start

```bash
make build
./bin/timberdb append --db /tmp/db --source syslog --payload '{"msg":"hello"}'
./bin/timberdb scan   --db /tmp/db --start 2025-01-01T00:00:00Z --end 2025-01-02T00:00:00Z
```

## Benchmarks

512-byte payload, sequential timestamps, single source. Intel Core i5-1334U, Go 1.26.
All engines use synchronous writes (`fsync` after every record) for a fair durability comparison.

**Append** (single record, fsync per write)

| Engine | ns/op | MB/s | B/op | allocs/op |
|---|---|---|---|---|
| **TimberDB** | **2 257** | **226.83** | **4 999** | **9** |
| Badger | 9 132 | 56.07 | 3 956 | 40 |
| Bbolt | 23 763 | 21.55 | 29 346 | 107 |
| Pebble | 2 469 | 207.36 | 34 | 0 |

**Scan** (1 000 records, 512 KB per iteration)

| Engine | ns/op | MB/s | B/op | allocs/op |
|---|---|---|---|---|
| **TimberDB** | **436 065** | **1 174.14** | **1 372 150** | **4 042** |
| Badger | 419 267 | 1 221.18 | 97 180 | 1 331 |
| Pebble | 32 294 | 15 854.50 | 16 | 1 |

**Reading the numbers**

- `ns/op` is the wall time per single operation (append or scan of 1000 records).
- `MB/s` is payload throughput: lower `ns/op` and higher `MB/s` are better.
- `allocs/op` reflects GC pressure; fewer allocations mean less GC pause.
- Scan benchmarks pre-load 1000 records before measuring; the per-iter `MB/s` reflects reading 512 KB per loop.

**Where timberdb wins**

Append throughput: timberdb writes at **4× the speed of Badger** and **10× Bbolt** with full `fsync` durability.
The advantage comes from time-partitioned SST layout — WAL appends go directly to the active partition's file, avoiding the cross-level write amplification of general-purpose LSM trees.
Scan latency over a bounded time range is comparable to Badger because both skip files outside the query window using metadata.

**Where timberdb trades off**

Pebble scan is ~13× faster because it uses zero-copy block access and returns values by reference.
timberdb's scan allocates a full copy of every payload into `record.Record.Payload` (reflected in the 4042 allocs/op vs Pebble's 1).
Workloads that scan large ranges infrequently will see this allocation overhead.
Point-key lookups are not a supported operation — timberdb is a range-scan store by design.

Reproduce: `go test -bench=. -benchmem ./test/bench/...`
