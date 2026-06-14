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

## Architecture

```mermaid
flowchart LR
    subgraph Write["Write path"]
        A(["Append(rec)"]) --> B["WAL - fsync"]
        B --> C["Memtable"]
        C -->|size limit| D[("SSTable")]
    end
    subgraph Read["Read path"]
        G(["Scan(t0, t1)"]) --> H["Router"]
        H -->|"ts-range skip"| I["mmap block"]
        I --> J["MergeIterator\nmin-heap"]
        J --> K(["RecordView\nzero-copy"])
    end
    subgraph BG["Background"]
        E["FIFO compactor\nn-to-1 per partition"]
        F["TTL sweeper\nexpired removed"]
    end
    D -.->|triggers| E
    D -.->|"MaxTS < horizon"| F
```

## Benchmarks

512-byte payload, sequential timestamps, single source. Intel Core i5-1334U, Go 1.26.
All engines use synchronous writes (`fsync` after every record) for a fair durability comparison.
TimberDB uses `CompressionZstd`; Badger and Pebble use their default snappy compression.
Medians across 5 benchmark runs.

**Append** (single record, fsync per write)

| Engine | ns/op | MB/s | B/op | allocs/op | Disk SA† |
|---|---|---|---|---|---|
| **TimberDB** | **2 837** | **180.47** | **3 558** | **4** | **0.011×** |
| Pebble | 6 204 | 82.53 | 32 | 0 | 0.23×‡ |
| Badger | 17 262 | 29.66 | 1 478 | 39 | 0.13×‡ |
| Bbolt | 30 495 | 16.79 | 28 604 | 103 | 3.13× |

```mermaid
xychart-beta
    title "Append latency in ns per op"
    x-axis [TimberDB, Pebble, Badger, bbolt]
    bar [2837, 6204, 17262, 30495]
```

**Scan** (1 000 records, 512 KB per iteration)

| Engine | ns/op | MB/s | B/op | allocs/op |
|---|---|---|---|---|
| Pebble | 63 301 | 8 088 | 16 | 1 |
| **TimberDB** | **498 343** | **1 027** | **660 984** | **25** |
| Badger | 1 065 390 | 481 | 100 679 | 1 332 |

bbolt is excluded from the scan comparison — it only supports full-bucket iteration, not efficient time-range scans.

```mermaid
xychart-beta
    title "Scan 1000 records latency in ns per op"
    x-axis [Pebble, TimberDB, Badger]
    bar [63301, 498343, 1065390]
```

**Reading the numbers**

- `ns/op` is the wall time per single operation (append or scan of 1000 records).
- `MB/s` is payload throughput: lower `ns/op` and higher `MB/s` are better.
- `allocs/op` reflects GC pressure; fewer allocations mean less GC pause.
- Scan benchmarks pre-load 1000 records before measuring; the per-iter `MB/s` reflects reading 512 KB of uncompressed payload per loop.

† **Disk SA** (storage amplification) = bytes on disk ÷ user bytes written, measured after engine close.
SA = 1.0× means the engine stores exactly as much as you wrote; SA < 1 means compression reduced the on-disk size below raw input.
‡ Badger and Pebble apply snappy compression by default. TimberDB uses zstd, which compresses the benchmark payload — 512 bytes of a single repeated character — at roughly 90:1, giving SA of 0.011×.
With incompressible data expect SA ≈ 1.04× for TimberDB (uncompressed), ≈ 1.1× for Badger, ≈ 1.0× for Pebble, and ≈ 3–5× for bbolt.

**Where timberdb wins**

Append throughput: timberdb writes at **2.2× the speed of Pebble**, **6.1× Badger**, and **10.7× Bbolt** with full `fsync` durability and zstd compression enabled.
The WAL fsync is the bottleneck; timberdb's partition-local sequential writes require exactly one fsync per record with no cross-level compaction amplification, and the zstd compression step runs only at memtable flush time — not on the hot write path — so it adds no latency to individual appends.

Storage efficiency: with `CompressionZstd`, timberdb stores this benchmark's compressible payload at **0.011× SA** — better than Badger (0.13×) or Pebble (0.23×), because zstd outcompresses snappy on uniform data.
With random, incompressible bytes, SA rises to ≈ 1.04×; WAL files are still removed after each memtable flush, so there is no multi-generational write amplification.

Scan over a bounded time range is **2.1× faster than Badger**, with GC pressure dominated by per-block decompression (25 allocs/op, 17 of which are block decompression buffers).

**Where timberdb trades off**

Pebble scan is **7.9× faster** (63 µs vs 498 µs) on this benchmark.
Two structural advantages drive this gap: Pebble's compressed scan reads only ~60 KB of compressed bytes from disk per 1 000 records, and its internal block cache amortises decompression across repeated scans so that most reads never pay the decompress cost at all.
TimberDB's current implementation allocates a fresh decompressed buffer for every data block on every scan; with 17 blocks covering 1 000 records, that costs ~645 KB/op (25 allocs/op vs 1 for Pebble).
Disabling compression (`CompressionNone`) cuts this to 8 allocs/op and ~160 µs/op — a 3× improvement — at the cost of 1.04× SA instead of 0.011×.
Point-key lookups are not a supported operation — timberdb is a range-scan store by design.

Reproduce: `go test -bench=. -benchmem ./test/bench/...`
