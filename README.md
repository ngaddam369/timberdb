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

```
BenchmarkTimberDBAppend-12      975861     6675 ns/op    76.71 MB/s    7023 B/op    13 allocs/op
BenchmarkBadgerAppend-12        228739    15457 ns/op    33.12 MB/s    3106 B/op    40 allocs/op
BenchmarkBboltAppend-12          91935    51115 ns/op    10.02 MB/s   30490 B/op   112 allocs/op
BenchmarkPebbleAppend-12        724254     5351 ns/op    95.68 MB/s      34 B/op     0 allocs/op
BenchmarkTimberDBScan-12          3408  1025570 ns/op   499.23 MB/s 1372148 B/op  4042 allocs/op
BenchmarkBadgerScan-12            3446  1008368 ns/op   507.75 MB/s   96465 B/op  1331 allocs/op
BenchmarkPebbleScan-12           54633    63332 ns/op  8084.34 MB/s      16 B/op     1 allocs/op
```

Reproduce: `go test -bench=. -benchmem ./test/bench/...`
