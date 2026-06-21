# Getting Started

## Installation

```bash
go get github.com/ngaddam369/timberdb
```

Import the public API package in your code:

```go
import "github.com/ngaddam369/timberdb/pkg/engine"
```

All types referenced below live in this package. You do not need to import any `internal/` packages.

---

## Minimal example

The following program opens a database, appends three records from different sources, scans them back, and prints each one.

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/ngaddam369/timberdb/pkg/engine"
)

func main() {
    e, err := engine.Open("./mydb", engine.DefaultOptions())
    if err != nil {
        log.Fatal(err)
    }
    defer e.Close()

    now := time.Now()

    // Append three records from different sources
    records := []struct {
        source  string
        payload string
    }{
        {"app-server", `{"level":"info","msg":"request received"}`},
        {"db-proxy", `{"level":"info","msg":"query executed","ms":12}`},
        {"api-gateway", `{"level":"warn","msg":"rate limit approaching"}`},
    }
    for i, r := range records {
        err := e.Append(engine.Record{
            Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
            SourceID:  []byte(r.source),
            Payload:   []byte(r.payload),
        })
        if err != nil {
            log.Fatal(err)
        }
    }

    // Scan all records written in the last minute
    it, err := e.Scan(now.Add(-time.Minute), now.Add(time.Minute), nil)
    if err != nil {
        log.Fatal(err)
    }
    defer it.Close()

    for it.Next() {
        v := it.View()
        t := time.Unix(0, v.Timestamp)
        fmt.Printf("[%s] source=%-12s  %s\n",
            t.Format(time.RFC3339), string(v.SourceID), string(v.Payload))
    }
    if err := it.Err(); err != nil {
        log.Fatal(err)
    }
}
```

---

## API reference

### Opening and closing

```go
func Open(dir string, opts engine.Options) (*engine.Engine, error)
```

Opens or creates a timberdb store at `dir`. On restart, it replays the manifest to re-open all known SSTables, then replays WAL segments to reconstruct any unflushed memtable state. The directory is created if it does not exist.

```go
func DefaultOptions() engine.Options
```

Returns production-ready defaults. See [Configuration](configuration.md) for all available options.

```go
func (e *Engine) Close() error
```

Flushes all in-memory data to SSTables, waits for background goroutines to finish, and releases all file handles. Always call `Close()` — either `defer e.Close()` or explicitly before process exit.

---

### Appending records

```go
func (e *Engine) Append(rec engine.Record) error
```

Durably writes `rec` to the WAL, then routes it to the correct time partition. If the partition's memtable exceeds `MemtableSizeBytes`, a background flush to SSTable is triggered asynchronously.

**`engine.Record` fields:**

| Field | Type | Description |
|---|---|---|
| `Timestamp` | `int64` | Unix nanoseconds — the partition key |
| `SourceID` | `[]byte` | Identifies the source stream (e.g. hostname, service name) |
| `Payload` | `[]byte` | Opaque bytes; timberdb does not interpret these |

```go
err := e.Append(engine.Record{
    Timestamp: time.Now().UnixNano(),
    SourceID:  []byte("my-service"),
    Payload:   []byte(`{"event":"login","user":"alice"}`),
})
```

**Errors:**
- `engine.ErrClosed` — `Append` was called after `Close`
- `engine.ErrLateArrival` — the record's timestamp is older than `now - LateArrivalWindow` and `LateArrivalMode` is `LateArrivalReject` (the default)

---

### Scanning records

```go
func (e *Engine) Scan(start, end time.Time, opts *engine.ScanOptions) (engine.Iterator, error)
```

Returns a merged iterator over all records with timestamps in `[start, end)`, sorted by `(timestamp, sourceID)`. Pass `nil` for `opts` to return all sources.

**`engine.ScanOptions` fields:**

| Field | Type | Description |
|---|---|---|
| `SourceID` | `[]byte` | If non-nil, only records from this source are returned |

```go
// Scan all sources for the last hour
it, err := e.Scan(time.Now().Add(-time.Hour), time.Now(), nil)

// Scan a single source
it, err := e.Scan(start, end, &engine.ScanOptions{SourceID: []byte("my-service")})
```

**Iterator lifecycle:**

Always close the iterator, and always check `Err()` after the loop:

```go
it, err := e.Scan(start, end, nil)
if err != nil {
    return err
}
defer it.Close()

for it.Next() {
    v := it.View()         // zero-copy view, valid until the next Next() call
    _ = v.Timestamp
    _ = v.SourceID
    _ = v.Payload
}
return it.Err()            // nil on normal exhaustion, non-nil on read error
```

**`RecordView` vs `Record`:**

| | Allocation | Lifetime |
|---|---|---|
| `it.View()` → `RecordView` | None (zero-copy) | Valid until next `Next()` call |
| `it.Record()` → `Record` | Allocates owned copy | Lives until GC |
| `it.View().Clone()` → `Record` | Allocates owned copy | Lives until GC |

Use `View()` inside the loop for maximum throughput. Use `Record()` or `Clone()` if you need to store records after the loop exits or after calling `Next()` again.

---

### Aggregating records

```go
func (e *Engine) Aggregate(start, end time.Time, opts engine.AggregateOpts) ([]engine.Bucket, error)
```

Returns per-bucket record counts or rates over `[start, end)`. This is significantly faster than scanning all records and counting manually — for version-2 (columnar) SSTables with no source filter, only the timestamps column is read per block, skipping payload data entirely.

**`engine.AggregateOpts` fields:**

| Field | Type | Description |
|---|---|---|
| `BucketWidth` | `time.Duration` | Width of each bucket (must be > 0) |
| `Fn` | `engine.AggFn` | `engine.AggCount` or `engine.AggRate` |
| `SourceID` | `[]byte` | If non-nil, only records from this source are counted |

**`engine.Bucket` fields and methods:**

| | Type | Description |
|---|---|---|
| `Start` | `time.Time` | Inclusive bucket start |
| `End` | `time.Time` | Exclusive bucket end |
| `Count` | `int64` | Number of records in this bucket |
| `Rate()` | `float64` | Records per second (`Count / BucketWidth.Seconds()`) |

```go
// Count records per minute for the last hour
buckets, err := e.Aggregate(
    time.Now().Add(-time.Hour),
    time.Now(),
    engine.AggregateOpts{
        BucketWidth: time.Minute,
        Fn:          engine.AggCount,
    },
)
for _, b := range buckets {
    fmt.Printf("%s  count=%d  rate=%.2f/s\n",
        b.Start.Format(time.RFC3339), b.Count, b.Rate())
}
```

**Errors:**
- `engine.ErrInvalidBucketWidth` — `BucketWidth` is zero or negative

---

### Inspecting partitions

```go
func (e *Engine) Partitions() []engine.PartitionInfo
```

Returns a snapshot of all partitions currently tracked by the engine.

**`engine.PartitionInfo` fields:**

| Field | Type | Description |
|---|---|---|
| `Start` | `time.Time` | Partition window start (inclusive) |
| `End` | `time.Time` | Partition window end (exclusive) |
| `State` | `string` | `"open"`, `"sealed"`, or `"deleted"` |
| `SSTFiles` | `int` | Number of live SSTable files |
| `SizeBytes` | `int64` | Total on-disk size of SSTable files |

```go
for _, p := range e.Partitions() {
    fmt.Printf("[%s → %s]  state=%-8s  ssts=%d  size=%d bytes\n",
        p.Start.Format(time.RFC3339),
        p.End.Format(time.RFC3339),
        p.State, p.SSTFiles, p.SizeBytes)
}
```

---

## Error reference

| Error | Returned by | Meaning |
|---|---|---|
| `engine.ErrClosed` | `Append`, `Scan` | Called after `Close()` |
| `engine.ErrLateArrival` | `Append` | Record timestamp too far behind `now` |
| `engine.ErrInvalidBucketWidth` | `Aggregate` | `BucketWidth` is not positive |
