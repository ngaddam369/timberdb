# Metrics

timberdb exposes Prometheus-format metrics through an `http.Handler`. The engine does **not** start an HTTP server — callers mount the handler on their own mux.

## Mounting the handler

```go
e, err := engine.Open("./mydb", engine.DefaultOptions())
if err != nil {
    log.Fatal(err)
}

mux := http.NewServeMux()
mux.Handle("/metrics", e.Metrics().Handler())

log.Fatal(http.ListenAndServe(":9090", mux))
```

The metrics registry is **isolated per engine instance** — it does not write to the global `prometheus.DefaultRegisterer`. Multiple engine instances in the same process are safe and will not produce metric name conflicts.

### Gathering metrics programmatically

```go
families, err := e.Metrics().Gather()
```

`Gather()` returns `[]*dto.MetricFamily` — the same data that Prometheus scrapes, but as Go structs. Useful for testing or custom exporters.

---

## Counters

Counters only go up. They reset to zero on process restart.

| Metric | Help |
|---|---|
| `timberdb_appends_total` | Total number of records appended. |
| `timberdb_append_bytes_total` | Total bytes written by Append (timestamp + sourceID + payload). |
| `timberdb_late_arrivals_total` | Total records rejected because they arrived outside the late-arrival window (strict mode). |
| `timberdb_wal_writes_total` | Total records written to the WAL. |
| `timberdb_memtable_flushes_total` | Total number of memtable-to-SSTable flushes completed. |
| `timberdb_scans_total` | Total number of Scan calls. |
| `timberdb_scan_records_total` | Total records yielded across all scans. |
| `timberdb_sstable_reads_total` | Total SSTable readers opened for a Scan (time-range overlapped). |
| `timberdb_sstable_skips_total` | Total SSTable readers skipped by time-range metadata pre-filter. |
| `timberdb_compactions_total` | Total number of compaction runs. |
| `timberdb_files_expired_total` | Total SSTable files deleted by the retention enforcer. |
| `timberdb_bytes_reclaimed_total` | Total bytes freed by the retention enforcer. |
| `timberdb_bytes_flushed_total` | Total bytes written to SSTables during memtable flushes. |
| `timberdb_bytes_compacted_total` | Total bytes written to SSTables during compaction merges. |

No labels on any counter.

---

## Gauges

Gauges reflect the current state and can go up or down.

| Metric | Help |
|---|---|
| `timberdb_active_partitions` | Current number of partitions tracked by the router. |
| `timberdb_sstable_files_total` | Current number of live SSTable readers. |
| `timberdb_sstable_bytes_total` | Current total bytes of live SSTable files. |
| `timberdb_retention_horizon_timestamp` | Unix nanoseconds of the current retention cutoff. 0 when retention is disabled. |

---

## Histograms

Histograms use the default Prometheus buckets (`.005`, `.01`, `.025`, `.05`, `.1`, `.25`, `.5`, `1`, `2.5`, `5`, `10` seconds).

| Metric | Help |
|---|---|
| `timberdb_scan_duration_seconds` | Time from Scan() call to iterator Close(), in seconds. |
| `timberdb_compaction_duration_seconds` | Time to complete a single compaction merge, in seconds. |

---

## Key metrics to watch

### SSTable skip ratio

```
timberdb_sstable_skips_total / (timberdb_sstable_reads_total + timberdb_sstable_skips_total)
```

This is the fraction of SSTable readers that were skipped entirely by the time-range pre-filter — roughly analogous to a bloom filter hit rate. For bounded time-range queries, this should be **> 95%**.

A low skip ratio (e.g. < 50%) means most scans touch most SSTable files. Possible causes:
- Queries with very wide time ranges spanning many partitions
- Many small partitions accumulating over a long retention window
- `PartitionDuration` set too large, causing many SSTs per partition

### Late arrivals

```
timberdb_late_arrivals_total
```

In `LateArrivalReject` mode (the default), any value > 0 means a producer sent a record with a timestamp older than `now - LateArrivalWindow`. Check your producer for clock skew, queue depth, or buffering delays. Consider widening `LateArrivalWindow` or switching to `LateArrivalAccept`.

### Compaction lag

```
timberdb_sstable_files_total / timberdb_active_partitions
```

The average number of SSTable files per partition. If this consistently exceeds `MaxFilesPerPartition`, compaction is not keeping up with flush rate. Check `timberdb_compaction_duration_seconds` for unusually slow compactions.

### Retention horizon

```
timberdb_retention_horizon_timestamp
```

The Unix nanoseconds of the current retention cutoff. Convert with `time.Unix(0, value)` to confirm retention is advancing as expected. A value of 0 means retention is disabled.

---

## Prometheus scrape configuration

```yaml
scrape_configs:
  - job_name: timberdb
    static_configs:
      - targets: ['localhost:9090']
    metrics_path: /metrics
```
