// Package metrics exposes Prometheus metrics for the timberdb engine.
// Key metric: sstable_skip_ratio (skips / (reads + skips)) — the time-range
// metadata equivalent of a bloom filter hit rate. Target: >95% for bounded queries.
package metrics
