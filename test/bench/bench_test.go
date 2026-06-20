// Package bench_test contains Go benchmarks for the engine and comparison databases.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem ./test/bench/...
//
// Run only append benchmarks:
//
//	go test -bench=Append -benchmem ./test/bench/...
package bench_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	badger "github.com/dgraph-io/badger/v4"
	bbolt "go.etcd.io/bbolt"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
)

// dirTotalBytes sums the sizes of all files under dir.
func dirTotalBytes(dir string) int64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// reportSA reports storage amplification: bytes on disk after the benchmark ÷ user bytes written.
// SA ≈ WA for engines that retain all intermediate files; SA < WA for engines that delete WAL
// segments after flush (timberdb, pebble). See README for interpretation.
func reportSA(b *testing.B, dir string) {
	b.Helper()
	userBytes := int64(b.N) * benchPayloadSize
	b.ReportMetric(float64(dirTotalBytes(dir))/float64(userBytes), "SA")
}

const (
	benchPayloadSize = 512
	benchScanN       = 1_000 // records pre-populated for scan benchmarks
)

var (
	benchPayload = bytes.Repeat([]byte("x"), benchPayloadSize)
	// benchBase is a fixed future timestamp so records are never rejected as
	// late arrivals, regardless of the engine's LateArrivalWindow setting.
	benchBase = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
)

// tsKey encodes a nanosecond timestamp as a big-endian 8-byte key so that
// lexicographic order matches chronological order.
func tsKey(ts int64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(ts))
	return k
}

// --- timberdb benchmarks ---

func BenchmarkTimberDBAppend(b *testing.B) {
	dir := b.TempDir()
	opts := engine.DefaultOptions()
	opts.MemtableSizeBytes = 64 << 20 // prevent auto-flush during benchmark
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour
	opts.CompressionType = sstable.CompressionZstd
	e, err := engine.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		if err := e.Append(record.Record{
			Timestamp: ts,
			SourceID:  []byte("bench"),
			Payload:   benchPayload,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// Close before SA so the engine flushes WAL→SST and releases pre-allocated space.
	if err := e.Close(); err != nil {
		b.Fatal(err)
	}
	reportSA(b, dir)
}

// timberdbScanAll drives a full scan of [start, end) and returns the record count.
func timberdbScanAll(b *testing.B, e *engine.Engine, start, end time.Time) int {
	b.Helper()
	it, err := e.Scan(start, end, nil)
	if err != nil {
		b.Fatal(err)
	}
	var n int
	for it.Next() {
		_ = it.View()
		n++
	}
	if err := it.Err(); err != nil {
		b.Fatal(err)
	}
	if err := it.Close(); err != nil {
		b.Fatal(err)
	}
	return n
}

func BenchmarkTimberDBScan(b *testing.B) {
	dir := b.TempDir()
	// Set up: write benchScanN records and flush them all to SST via Close.
	{
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 64 << 20
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour
		opts.CompressionType = sstable.CompressionZstd
		e, err := engine.Open(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		for i := range benchScanN {
			ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
			if err := e.Append(record.Record{
				Timestamp: ts,
				SourceID:  []byte("bench"),
				Payload:   benchPayload,
			}); err != nil {
				b.Fatal(err)
			}
		}
		// Close flushes remaining memtable to SST.
		if err := e.Close(); err != nil {
			b.Fatal(err)
		}
	}

	// Reopen for scan.
	opts := engine.DefaultOptions()
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour
	e, err := engine.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	scanEnd := benchBase.Add(time.Duration(benchScanN) * time.Millisecond)

	// Warm-up: pre-warm the block cache so the timed loop measures steady-state performance.
	timberdbScanAll(b, e, benchBase, scanEnd)

	b.SetBytes(int64(benchScanN) * benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := timberdbScanAll(b, e, benchBase, scanEnd); n != benchScanN {
			b.Fatalf("scan returned %d records, want %d", n, benchScanN)
		}
	}
}

func BenchmarkTimberDBConcurrentAppend(b *testing.B) {
	dir := b.TempDir()
	opts := engine.DefaultOptions()
	opts.MemtableSizeBytes = 64 << 20
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour
	opts.CompressionType = sstable.CompressionZstd
	e, err := engine.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}

	src := []byte("bench-concurrent")
	tsCounter := benchBase.UnixNano()
	b.SetBytes(benchPayloadSize)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ts := atomic.AddInt64(&tsCounter, 1)
			if err := e.Append(record.Record{
				Timestamp: ts,
				SourceID:  src,
				Payload:   benchPayload,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	if err := e.Close(); err != nil {
		b.Fatal(err)
	}
	reportSA(b, dir)
}

// --- badger benchmarks ---

func BenchmarkBadgerAppend(b *testing.B) {
	dir := b.TempDir()
	bopts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(true)
	db, err := badger.Open(bopts)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		key := tsKey(ts)
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, benchPayload)
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	reportSA(b, dir)
}

// badgerScanAll drives a full scan over [startKey, endKey) and returns the record count.
func badgerScanAll(b *testing.B, db *badger.DB, startKey, endKey []byte) int {
	b.Helper()
	var n int
	if err := db.View(func(txn *badger.Txn) error {
		iopts := badger.DefaultIteratorOptions
		iopts.PrefetchSize = 64
		it := txn.NewIterator(iopts)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), endKey) >= 0 {
				break
			}
			if err := it.Item().Value(func(v []byte) error {
				_ = v
				return nil
			}); err != nil {
				return err
			}
			n++
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return n
}

func BenchmarkBadgerScan(b *testing.B) {
	dir := b.TempDir()
	bopts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(true)
	db, err := badger.Open(bopts)
	if err != nil {
		b.Fatal(err)
	}

	for i := range benchScanN {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		key := tsKey(ts)
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, benchPayload)
		}); err != nil {
			b.Fatal(err)
		}
	}

	// Close and reopen to flush the MemTable to SSTables, matching TimberDB's state.
	if closeErr := db.Close(); closeErr != nil {
		b.Fatal(closeErr)
	}
	db, err = badger.Open(bopts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	startKey := tsKey(benchBase.UnixNano())
	endKey := tsKey(benchBase.Add(time.Duration(benchScanN) * time.Millisecond).UnixNano())

	// Warm-up: pre-warm internal caches before timing.
	badgerScanAll(b, db, startKey, endKey)

	b.SetBytes(int64(benchScanN) * benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := badgerScanAll(b, db, startKey, endKey); n != benchScanN {
			b.Fatalf("scan returned %d records, want %d", n, benchScanN)
		}
	}
}

// --- bbolt benchmark (append only; no scan since bbolt lacks efficient range queries) ---

func BenchmarkBboltAppend(b *testing.B) {
	dir := b.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "bbolt.db"), 0o600, nil)
	if err != nil {
		b.Fatal(err)
	}

	bucket := []byte("logs")
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(bucket)
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		key := tsKey(ts)
		if err := db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket(bucket).Put(key, benchPayload)
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	reportSA(b, dir)
}

// --- pebble benchmarks ---

func BenchmarkPebbleAppend(b *testing.B) {
	dir := b.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		key := tsKey(ts)
		if err := db.Set(key, benchPayload, pebble.Sync); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	reportSA(b, dir)
}

// pebbleScanAll drives a full scan over [startKey, endKey) and returns the record count.
func pebbleScanAll(b *testing.B, db *pebble.DB, startKey, endKey []byte) int {
	b.Helper()
	it, err := db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		b.Fatal(err)
	}
	var n int
	for valid := it.First(); valid; valid = it.Next() {
		_ = it.Value()
		n++
	}
	if err := it.Close(); err != nil {
		b.Fatal(err)
	}
	return n
}

func BenchmarkPebbleScan(b *testing.B) {
	dir := b.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for i := range benchScanN {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		key := tsKey(ts)
		if err := db.Set(key, benchPayload, pebble.Sync); err != nil {
			b.Fatal(err)
		}
	}

	// Flush MemTable to L0 SSTables so the scan measures on-disk performance.
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}

	startKey := tsKey(benchBase.UnixNano())
	endKey := tsKey(benchBase.Add(time.Duration(benchScanN) * time.Millisecond).UnixNano())

	// Warm-up: pre-warm Pebble's block cache before timing.
	pebbleScanAll(b, db, startKey, endKey)

	b.SetBytes(int64(benchScanN) * benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := pebbleScanAll(b, db, startKey, endKey); n != benchScanN {
			b.Fatalf("scan returned %d records, want %d", n, benchScanN)
		}
	}
}
