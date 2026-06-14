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
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	badger "github.com/dgraph-io/badger/v4"
	bbolt "go.etcd.io/bbolt"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
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
	opts.MetricsAddr = ""
	opts.MemtableSizeBytes = 64 << 20 // prevent auto-flush during benchmark
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour
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

func BenchmarkTimberDBScan(b *testing.B) {
	dir := b.TempDir()
	// Set up: write benchScanN records and flush them all to SST via Close.
	{
		opts := engine.DefaultOptions()
		opts.MetricsAddr = ""
		opts.MemtableSizeBytes = 64 << 20
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour
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
	opts.MetricsAddr = ""
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour
	e, err := engine.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	scanEnd := benchBase.Add(time.Duration(benchScanN) * time.Millisecond)

	b.SetBytes(int64(benchScanN) * benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := e.Scan(benchBase, scanEnd, nil)
		if err != nil {
			b.Fatal(err)
		}
		// View returns zero-copy slices into the block buffer — no per-record allocation.
		for it.Next() {
			_ = it.View()
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}
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

func BenchmarkBadgerScan(b *testing.B) {
	dir := b.TempDir()
	bopts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(true)
	db, err := badger.Open(bopts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for i := range benchScanN {
		ts := benchBase.Add(time.Duration(i) * time.Millisecond).UnixNano()
		key := tsKey(ts)
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, benchPayload)
		}); err != nil {
			b.Fatal(err)
		}
	}

	startKey := tsKey(benchBase.UnixNano())
	endKey := tsKey(benchBase.Add(time.Duration(benchScanN) * time.Millisecond).UnixNano())

	b.SetBytes(int64(benchScanN) * benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
			}
			return nil
		}); err != nil {
			b.Fatal(err)
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

	startKey := tsKey(benchBase.UnixNano())
	endKey := tsKey(benchBase.Add(time.Duration(benchScanN) * time.Millisecond).UnixNano())

	b.SetBytes(int64(benchScanN) * benchPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := db.NewIter(&pebble.IterOptions{
			LowerBound: startKey,
			UpperBound: endKey,
		})
		if err != nil {
			b.Fatal(err)
		}
		for valid := it.First(); valid; valid = it.Next() {
			_ = it.Value()
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
