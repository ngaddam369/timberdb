package integration_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
)

func openEngine(t *testing.T, dir string, opts engine.Options) *engine.Engine {
	t.Helper()
	e, err := engine.Open(dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func drainEngine(t *testing.T, e *engine.Engine, start, end time.Time) []record.Record {
	t.Helper()
	it, err := e.Scan(start, end, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	var out []record.Record
	for it.Next() {
		out = append(out, it.Record())
	}
	require.NoError(t, it.Err())
	return out
}

func TestEngineIntegration(t *testing.T) {
	t.Run("round_trip", func(t *testing.T) {
		e := openEngine(t, t.TempDir(), engine.DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		const (
			hours   = 3
			perHour = 100
		)
		for h := range hours {
			for i := range perHour {
				require.NoError(t, e.Append(record.Record{
					Timestamp: base.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   []byte("p"),
				}))
			}
		}

		got := drainEngine(t, e, base, base.Add(time.Duration(hours)*time.Hour))
		require.Len(t, got, hours*perHour)
		for i := 1; i < len(got); i++ {
			assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp, "records must be sorted")
		}
	})

	t.Run("reopen", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		const N = 200

		// Session 1: write and close.
		func() {
			opts := engine.DefaultOptions()
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			for i := range N {
				require.NoError(t, e.Append(record.Record{
					Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   []byte("p"),
				}))
			}
		}()

		// Session 2: reopen and scan — all records must survive.
		e2 := openEngine(t, dir, engine.DefaultOptions())
		got := drainEngine(t, e2, base, base.Add(time.Hour))
		assert.Len(t, got, N, "all records must be recoverable after reopen")
	})

	t.Run("flush_and_scan", func(t *testing.T) {
		opts := engine.DefaultOptions()
		opts.MemtableSizeBytes = 1 // force flush on every append

		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		const N = 300

		func() {
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			for i := range N {
				require.NoError(t, e.Append(record.Record{
					Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   make([]byte, 512),
				}))
			}
		}()

		// After close, reopen and verify all records are scannable from SSTables.
		e2 := openEngine(t, dir, engine.DefaultOptions())
		got := drainEngine(t, e2, base, base.Add(time.Hour))
		assert.Len(t, got, N, "all records must be scannable from SSTables after flush+reopen")
	})
}

// TestConcurrentAppendScan verifies that concurrent Append and Scan calls do not
// corrupt data or trigger the race detector.
func TestConcurrentAppendScan(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour
	e := openEngine(t, t.TempDir(), opts)

	// Use a well-future base so records never hit the late-arrival window.
	base := time.Now().Add(24 * time.Hour).Truncate(time.Hour)
	tsCounter := base.UnixNano()

	const appendN = 500
	var appended int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range appendN {
			ts := atomic.AddInt64(&tsCounter, 1)
			if err := e.Append(record.Record{
				Timestamp: ts,
				SourceID:  []byte("src"),
				Payload:   []byte("payload"),
			}); err != nil {
				return
			}
			atomic.AddInt64(&appended, 1)
		}
	}()

	scanEnd := base.Add(time.Hour)
	for range 20 {
		it, err := e.Scan(base, scanEnd, nil)
		require.NoError(t, err)
		for it.Next() {
			_ = it.View()
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
	}

	wg.Wait()

	got := drainEngine(t, e, base, scanEnd)
	assert.GreaterOrEqual(t, int64(len(got)), atomic.LoadInt64(&appended),
		"scan after appender finishes must see all appended records")
}

// TestScanAcrossPartitions verifies that a scan spanning multiple partition windows
// returns all records in timestamp order after a flush+reopen cycle.
func TestScanAcrossPartitions(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PartitionDuration = time.Minute
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour

	dir := t.TempDir()
	// Use a well-future base anchored to a minute boundary so partition windows
	// align predictably: [base, base+1m), [base+1m, base+2m), [base+2m, base+3m).
	base := time.Now().Add(24 * time.Hour).Truncate(time.Minute)

	var written []record.Record
	func() {
		e, err := engine.Open(dir, opts)
		require.NoError(t, err)
		defer func() { require.NoError(t, e.Close()) }()

		for partition := range 3 {
			for seq := range 10 {
				rec := record.Record{
					Timestamp: base.Add(time.Duration(partition)*time.Minute + time.Duration(seq)*time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   []byte("p"),
				}
				require.NoError(t, e.Append(rec))
				written = append(written, rec)
			}
		}
	}()

	// Reopen: all data is now in SSTables spanning 3 distinct partition windows.
	e2 := openEngine(t, dir, opts)
	got := drainEngine(t, e2, base, base.Add(3*time.Minute))

	require.Len(t, got, len(written), "all records across 3 partition windows must be returned")
	for i := 1; i < len(got); i++ {
		assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp, "records must be in timestamp order")
	}
}
