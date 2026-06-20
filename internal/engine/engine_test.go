package engine

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/record"
)

func openTestEngine(t *testing.T, opts Options) *Engine {
	t.Helper()
	e, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func drainIter(t *testing.T, it record.Iterator) []record.Record {
	t.Helper()
	defer func() { require.NoError(t, it.Close()) }()
	var out []record.Record
	for it.Next() {
		out = append(out, it.Record())
	}
	return out
}

func TestEngine(t *testing.T) {
	t.Run("append_scan_round_trip", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		const N = 1000
		for i := range N {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  fmt.Appendf(nil, "src-%d", i%5),
				Payload:   fmt.Appendf(nil, "p%d", i),
			}))
		}

		it, err := e.Scan(base, base.Add(time.Hour), nil)
		require.NoError(t, err)
		got := drainIter(t, it)

		require.Len(t, got, N)
		for i := 1; i < len(got); i++ {
			assert.LessOrEqual(t, got[i-1].Timestamp, got[i].Timestamp, "records must be sorted by timestamp")
		}
	})

	t.Run("range_query", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		var written []record.Record
		for h := range 3 {
			for i := range 100 {
				rec := record.Record{
					Timestamp: base.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   []byte("p"),
				}
				require.NoError(t, e.Append(rec))
				written = append(written, rec)
			}
		}

		// Scan the middle hour only.
		scanStart := base.Add(time.Hour)
		scanEnd := base.Add(2 * time.Hour)
		it, err := e.Scan(scanStart, scanEnd, nil)
		require.NoError(t, err)
		got := drainIter(t, it)

		startNs, endNs := scanStart.UnixNano(), scanEnd.UnixNano()
		var want []record.Record
		for _, r := range written {
			if r.Timestamp >= startNs && r.Timestamp < endNs {
				want = append(want, r)
			}
		}
		sort.Slice(want, func(i, j int) bool { return want[i].Timestamp < want[j].Timestamp })

		require.Len(t, got, len(want))
		for i, w := range want {
			assert.Equal(t, w.Timestamp, got[i].Timestamp)
		}
	})

	t.Run("concurrent_append", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		const (
			goroutines = 10
			perG       = 1000
		)
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Go(func() {
				for i := range perG {
					ts := base.Add(time.Duration(g*perG+i) * time.Millisecond).UnixNano()
					assert.NoError(t, e.Append(record.Record{
						Timestamp: ts,
						SourceID:  fmt.Appendf(nil, "src-%d", g),
						Payload:   []byte("p"),
					}))
				}
			})
		}
		wg.Wait()

		it, err := e.Scan(base, base.Add(2*time.Hour), nil)
		require.NoError(t, err)
		got := drainIter(t, it)
		assert.Len(t, got, goroutines*perG)
	})

	t.Run("flush_triggered", func(t *testing.T) {
		opts := DefaultOptions()
		opts.MemtableSizeBytes = 1 // flush after every append

		dir := t.TempDir()
		e, err := Open(dir, opts)
		require.NoError(t, err)

		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		for i := range 500 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   make([]byte, 1024),
			}))
		}
		require.NoError(t, e.Close())

		ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
		require.NoError(t, err)
		assert.NotEmpty(t, ssts, "flush must have produced at least one SSTable file")
	})
}

func TestEngineClosed(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, e.Close())

	t.Run("append_after_close", func(t *testing.T) {
		err := e.Append(record.Record{
			Timestamp: time.Now().UnixNano(),
			SourceID:  []byte("s"),
			Payload:   []byte("p"),
		})
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("scan_after_close", func(t *testing.T) {
		now := time.Now()
		it, err := e.Scan(now, now.Add(time.Hour), nil)
		require.ErrorIs(t, err, ErrClosed)
		assert.Nil(t, it)
	})
}

func TestMergeIteratorZeroAllocsPerRecord(t *testing.T) {
	const N = 100
	recs := make([]record.Record, N)
	for i := range recs {
		recs[i] = record.Record{
			Timestamp: int64(i),
			SourceID:  []byte("src"),
			Payload:   []byte("p"),
		}
	}
	half := N / 2

	allocs := testing.AllocsPerRun(50, func() {
		m := newMergeIterator([]record.Iterator{
			newStaticIter(recs[:half]),
			newStaticIter(recs[half:]),
		})
		for m.Next() {
			_ = m.View()
		}
		_ = m.Close()
	})
	assert.LessOrEqual(t, allocs, float64(10), "expected ≤ 10 total allocs for %d-record merge, got %.1f", N, allocs)
}

func TestMergeIteratorView(t *testing.T) {
	// Two iterators with interleaved timestamps.
	recs1 := []record.Record{
		{Timestamp: 10, SourceID: []byte("a"), Payload: []byte("p10")},
		{Timestamp: 30, SourceID: []byte("a"), Payload: []byte("p30")},
		{Timestamp: 50, SourceID: []byte("a"), Payload: []byte("p50")},
	}
	recs2 := []record.Record{
		{Timestamp: 20, SourceID: []byte("b"), Payload: []byte("p20")},
		{Timestamp: 40, SourceID: []byte("b"), Payload: []byte("p40")},
	}

	want := []record.Record{recs1[0], recs2[0], recs1[1], recs2[1], recs1[2]}

	iter1 := newStaticIter(recs1)
	iter2 := newStaticIter(recs2)
	m := newMergeIterator([]record.Iterator{iter1, iter2})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	var i int
	for m.Next() {
		view := m.View()
		assert.Equal(t, want[i].Timestamp, view.Timestamp, "view timestamp at %d", i)
		assert.Equal(t, want[i].SourceID, view.SourceID, "view SourceID at %d", i)
		assert.Equal(t, want[i].Payload, view.Payload, "view Payload at %d", i)

		rec := m.Record()
		assert.Equal(t, want[i].Timestamp, rec.Timestamp, "Record() timestamp at %d", i)
		assert.Equal(t, want[i].SourceID, rec.SourceID, "Record() SourceID at %d", i)
		assert.Equal(t, want[i].Payload, rec.Payload, "Record() Payload at %d", i)
		i++
	}
	require.NoError(t, m.Err())
	assert.Equal(t, len(want), i)
}

func TestEngineAggregate(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		// 120 records at 1/second spans exactly 2 minutes.
		for i := range 120 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		buckets, err := e.Aggregate(base, base.Add(2*time.Minute), AggregateOpts{
			BucketWidth: time.Minute,
			Fn:          AggCount,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 2)
		assert.Equal(t, int64(60), buckets[0].Count)
		assert.Equal(t, int64(60), buckets[1].Count)
		assert.Equal(t, base, buckets[0].Start)
		assert.Equal(t, base.Add(time.Minute), buckets[0].End)
		assert.Equal(t, base.Add(time.Minute), buckets[1].Start)
		assert.Equal(t, base.Add(2*time.Minute), buckets[1].End)
	})

	t.Run("rate", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		for i := range 120 {
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			}))
		}

		buckets, err := e.Aggregate(base, base.Add(2*time.Minute), AggregateOpts{
			BucketWidth: time.Minute,
			Fn:          AggRate,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 2)
		// 60 records / 60 seconds = 1.0 records/sec
		assert.InDelta(t, 1.0, buckets[0].Rate(), 0.01)
		assert.InDelta(t, 1.0, buckets[1].Rate(), 0.01)
	})

	t.Run("source_filter", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		// First 60 records from src-a (first minute), next 60 from src-b (second minute).
		for i := range 120 {
			src := []byte("src-a")
			if i >= 60 {
				src = []byte("src-b")
			}
			require.NoError(t, e.Append(record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  src,
				Payload:   []byte("p"),
			}))
		}

		buckets, err := e.Aggregate(base, base.Add(2*time.Minute), AggregateOpts{
			SourceID:    []byte("src-a"),
			BucketWidth: time.Minute,
			Fn:          AggCount,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 2)
		assert.Equal(t, int64(60), buckets[0].Count, "src-a is only in the first minute")
		assert.Equal(t, int64(0), buckets[1].Count)
	})

	t.Run("empty_range", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		buckets, err := e.Aggregate(base, base.Add(time.Minute), AggregateOpts{
			BucketWidth: time.Minute,
			Fn:          AggCount,
		})
		require.NoError(t, err)
		require.Len(t, buckets, 1)
		assert.Equal(t, int64(0), buckets[0].Count)
	})

	t.Run("invalid_bucket_width", func(t *testing.T) {
		e := openTestEngine(t, DefaultOptions())
		base := time.Now().Add(time.Hour).Truncate(time.Hour)

		_, err := e.Aggregate(base, base.Add(time.Minute), AggregateOpts{
			BucketWidth: 0,
			Fn:          AggCount,
		})
		require.ErrorIs(t, err, ErrInvalidBucketWidth)
	})
}

// staticIter is a test-only iterator over an in-memory slice.
type staticIter struct {
	recs []record.Record
	pos  int
}

func newStaticIter(recs []record.Record) *staticIter { return &staticIter{recs: recs, pos: -1} }
func (s *staticIter) Next() bool {
	s.pos++
	return s.pos < len(s.recs)
}
func (s *staticIter) Record() record.Record   { return s.recs[s.pos] }
func (s *staticIter) View() record.RecordView { return record.RecordView(s.recs[s.pos]) }
func (s *staticIter) Close() error            { return nil }
func (s *staticIter) Err() error              { return nil }
