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
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := range perG {
					ts := base.Add(time.Duration(g*perG+i) * time.Millisecond).UnixNano()
					assert.NoError(t, e.Append(record.Record{
						Timestamp: ts,
						SourceID:  fmt.Appendf(nil, "src-%d", g),
						Payload:   []byte("p"),
					}))
				}
			}(g)
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
