package integration_test

import (
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
			e, err := engine.Open(dir, engine.DefaultOptions())
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
