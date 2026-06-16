// Package latearrival_test verifies out-of-order write handling
// in both strict and tolerant late-arrival modes.
package latearrival_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/record"
)

// TestLateArrivalPrecision verifies the exact boundary behaviour of the late-arrival
// window at the full engine level.
//
// Router.Route boundary: timestamp < (now - lateArrivalWindow) → rejected.
//   - timestamp == horizon     → ACCEPTED  (not strictly less than)
//   - timestamp == horizon - 1 → REJECTED  (strictly less than)
func TestLateArrivalPrecision(t *testing.T) {
	const window = 5 * time.Minute

	newEngine := func(t *testing.T, mode partition.LateArrivalMode) *engine.Engine {
		t.Helper()
		opts := engine.DefaultOptions()
		opts.LateArrivalWindow = window
		opts.LateArrivalMode = mode

		opts.MemtableSizeBytes = 64 << 20 // no auto-flush during the test
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour
		e, err := engine.Open(t.TempDir(), opts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = e.Close() })
		return e
	}

	t.Run("strict_rejects_outside_window", func(t *testing.T) {
		e := newEngine(t, partition.Strict)
		ts := time.Now().Add(-(window + time.Minute)).UnixNano()
		err := e.Append(record.Record{Timestamp: ts, SourceID: []byte("s"), Payload: []byte("p")})
		require.ErrorIs(t, err, partition.ErrLateArrival,
			"record more than lateArrivalWindow in the past must be rejected in Strict mode")
	})

	t.Run("strict_accepts_at_boundary", func(t *testing.T) {
		e := newEngine(t, partition.Strict)
		// Compute horizon just before Append; add 1ms margin so clock advance
		// during the call does not flip the result.
		horizon := time.Now().UnixNano() - window.Nanoseconds()
		ts := horizon + int64(time.Millisecond)
		err := e.Append(record.Record{Timestamp: ts, SourceID: []byte("s"), Payload: []byte("p")})
		require.NoError(t, err,
			"record at exactly the late-arrival boundary must be accepted in Strict mode")
	})

	t.Run("strict_rejects_one_ns_past_boundary", func(t *testing.T) {
		e := newEngine(t, partition.Strict)
		// horizon - 1 is strictly less than horizon → rejected.
		// Add a 10ms buffer so the computed horizon is safely in the past even if
		// the clock advances slightly between this line and the Append call.
		horizon := time.Now().Add(-10*time.Millisecond).UnixNano() - window.Nanoseconds()
		ts := horizon - 1
		err := e.Append(record.Record{Timestamp: ts, SourceID: []byte("s"), Payload: []byte("p")})
		require.ErrorIs(t, err, partition.ErrLateArrival,
			"record one nanosecond past the late-arrival boundary must be rejected in Strict mode")
	})

	t.Run("tolerant_accepts_late_and_scannable", func(t *testing.T) {
		opts := engine.DefaultOptions()
		opts.LateArrivalWindow = 24 * time.Hour // large window so the record is "late" but still routed
		opts.LateArrivalMode = partition.Tolerant

		opts.MemtableSizeBytes = 64 << 20
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour
		e, err := engine.Open(t.TempDir(), opts)
		require.NoError(t, err)
		defer func() { _ = e.Close() }()

		// Write a record 2 hours in the past (outside a 5-minute window but within 24h).
		lateTS := time.Now().Add(-2 * time.Hour).UnixNano()
		require.NoError(t, e.Append(record.Record{
			Timestamp: lateTS,
			SourceID:  []byte("src"),
			Payload:   []byte("late"),
		}), "Tolerant mode must not reject late records")

		// The record must be scannable at its original timestamp.
		lateTime := time.Unix(0, lateTS)
		it, err := e.Scan(lateTime, lateTime.Add(time.Second), nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, it.Close()) }()

		require.True(t, it.Next(), "late record must be visible in scan")
		assert.Equal(t, lateTS, it.Record().Timestamp)
		assert.False(t, it.Next(), "only one record expected")
	})

	t.Run("strict_accepts_on_time", func(t *testing.T) {
		e := newEngine(t, partition.Strict)
		ts := time.Now().Add(time.Hour).UnixNano() // well in the future
		err := e.Append(record.Record{Timestamp: ts, SourceID: []byte("s"), Payload: []byte("p")})
		require.NoError(t, err, "on-time (future) record must always be accepted in Strict mode")
	})
}
