package partition_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/record"
)

// collectAll drains an iterator into a slice and closes it.
func collectAll(t *testing.T, it record.Iterator) []record.Record {
	t.Helper()
	defer it.Close()
	var out []record.Record
	for it.Next() {
		out = append(out, it.Record())
	}
	return out
}

// ── PartitionWindow ──────────────────────────────────────────────────────────

func TestPartitionWindowContains(t *testing.T) {
	win := partition.PartitionWindow{Start: 100, End: 200}
	tests := []struct {
		name string
		ts   int64
		want bool
	}{
		{"at_start", 100, true},
		{"mid", 150, true},
		{"one_before_end", 199, true},
		{"at_end_exclusive", 200, false},
		{"before_start", 99, false},
		{"well_after_end", 300, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, win.Start <= tc.ts && tc.ts < win.End)
		})
	}
}

func TestPartitionWindowOverlaps(t *testing.T) {
	win := partition.PartitionWindow{Start: 100, End: 200}
	tests := []struct {
		name       string
		start, end int64
		want       bool
	}{
		{"fully_inside", 120, 180, true},
		{"overlaps_start", 50, 150, true},
		{"overlaps_end", 150, 250, true},
		{"fully_covers", 50, 250, true},
		{"adjacent_before", 0, 100, false},
		{"adjacent_after", 200, 300, false},
		{"no_overlap_before", 0, 99, false},
		{"no_overlap_after", 201, 300, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, win.Overlaps(tc.start, tc.end))
		})
	}
}

// ── TimePartition lifecycle ──────────────────────────────────────────────────

func TestPartitionAppendAndSeal(t *testing.T) {
	win := partition.PartitionWindow{Start: 0, End: 1_000}
	p := partition.NewPartition(win, "")
	r := record.Record{Timestamp: 500, SourceID: []byte("s"), Payload: []byte("p")}

	t.Run("accepts_write_when_open", func(t *testing.T) {
		require.NoError(t, p.Append(r))
		assert.Equal(t, partition.StateOpen, p.State())
	})

	t.Run("rejects_write_after_seal", func(t *testing.T) {
		p.Seal()
		assert.Equal(t, partition.StateSealed, p.State())
		assert.ErrorIs(t, p.Append(r), partition.ErrPartitionSealed)
	})

	t.Run("seal_is_idempotent", func(t *testing.T) {
		p.Seal()
		assert.Equal(t, partition.StateSealed, p.State())
	})

	t.Run("deleted_after_mark_deleted", func(t *testing.T) {
		p.MarkDeleted()
		assert.Equal(t, partition.StateDeleted, p.State())
		assert.ErrorIs(t, p.Append(r), partition.ErrPartitionSealed)
	})
}

func TestPartitionScan(t *testing.T) {
	win := partition.PartitionWindow{Start: 0, End: 1_000}
	p := partition.NewPartition(win, "")
	for i := range 10 {
		require.NoError(t, p.Append(record.Record{
			Timestamp: int64(i * 100),
			SourceID:  []byte("s"),
			Payload:   []byte("p"),
		}))
	}

	tests := []struct {
		name      string
		start     int64
		end       int64
		wantCount int
	}{
		{"full_range", 0, 1_000, 10},
		{"mid_range", 200, 500, 3}, // ts 200, 300, 400
		{"exact_start", 0, 100, 1}, // ts 0 only
		{"empty_range", 500, 500, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := collectAll(t, p.Scan(tc.start, tc.end, nil))
			assert.Len(t, got, tc.wantCount)
		})
	}
}

func TestPartitionIsSealable(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name              string
		windowEndOffset   time.Duration // window end relative to now
		lateArrivalWindow time.Duration
		want              bool
	}{
		{"sealable: past late arrival", -2 * time.Second, time.Second, true},
		{"not sealable: within late arrival", -30 * time.Second, time.Minute, false},
		{"not sealable: window still open", time.Hour, 5 * time.Minute, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			end := now.Add(tc.windowEndOffset).UnixNano()
			win := partition.PartitionWindow{Start: end - time.Hour.Nanoseconds(), End: end}
			p := partition.NewPartition(win, "")
			assert.Equal(t, tc.want, p.IsSealable(now, tc.lateArrivalWindow))
		})
	}
}

func TestPartitionIsExpired(t *testing.T) {
	win := partition.PartitionWindow{Start: 0, End: 1_000}
	p := partition.NewPartition(win, "")
	tests := []struct {
		name             string
		retentionHorizon int64
		want             bool
	}{
		{"horizon_past_window_end", 1_001, true},
		{"horizon_at_window_end", 1_000, false}, // IsExpired: End < horizon, not <=
		{"horizon_before_window_end", 500, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, p.IsExpired(tc.retentionHorizon))
		})
	}
}

// ── Router ───────────────────────────────────────────────────────────────────

func TestRouterWindowAssignment(t *testing.T) {
	r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)
	// Start one full hour ahead so all 24 timestamps are always in the future,
	// regardless of when in the current hour this test runs.
	base := time.Now().Add(time.Hour).Truncate(time.Hour)

	seen := make(map[partition.PartitionWindow]bool)
	for h := range 24 {
		ts := base.Add(time.Duration(h)*time.Hour + 30*time.Minute).UnixNano()
		p, err := r.Route(ts)
		require.NoError(t, err, "hour %d", h)
		assert.True(t, p.Window.Start <= ts && ts < p.Window.End, "timestamp must fall within returned partition window")
		seen[p.Window] = true
	}
	assert.Len(t, seen, 24, "24 distinct 1-hour partitions expected")
}

func TestRouterRouteStability(t *testing.T) {
	r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)
	ts := time.Now().Add(time.Hour).UnixNano() // safely in the future

	p1, err := r.Route(ts)
	require.NoError(t, err)
	p2, err := r.Route(ts)
	require.NoError(t, err)

	assert.Same(t, p1, p2, "same timestamp must always return the same partition pointer")
}

func TestRouterLateArrival(t *testing.T) {
	lateTS := time.Now().Add(-2 * time.Hour).UnixNano() // well outside any reasonable window

	t.Run("strict_rejects", func(t *testing.T) {
		r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)
		_, err := r.Route(lateTS)
		assert.ErrorIs(t, err, partition.ErrLateArrival)
	})

	t.Run("tolerant_accepts", func(t *testing.T) {
		r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Tolerant)
		p, err := r.Route(lateTS)
		require.NoError(t, err)
		require.NotNil(t, p, "tolerant mode must return a late-arrival partition")
	})

	t.Run("tolerant_same_partition_for_all_late", func(t *testing.T) {
		r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Tolerant)
		p1, _ := r.Route(lateTS)
		p2, _ := r.Route(time.Now().Add(-3 * time.Hour).UnixNano())
		assert.Same(t, p1, p2, "all late arrivals must go to the same dedicated partition")
	})
}

func TestRouterOverlapping(t *testing.T) {
	r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)
	base := time.Now().Truncate(time.Hour)

	// Create partitions for hours +10, +11, +12.
	for h := 10; h <= 12; h++ {
		ts := base.Add(time.Duration(h)*time.Hour + 30*time.Minute).UnixNano()
		_, err := r.Route(ts)
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		startOffset time.Duration
		endOffset   time.Duration
		wantCount   int
	}{
		// [+10.5h, +11.5h) → overlaps hour-10 and hour-11 partitions
		{"two_partitions", 10*time.Hour + 30*time.Minute, 11*time.Hour + 30*time.Minute, 2},
		// [+10h, +13h) → covers all three partitions
		{"all_three", 10 * time.Hour, 13 * time.Hour, 3},
		// [+9h, +10h) → no partition created there
		{"none", 9 * time.Hour, 10 * time.Hour, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := base.Add(tc.startOffset).UnixNano()
			end := base.Add(tc.endOffset).UnixNano()
			got := r.Overlapping(start, end)
			assert.Len(t, got, tc.wantCount)
		})
	}
}

func TestRouterSealExpired(t *testing.T) {
	r := partition.NewRouter(time.Hour, 5*time.Minute, partition.Strict)
	now := time.Now()

	// Inject a partition whose window closed 2 hours ago — past the 5-min late-arrival window.
	oldWin := partition.PartitionWindow{
		Start: now.Add(-3 * time.Hour).UnixNano(),
		End:   now.Add(-2 * time.Hour).UnixNano(),
	}
	old := partition.NewPartition(oldWin, "")
	r.AddPartition(old)

	// Inject a partition whose window closed 3 minutes ago — still within late-arrival window.
	recentWin := partition.PartitionWindow{
		Start: now.Add(-4 * time.Minute).UnixNano(),
		End:   now.Add(-3 * time.Minute).UnixNano(),
	}
	recent := partition.NewPartition(recentWin, "")
	r.AddPartition(recent)

	r.SealExpired(now)

	assert.Equal(t, partition.StateSealed, old.State(), "old partition must be sealed")
	assert.Equal(t, partition.StateOpen, recent.State(), "recent partition is still within late-arrival window")
}
