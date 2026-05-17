package memtable_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/memtable"
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

// isSorted returns true if records are in non-decreasing (Timestamp, SourceID) order.
func isSorted(records []record.Record) bool {
	for i := 1; i < len(records); i++ {
		a, b := records[i-1], records[i]
		if a.Timestamp > b.Timestamp {
			return false
		}
		if a.Timestamp == b.Timestamp && string(a.SourceID) > string(b.SourceID) {
			return false
		}
	}
	return true
}

// TestSortedInsertion verifies that records are iterated in (Timestamp, SourceID)
// ascending order after insertion. Tests both the common case (mostly in-order arrivals,
// which is what the sorted-slice design is optimised for) and the edge case of fully
// random timestamps within a small batch.
func TestSortedInsertion(t *testing.T) {
	tests := []struct {
		name   string
		n      int
		makeTS func(i int, rng *rand.Rand) int64
	}{
		{
			// Typical production case: timestamps advance monotonically with small jitter.
			name: "mostly_in_order",
			n:    100_000,
			makeTS: func(i int, rng *rand.Rand) int64 {
				return int64(i*1_000) + rng.Int64N(100) // ±100ns jitter
			},
		},
		{
			// Edge case: fully random timestamps within a small batch.
			name: "fully_random",
			n:    5_000,
			makeTS: func(_ int, rng *rand.Rand) int64 {
				return rng.Int64N(3_600_000_000_000) // random within 1-hour window
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := memtable.New()
			rng := rand.New(rand.NewPCG(42, 0))

			for i := range tc.n {
				m.Append(record.Record{
					Timestamp: tc.makeTS(i, rng),
					SourceID:  []byte("src"),
					Payload:   []byte("data"),
				})
			}

			got := collectAll(t, m.Iterator())
			require.Len(t, got, tc.n)
			assert.True(t, isSorted(got), "records must be in (Timestamp, SourceID) order")
		})
	}
}

// TestScanBounds verifies that Scan returns only records within [start, end).
func TestScanBounds(t *testing.T) {
	tests := []struct {
		name      string
		start     int64
		end       int64
		wantCount int
	}{
		{"full_range", 0, 1_000, 100},
		{"mid_range", 100, 500, 40},
		{"exact_boundary", 0, 10, 1}, // only ts=0
		{"empty_range", 500, 500, 0}, // zero-width window
		{"beyond_range", 1_000, 2_000, 0},
	}

	m := memtable.New()
	for i := range 100 {
		m.Append(record.Record{
			Timestamp: int64(i * 10),
			SourceID:  []byte("s"),
			Payload:   []byte("p"),
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := collectAll(t, m.Scan(tc.start, tc.end, nil))
			require.Len(t, got, tc.wantCount)
			for _, r := range got {
				assert.GreaterOrEqual(t, r.Timestamp, tc.start, "timestamp below start")
				assert.Less(t, r.Timestamp, tc.end, "timestamp at or above end")
			}
		})
	}
}

// TestScanSourceFilter verifies that Scan filters by SourceID when one is provided.
func TestScanSourceFilter(t *testing.T) {
	m := memtable.New()
	for i := range 20 {
		m.Append(record.Record{Timestamp: int64(i), SourceID: []byte("alpha"), Payload: []byte("a")})
		m.Append(record.Record{Timestamp: int64(i), SourceID: []byte("beta"), Payload: []byte("b")})
	}

	tests := []struct {
		name      string
		sourceID  []byte
		wantCount int
	}{
		{"filter_alpha", []byte("alpha"), 20},
		{"filter_beta", []byte("beta"), 20},
		{"no_filter", nil, 40},
		{"unknown_source", []byte("gamma"), 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := collectAll(t, m.Scan(0, 100, tc.sourceID))
			require.Len(t, got, tc.wantCount)
			if tc.sourceID != nil {
				for _, r := range got {
					assert.Equal(t, tc.sourceID, r.SourceID)
				}
			}
		})
	}
}

// TestFreeze verifies that the frozen snapshot is unaffected by subsequent Append calls.
func TestFreeze(t *testing.T) {
	m := memtable.New()
	m.Append(record.Record{Timestamp: 1, SourceID: []byte("s"), Payload: []byte("p")})

	frozen := m.Freeze()

	// Appending after freeze must not alter the snapshot.
	for range 100 {
		m.Append(record.Record{Timestamp: 2, SourceID: []byte("s"), Payload: []byte("p")})
	}

	got := collectAll(t, frozen.Iterator())
	assert.Len(t, got, 1, "frozen snapshot must not reflect post-freeze appends")
}

// TestApproximateSize verifies the byte-size accounting matches the expected formula.
func TestApproximateSize(t *testing.T) {
	tests := []struct {
		name     string
		records  []record.Record
		wantSize int64
	}{
		{
			name: "single_record",
			records: []record.Record{
				{Timestamp: 1, SourceID: []byte("abc"), Payload: []byte("1234567890")},
			},
			wantSize: 8 + 3 + 10, // 21
		},
		{
			name: "multiple_records",
			records: []record.Record{
				{Timestamp: 1, SourceID: []byte("s"), Payload: []byte("pp")},
				{Timestamp: 2, SourceID: []byte("ss"), Payload: []byte("p")},
			},
			wantSize: (8 + 1 + 2) + (8 + 2 + 1), // 11 + 11 = 22
		},
		{
			name:     "empty",
			records:  nil,
			wantSize: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := memtable.New()
			for _, r := range tc.records {
				m.Append(r)
			}
			assert.Equal(t, tc.wantSize, m.ApproximateSize())
		})
	}
}
