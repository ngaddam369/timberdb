package metrics

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {
	t.Run("new_nonnil", func(t *testing.T) {
		m := New()
		require.NotNil(t, m)
		assert.NotNil(t, m.AppendsTotal)
		assert.NotNil(t, m.AppendBytesTotal)
		assert.NotNil(t, m.LateArrivalsTotal)
		assert.NotNil(t, m.WALWritesTotal)
		assert.NotNil(t, m.FlushedTotal)
		assert.NotNil(t, m.ScansTotal)
		assert.NotNil(t, m.ScanRecordsTotal)
		assert.NotNil(t, m.SSTReadsTotal)
		assert.NotNil(t, m.SSTSkipsTotal)
		assert.NotNil(t, m.CompactionsTotal)
		assert.NotNil(t, m.FilesExpiredTotal)
		assert.NotNil(t, m.BytesReclaimedTotal)
		assert.NotNil(t, m.ActivePartitions)
		assert.NotNil(t, m.SSTFilesTotal)
		assert.NotNil(t, m.SSTBytesTotal)
		assert.NotNil(t, m.RetentionHorizonTS)
		assert.NotNil(t, m.ScanDuration)
		assert.NotNil(t, m.CompactionDuration)
	})

	t.Run("handler_nonnil", func(t *testing.T) {
		m := New()
		assert.NotNil(t, m.Handler())
	})

	t.Run("gather_roundtrip", func(t *testing.T) {
		m := New()
		m.AppendsTotal.Add(42)

		families, err := m.Gather()
		require.NoError(t, err)

		var found bool
		for _, f := range families {
			if f.GetName() == "timberdb_appends_total" {
				found = true
				require.Len(t, f.GetMetric(), 1)
				assert.Equal(t, float64(42), f.GetMetric()[0].GetCounter().GetValue())
			}
		}
		assert.True(t, found, "timberdb_appends_total must appear in gathered output")
	})

	t.Run("isolated_registries", func(t *testing.T) {
		m1 := New()
		m2 := New()
		m1.AppendsTotal.Add(10)

		f1, err := m1.Gather()
		require.NoError(t, err)
		f2, err := m2.Gather()
		require.NoError(t, err)

		val := func(families []*dto.MetricFamily) float64 {
			for _, f := range families {
				if f.GetName() == "timberdb_appends_total" {
					return f.GetMetric()[0].GetCounter().GetValue()
				}
			}
			return 0
		}
		assert.Equal(t, float64(10), val(f1))
		assert.Equal(t, float64(0), val(f2))
	})
}
