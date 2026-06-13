package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordViewClone(t *testing.T) {
	src := []byte("source")
	pay := []byte("payload")
	view := RecordView{Timestamp: 42, SourceID: src, Payload: pay}

	cloned := view.Clone()
	require.Equal(t, view.Timestamp, cloned.Timestamp)
	require.Equal(t, view.SourceID, cloned.SourceID)
	require.Equal(t, view.Payload, cloned.Payload)

	// Mutating the original slices must not affect the clone.
	src[0] = 'X'
	pay[0] = 'X'
	assert.Equal(t, []byte("source"), cloned.SourceID, "Clone must own its SourceID")
	assert.Equal(t, []byte("payload"), cloned.Payload, "Clone must own its Payload")
}
