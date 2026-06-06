package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/record"
)

var blockFixture = []record.Record{
	{Timestamp: 1_000, SourceID: []byte("sensor-1"), Payload: []byte("data-a")},
	{Timestamp: 2_000, SourceID: []byte("sensor-2"), Payload: []byte("data-b")},
	{Timestamp: 2_000, SourceID: []byte("sensor-3"), Payload: []byte("data-c")}, // same ts, different src
	{Timestamp: 3_000, SourceID: []byte("sensor-1"), Payload: []byte("data-d")},
}

func TestBlockRoundTrip(t *testing.T) {
	encoded := encodeBlock(blockFixture)
	got, err := decodeBlock(encoded)
	require.NoError(t, err)
	assert.Equal(t, blockFixture, got)
}

func TestBlockEmptyRoundTrip(t *testing.T) {
	encoded := encodeBlock(nil)
	got, err := decodeBlock(encoded)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestBlockCRCMismatch(t *testing.T) {
	cases := []struct {
		name  string
		setup func() []byte
	}{
		{
			"flip last data byte",
			func() []byte {
				b := encodeBlock(blockFixture)
				b[len(b)-5] ^= 0xFF
				return b
			},
		},
		{
			"flip first byte",
			func() []byte {
				b := encodeBlock(blockFixture)
				b[0] ^= 0x01
				return b
			},
		},
		{
			"truncate one byte",
			func() []byte {
				b := encodeBlock(blockFixture)
				return b[:len(b)-1]
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeBlock(tc.setup())
			assert.ErrorIs(t, err, ErrBlockCorrupt)
		})
	}
}

func TestBlockTooShort(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"nil input", nil},
		{"empty input", []byte{}},
		{"3 bytes — below minimum of 8", []byte{0x01, 0x02, 0x03}},
		{"7 bytes — missing one CRC byte", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeBlock(tc.data)
			assert.ErrorIs(t, err, ErrBlockCorrupt)
		})
	}
}

func TestBlockZeroLengthFields(t *testing.T) {
	// nil and []byte{} both encode as length-0; both decode back as []byte{}.
	input := []record.Record{
		{Timestamp: 0, SourceID: []byte{}, Payload: []byte{}},
		{Timestamp: -1, SourceID: []byte{}, Payload: []byte{}},
	}
	encoded := encodeBlock(input)
	got, err := decodeBlock(encoded)
	require.NoError(t, err)
	assert.Equal(t, input, got)
}
