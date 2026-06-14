package sstable

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var compressFixture = bytes.Repeat([]byte("timberdb block payload "), 64) // ~1.4 KB, compressible

func TestCompressNoneIsIdentity(t *testing.T) {
	got, err := compress(CompressionNone, compressFixture)
	require.NoError(t, err)
	assert.Equal(t, compressFixture, got)
}

func TestCompressZstdRoundTrip(t *testing.T) {
	compressed, err := compress(CompressionZstd, compressFixture)
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(compressFixture), "zstd must reduce size for compressible data")

	got, err := decompress(CompressionZstd, compressed)
	require.NoError(t, err)
	assert.Equal(t, compressFixture, got)
}

func TestCompressSnappyRoundTrip(t *testing.T) {
	compressed, err := compress(CompressionSnappy, compressFixture)
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(compressFixture), "snappy must reduce size for compressible data")

	got, err := decompress(CompressionSnappy, compressed)
	require.NoError(t, err)
	assert.Equal(t, compressFixture, got)
}

func TestDecompressNoneIsIdentity(t *testing.T) {
	got, err := decompress(CompressionNone, compressFixture)
	require.NoError(t, err)
	assert.Equal(t, compressFixture, got)
}

func TestCompressUnknownType(t *testing.T) {
	_, err := compress(CompressionType(99), compressFixture)
	assert.Error(t, err)
}
