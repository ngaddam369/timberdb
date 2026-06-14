package sstable

import (
	"fmt"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// CompressionType identifies the compression algorithm applied to data blocks.
type CompressionType uint8

const (
	// CompressionNone leaves data blocks uncompressed (v0-compatible).
	CompressionNone CompressionType = 0
	// CompressionZstd compresses data blocks with Zstandard.
	CompressionZstd CompressionType = 1
	// CompressionSnappy compresses data blocks with Snappy.
	CompressionSnappy CompressionType = 2
)

// Package-level zstd encoder/decoder (thread-safe; EncodeAll/DecodeAll are stateless).
var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	var err error
	zstdEnc, err = zstd.NewWriter(nil)
	if err != nil {
		panic("sstable: init zstd encoder: " + err.Error())
	}
	zstdDec, err = zstd.NewReader(nil)
	if err != nil {
		panic("sstable: init zstd decoder: " + err.Error())
	}
}

// compress applies ct to src and returns the compressed bytes.
// CompressionNone returns src unchanged (no copy).
func compress(ct CompressionType, src []byte) ([]byte, error) {
	switch ct {
	case CompressionNone:
		return src, nil
	case CompressionZstd:
		return zstdEnc.EncodeAll(src, nil), nil
	case CompressionSnappy:
		return snappy.Encode(nil, src), nil
	default:
		return nil, fmt.Errorf("sstable: unknown compression type %d", ct)
	}
}

// decompress reverses the compression applied by compress.
// CompressionNone returns src unchanged (no copy).
func decompress(ct CompressionType, src []byte) ([]byte, error) {
	switch ct {
	case CompressionNone:
		return src, nil
	case CompressionZstd:
		return zstdDec.DecodeAll(src, nil)
	case CompressionSnappy:
		return snappy.Decode(nil, src)
	default:
		return nil, fmt.Errorf("sstable: unknown compression type %d", ct)
	}
}
