package sstable

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockCacheGetMiss(t *testing.T) {
	c := NewBlockCache(1024)
	_, ok := c.get("f", 0)
	assert.False(t, ok)
}

func TestBlockCacheGetHit(t *testing.T) {
	c := NewBlockCache(1024)
	data := []byte("hello")
	c.put("f", 0, data)
	got, ok := c.get("f", 0)
	require.True(t, ok)
	assert.Equal(t, data, got)
}

func TestBlockCacheCopyOnPut(t *testing.T) {
	c := NewBlockCache(1024)
	orig := []byte("original")
	c.put("f", 0, orig)
	// Mutate the original slice after put.
	orig[0] = 'X'
	got, ok := c.get("f", 0)
	require.True(t, ok)
	assert.Equal(t, byte('o'), got[0], "cache entry must be a copy, not an alias")
}

func TestBlockCacheLRUEviction(t *testing.T) {
	// maxBytes = 10; each entry is 5 bytes.
	c := NewBlockCache(10)
	c.put("f", 0, []byte("aaaaa")) // entry A (5B, total 5B)
	c.put("f", 8, []byte("bbbbb")) // entry B (5B, total 10B)

	// Access A to make it MRU; B becomes LRU.
	_, ok := c.get("f", 0)
	require.True(t, ok, "A must still be present")

	// Adding a third 5-byte entry exceeds capacity; LRU (B) must be evicted.
	c.put("f", 16, []byte("ccccc"))

	_, hasA := c.get("f", 0)
	_, hasB := c.get("f", 8)
	_, hasC := c.get("f", 16)
	assert.True(t, hasA, "A (MRU) must survive eviction")
	assert.False(t, hasB, "B (LRU) must be evicted")
	assert.True(t, hasC, "C (newest) must be present")

	_, used := c.Metrics()
	assert.LessOrEqual(t, used, c.maxBytes)
}

func TestBlockCacheMetrics(t *testing.T) {
	c := NewBlockCache(1024)
	c.put("f", 0, []byte("abc"))
	c.put("f", 8, []byte("de"))
	entries, used := c.Metrics()
	assert.Equal(t, 2, entries)
	assert.Equal(t, int64(5), used)
}

func TestBlockCacheZeroMaxBytes(t *testing.T) {
	c := NewBlockCache(0)
	c.put("f", 0, []byte("hello"))
	entries, used := c.Metrics()
	assert.Equal(t, 0, entries, "zero-capacity cache must evict every entry immediately")
	assert.Equal(t, int64(0), used)
}

func TestBlockCachePutEmpty(t *testing.T) {
	c := NewBlockCache(1024)
	c.put("f", 0, []byte{})
	got, ok := c.get("f", 0)
	require.True(t, ok, "empty-slice entry must be stored")
	assert.Equal(t, []byte{}, got)
	entries, used := c.Metrics()
	assert.Equal(t, 1, entries)
	assert.Equal(t, int64(0), used)
}

func TestBlockCachePutOversized(t *testing.T) {
	c := NewBlockCache(4)
	c.put("f", 0, []byte("12345")) // 5 bytes > maxBytes 4
	entries, used := c.Metrics()
	assert.Equal(t, 0, entries, "entry larger than maxBytes must be self-evicted")
	assert.Equal(t, int64(0), used)
}

func TestBlockCacheConcurrent(t *testing.T) {
	c := NewBlockCache(1 << 20) // 1 MiB — large enough that eviction doesn't interfere
	const goroutines = 20
	const ops = 200
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range ops {
				offset := uint64(g*ops + i)
				c.put("f", offset, []byte("payload"))
				c.get("f", offset)
			}
		})
	}
	wg.Wait()
	entries, _ := c.Metrics()
	assert.Positive(t, entries)
}
