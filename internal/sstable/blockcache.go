package sstable

import (
	"container/list"
	"sync"
)

type cacheKey struct {
	path   string
	offset uint64
}

type cacheEntry struct {
	data []byte
	elem *list.Element
}

// BlockCache is a size-bounded LRU cache for decompressed SSTable block payloads.
// Blocks are stored as validated payload slices (record-count header + records,
// without the trailing CRC). A nil *BlockCache disables caching.
// All methods are safe for concurrent use.
type BlockCache struct {
	mu        sync.Mutex
	maxBytes  int64
	usedBytes int64
	items     map[cacheKey]*cacheEntry
	lru       list.List // front = most recently used; Element.Value = cacheKey
}

// NewBlockCache creates a cache capped at maxBytes of decompressed block data.
func NewBlockCache(maxBytes int64) *BlockCache {
	return &BlockCache{
		maxBytes: maxBytes,
		items:    make(map[cacheKey]*cacheEntry),
	}
}

func (c *BlockCache) get(path string, offset uint64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[cacheKey{path, offset}]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	return e.data, true
}

func (c *BlockCache) put(path string, offset uint64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey{path, offset}
	if _, ok := c.items[k]; ok {
		return // already present; no update needed (blocks are immutable)
	}
	// Copy so the cache entry is independent of the caller's decompression buffer,
	// which will be reused for subsequent blocks and would corrupt the entry otherwise.
	copied := make([]byte, len(data))
	copy(copied, data)
	elem := c.lru.PushFront(k)
	c.items[k] = &cacheEntry{data: copied, elem: elem}
	c.usedBytes += int64(len(copied))
	for c.usedBytes > c.maxBytes && c.lru.Len() > 0 {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.lru.Remove(back)
		if old, ok := back.Value.(cacheKey); ok {
			if e, ok2 := c.items[old]; ok2 {
				c.usedBytes -= int64(len(e.data))
				delete(c.items, old)
			}
		}
	}
}

// Metrics returns the current number of cached entries and total bytes used.
func (c *BlockCache) Metrics() (entries int, usedBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items), c.usedBytes
}
