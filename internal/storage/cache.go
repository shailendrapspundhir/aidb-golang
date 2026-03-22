package storage

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// CacheItem represents an item stored in the cache
type CacheItem struct {
	Key       string
	Value     []byte
	Size      int64
	Frequency int32 // For LFU-like behavior
}

// LRUCache is a thread-safe LRU cache with configurable size
type LRUCache struct {
	mu            sync.RWMutex
	maxSize       int64 // Maximum size in bytes
	currentSize   int64 // Current size in bytes
	items         map[string]*list.Element
	evictionList  *list.List
	hits          int64
	misses        int64
	evictions     int64
}

// NewLRUCache creates a new LRU cache with the specified maximum size in bytes
func NewLRUCache(maxSizeBytes int64) *LRUCache {
	return &LRUCache{
		maxSize:      maxSizeBytes,
		currentSize:  0,
		items:        make(map[string]*list.Element),
		evictionList: list.New(),
	}
}

// Get retrieves an item from the cache
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, exists := c.items[key]; exists {
		// Move to front (most recently used)
		c.evictionList.MoveToFront(element)
		item := element.Value.(*CacheItem)
		atomic.AddInt32(&item.Frequency, 1)
		atomic.AddInt64(&c.hits, 1)
		return item.Value, true
	}

	atomic.AddInt64(&c.misses, 1)
	return nil, false
}

// Set adds or updates an item in the cache
func (c *LRUCache) Set(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	itemSize := int64(len(value))
	
	// If single item is larger than cache, don't cache it
	if itemSize > c.maxSize {
		return
	}

	// If item already exists, update it
	if element, exists := c.items[key]; exists {
		item := element.Value.(*CacheItem)
		c.currentSize -= item.Size
		c.currentSize += itemSize
		item.Value = value
		item.Size = itemSize
		c.evictionList.MoveToFront(element)
		return
	}

	// Evict items until we have enough space
	for c.currentSize+itemSize > c.maxSize && c.evictionList.Len() > 0 {
		c.evictOldest()
	}

	// Add new item
	item := &CacheItem{
		Key:       key,
		Value:     value,
		Size:      itemSize,
		Frequency: 1,
	}
	element := c.evictionList.PushFront(item)
	c.items[key] = element
	c.currentSize += itemSize
}

// Delete removes an item from the cache
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, exists := c.items[key]; exists {
		c.removeElement(element)
	}
}

// Clear removes all items from the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictionList.Init()
	c.currentSize = 0
}

// Size returns the current size of the cache in bytes
func (c *LRUCache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentSize
}

// Count returns the number of items in the cache
func (c *LRUCache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Stats returns cache statistics
func (c *LRUCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hits := atomic.LoadInt64(&c.hits)
	misses := atomic.LoadInt64(&c.misses)
	evictions := atomic.LoadInt64(&c.evictions)

	var hitRate float64
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return CacheStats{
		Hits:          hits,
		Misses:        misses,
		Evictions:     evictions,
		HitRate:       hitRate,
		CurrentSize:   c.currentSize,
		MaxSize:       c.maxSize,
		ItemCount:     len(c.items),
		SizePercent:   float64(c.currentSize) / float64(c.maxSize) * 100,
	}
}

// CacheStats contains cache statistics
type CacheStats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	HitRate     float64
	CurrentSize int64
	MaxSize     int64
	ItemCount   int
	SizePercent float64
}

// evictOldest removes the least recently used item
func (c *LRUCache) evictOldest() {
	element := c.evictionList.Back()
	if element != nil {
		c.removeElement(element)
		atomic.AddInt64(&c.evictions, 1)
	}
}

// removeElement removes an element from the cache
func (c *LRUCache) removeElement(element *list.Element) {
	item := element.Value.(*CacheItem)
	delete(c.items, item.Key)
	c.evictionList.Remove(element)
	c.currentSize -= item.Size
}

// MultiGet retrieves multiple items from the cache
func (c *LRUCache) MultiGet(keys []string) map[string][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make(map[string][]byte)
	for _, key := range keys {
		if element, exists := c.items[key]; exists {
			c.evictionList.MoveToFront(element)
			item := element.Value.(*CacheItem)
			atomic.AddInt32(&item.Frequency, 1)
			atomic.AddInt64(&c.hits, 1)
			result[key] = item.Value
		} else {
			atomic.AddInt64(&c.misses, 1)
		}
	}
	return result
}

// MultiSet adds multiple items to the cache
func (c *LRUCache) MultiSet(items map[string][]byte) {
	for key, value := range items {
		c.Set(key, value)
	}
}

// Contains checks if a key exists in the cache
func (c *LRUCache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.items[key]
	return exists
}

// Keys returns all keys in the cache
func (c *LRUCache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}
	return keys
}
