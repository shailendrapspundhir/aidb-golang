package storage

import (
	"aidb/internal/document"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// HybridStorage combines an in-memory cache with persistent disk storage
type HybridStorage struct {
	cache         *LRUCache
	disk          Storage // Use interface instead of concrete type
	cacheEnabled  bool
	mu            sync.RWMutex
	
	// Statistics
	cacheHits   int64
	cacheMisses int64
	diskReads   int64
	diskWrites  int64
}

// HybridStorageOptions contains options for hybrid storage
type HybridStorageOptions struct {
	Path           string
	CollectionName string
	CacheSizeMB    int
	CacheEnabled   bool
	BoltDBStorage  Storage // Optional BoltDB storage to use instead of RocksDB
}

// NewHybridStorage creates a new hybrid storage with cache and disk backend
func NewHybridStorage(opts HybridStorageOptions) (*HybridStorage, error) {
	// Create application-level LRU cache
	var cache *LRUCache
	cacheSizeBytes := int64(opts.CacheSizeMB) * 1024 * 1024 / 2 // Use half for app-level cache
	if cacheSizeBytes > 0 {
		cache = NewLRUCache(cacheSizeBytes)
	}

	// If BoltDB storage is provided, use it
	if opts.BoltDBStorage != nil {
		return &HybridStorage{
			cache:        cache,
			disk:         opts.BoltDBStorage,
			cacheEnabled: opts.CacheEnabled && cache != nil,
		}, nil
	}

	// Try RocksDB storage
	rocksOpts := RocksDBOptions{
		Path:           opts.Path,
		CollectionName: opts.CollectionName,
		CacheSizeMB:    opts.CacheSizeMB,
		EnableCache:    true,
	}
	
	disk, err := NewRocksDBStorage(rocksOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk storage: %w", err)
	}
	
	return &HybridStorage{
		cache:        cache,
		disk:         disk,
		cacheEnabled: opts.CacheEnabled && cache != nil,
	}, nil
}

// NewHybridStorageWithBoltDB creates a hybrid storage with BoltDB backend
func NewHybridStorageWithBoltDB(boltStorage Storage, cacheSizeMB int, cacheEnabled bool) *HybridStorage {
	var cache *LRUCache
	cacheSizeBytes := int64(cacheSizeMB) * 1024 * 1024 / 2
	if cacheSizeBytes > 0 {
		cache = NewLRUCache(cacheSizeBytes)
	}

	return &HybridStorage{
		cache:        cache,
		disk:         boltStorage,
		cacheEnabled: cacheEnabled && cache != nil,
	}
}

// Insert stores a new document
func (s *HybridStorage) Insert(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check cache first
	if s.cacheEnabled && s.cache != nil {
		if _, exists := s.cache.Get(doc.ID); exists {
			return ErrDocumentExists
		}
	}

	// Insert to disk
	if err := s.disk.Insert(doc); err != nil {
		return err
	}
	
	atomic.AddInt64(&s.diskWrites, 1)

	// Add to cache
	if s.cacheEnabled && s.cache != nil {
		data, err := json.Marshal(doc)
		if err == nil {
			s.cache.Set(doc.ID, data)
		}
	}

	return nil
}

// Get retrieves a document by ID
func (s *HybridStorage) Get(id string) (*document.Document, error) {
	// Try cache first
	if s.cacheEnabled && s.cache != nil {
		if data, found := s.cache.Get(id); found {
			atomic.AddInt64(&s.cacheHits, 1)
			var doc document.Document
			if err := json.Unmarshal(data, &doc); err == nil {
				return &doc, nil
			}
		}
		atomic.AddInt64(&s.cacheMisses, 1)
	}

	// Fall back to disk
	atomic.AddInt64(&s.diskReads, 1)
	doc, err := s.disk.Get(id)
	if err != nil {
		return nil, err
	}

	// Populate cache
	if s.cacheEnabled && s.cache != nil && doc != nil {
		data, marshalErr := json.Marshal(doc)
		if marshalErr == nil {
			s.cache.Set(id, data)
		}
	}

	return doc, nil
}

// GetRaw retrieves raw bytes by ID (checks cache first)
func (s *HybridStorage) GetRaw(id string) ([]byte, error) {
	// Try cache first
	if s.cacheEnabled && s.cache != nil {
		if data, found := s.cache.Get(id); found {
			atomic.AddInt64(&s.cacheHits, 1)
			return data, nil
		}
		atomic.AddInt64(&s.cacheMisses, 1)
	}

	// Fall back to disk
	atomic.AddInt64(&s.diskReads, 1)
	data, err := s.disk.GetRaw(id)
	if err != nil {
		return nil, err
	}

	// Populate cache
	if s.cacheEnabled && s.cache != nil {
		s.cache.Set(id, data)
	}

	return data, nil
}

// Update updates an existing document
func (s *HybridStorage) Update(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update on disk
	if err := s.disk.Update(doc); err != nil {
		return err
	}
	
	atomic.AddInt64(&s.diskWrites, 1)

	// Update cache
	if s.cacheEnabled && s.cache != nil {
		data, err := json.Marshal(doc)
		if err == nil {
			s.cache.Set(doc.ID, data)
		}
	}

	return nil
}

// Delete removes a document by ID
func (s *HybridStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Delete from disk
	if err := s.disk.Delete(id); err != nil {
		return err
	}
	
	atomic.AddInt64(&s.diskWrites, 1)

	// Remove from cache
	if s.cacheEnabled && s.cache != nil {
		s.cache.Delete(id)
	}

	return nil
}

// Find retrieves documents matching a filter
func (s *HybridStorage) Find(filter map[string]interface{}) ([]*document.Document, error) {
	// For filtered queries, we need to scan disk
	// In the future, we can use indexes to optimize this
	atomic.AddInt64(&s.diskReads, 1)
	return s.disk.Find(filter)
}

// FindAll retrieves all documents
func (s *HybridStorage) FindAll() ([]*document.Document, error) {
	atomic.AddInt64(&s.diskReads, 1)
	return s.disk.FindAll()
}

// Count returns the number of documents
func (s *HybridStorage) Count() int {
	return s.disk.Count()
}

// Clear removes all documents from the collection
func (s *HybridStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear disk
	if err := s.disk.Clear(); err != nil {
		return err
	}

	// Clear cache
	if s.cacheEnabled && s.cache != nil {
		s.cache.Clear()
	}

	return nil
}

// Close closes the storage
func (s *HybridStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cacheEnabled && s.cache != nil {
		s.cache.Clear()
	}
	return s.disk.Close()
}

// Flush flushes all data to disk
func (s *HybridStorage) Flush() error {
	return s.disk.Flush()
}

// Stats returns storage statistics
func (s *HybridStorage) Stats() HybridStorageStats {
	var cacheStats CacheStats
	if s.cacheEnabled && s.cache != nil {
		cacheStats = s.cache.Stats()
	}

	return HybridStorageStats{
		CacheEnabled: s.cacheEnabled,
		CacheStats:   cacheStats,
		CacheHits:    atomic.LoadInt64(&s.cacheHits),
		CacheMisses:  atomic.LoadInt64(&s.cacheMisses),
		DiskReads:    atomic.LoadInt64(&s.diskReads),
		DiskWrites:   atomic.LoadInt64(&s.diskWrites),
	}
}

// HybridStorageStats contains hybrid storage statistics
type HybridStorageStats struct {
	CacheEnabled bool
	CacheStats   CacheStats
	CacheHits    int64
	CacheMisses  int64
	DiskReads    int64
	DiskWrites   int64
}

// ImportDocuments imports multiple documents at once
func (s *HybridStorage) ImportDocuments(docs []*document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Import to disk
	if err := s.disk.ImportDocuments(docs); err != nil {
		return err
	}
	
	atomic.AddInt64(&s.diskWrites, int64(len(docs)))

	// Populate cache with imported documents
	if s.cacheEnabled && s.cache != nil {
		for _, doc := range docs {
			data, err := json.Marshal(doc)
			if err == nil {
				s.cache.Set(doc.ID, data)
			}
		}
	}

	return nil
}

// PutRaw stores raw bytes by ID
func (s *HybridStorage) PutRaw(id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.disk.PutRaw(id, data); err != nil {
		return err
	}
	
	atomic.AddInt64(&s.diskWrites, 1)

	if s.cacheEnabled && s.cache != nil {
		s.cache.Set(id, data)
	}

	return nil
}

// DeleteRaw removes raw bytes by ID
func (s *HybridStorage) DeleteRaw(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.disk.DeleteRaw(id); err != nil {
		return err
	}
	
	atomic.AddInt64(&s.diskWrites, 1)

	if s.cacheEnabled && s.cache != nil {
		s.cache.Delete(id)
	}

	return nil
}

// CompactRange compacts the underlying storage
func (s *HybridStorage) CompactRange(start, end string) error {
	return s.disk.CompactRange(start, end)
}

// GetCache returns the cache instance for direct access
func (s *HybridStorage) GetCache() *LRUCache {
	return s.cache
}

// GetDisk returns the disk storage instance for direct access
func (s *HybridStorage) GetDisk() Storage {
	return s.disk
}

// WarmCache loads frequently accessed documents into cache
func (s *HybridStorage) WarmCache(docIDs []string) error {
	if !s.cacheEnabled || s.cache == nil {
		return nil
	}

	for _, id := range docIDs {
		data, err := s.disk.GetRaw(id)
		if err == nil {
			s.cache.Set(id, data)
		}
	}

	return nil
}

// Prefetch prefetches documents into cache in the background
func (s *HybridStorage) Prefetch(docIDs []string) {
	if !s.cacheEnabled || s.cache == nil {
		return
	}

	go func() {
		for _, id := range docIDs {
			// Check if already in cache
			if s.cache.Contains(id) {
				continue
			}
			
			data, err := s.disk.GetRaw(id)
			if err == nil {
				s.cache.Set(id, data)
			}
		}
	}()
}

// Cursor returns a streaming iterator over all documents (delegates to disk)
func (s *HybridStorage) Cursor() (Cursor, error) {
	atomic.AddInt64(&s.diskReads, 1)
	return s.disk.Cursor()
}
