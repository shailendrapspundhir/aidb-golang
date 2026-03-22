//go:build rocksdb
// +build rocksdb

package storage

import (
	"aidb/internal/document"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/linxGnu/grocksdb"
)

// RocksDBStorage is a RocksDB-backed implementation of the Storage interface
type RocksDBStorage struct {
	db             *grocksdb.DB
	cfHandle       *grocksdb.ColumnFamilyHandle
	cfName         string
	mu             sync.RWMutex
	writeOptions   *grocksdb.WriteOptions
	readOptions    *grocksdb.ReadOptions
	flushOptions   *grocksdb.FlushOptions
}

// RocksDBOptions contains options for RocksDB storage
type RocksDBOptions struct {
	Path           string
	CollectionName string
	CacheSizeMB    int
	EnableCache    bool
}

// NewRocksDBStorage creates a new RocksDB storage for a collection
func NewRocksDBStorage(opts RocksDBOptions) (*RocksDBStorage, error) {
	if opts.CacheSizeMB <= 0 {
		opts.CacheSizeMB = 256
	}

	// Create options
	options := grocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetCreateIfMissingColumnFamilies(true)
	
	// Optimize for performance
	options.IncreaseParallelism(4)
	options.OptimizeLevelStyleCompaction(uint64(opts.CacheSizeMB) * 1024 * 1024)
	
	// Set up block cache if enabled
	if opts.EnableCache {
		blockCache := grocksdb.NewLRUCache(uint64(opts.CacheSizeMB) * 1024 * 1024)
		blockBasedTableOptions := grocksdb.NewDefaultBlockBasedTableOptions()
		blockBasedTableOptions.SetBlockCache(blockCache)
		options.SetBlockBasedTableFactory(blockBasedTableOptions)
	}
	
	// Set compression
	options.SetCompression(grocksdb.SnappyCompression)

	// Define column families
	cfNames := []string{"default", opts.CollectionName}
	cfOptions := []*grocksdb.Options{options, options}

	// Open database with column families
	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(options, opts.Path, cfNames, cfOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	// Find our column family handle
	var cfHandle *grocksdb.ColumnFamilyHandle
	for i, name := range cfNames {
		if name == opts.CollectionName {
			cfHandle = cfHandles[i]
			break
		}
	}
	
	if cfHandle == nil {
		db.Close()
		return nil, fmt.Errorf("failed to find column family handle for %s", opts.CollectionName)
	}

	storage := &RocksDBStorage{
		db:           db,
		cfHandle:     cfHandle,
		cfName:       opts.CollectionName,
		writeOptions: grocksdb.NewDefaultWriteOptions(),
		readOptions:  grocksdb.NewDefaultReadOptions(),
		flushOptions: grocksdb.NewDefaultFlushOptions(),
	}

	return storage, nil
}

// Insert stores a new document
func (s *RocksDBStorage) Insert(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(doc.ID)
	
	// Check if document already exists
	existing, err := s.db.GetCF(s.readOptions, s.cfHandle, key)
	if err != nil {
		return fmt.Errorf("failed to check existence: %w", err)
	}
	if existing.Exists() {
		existing.Free()
		return ErrDocumentExists
	}
	existing.Free()

	// Marshal and store the document
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	if err := s.db.PutCF(s.writeOptions, s.cfHandle, key, data); err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	return nil
}

// Get retrieves a document by ID
func (s *RocksDBStorage) Get(id string) (*document.Document, error) {
	key := []byte(id)
	
	value, err := s.db.GetCF(s.readOptions, s.cfHandle, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get document: %w", err)
	}
	defer value.Free()

	if !value.Exists() {
		return nil, ErrDocumentNotFound
	}

	var doc document.Document
	if err := json.Unmarshal(value.Data(), &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}

	return &doc, nil
}

// Update updates an existing document
func (s *RocksDBStorage) Update(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(doc.ID)
	
	// Check if document exists
	existing, err := s.db.GetCF(s.readOptions, s.cfHandle, key)
	if err != nil {
		return fmt.Errorf("failed to check existence: %w", err)
	}
	if !existing.Exists() {
		existing.Free()
		return ErrDocumentNotFound
	}
	existing.Free()

	// Marshal and store the document
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	if err := s.db.PutCF(s.writeOptions, s.cfHandle, key, data); err != nil {
		return fmt.Errorf("failed to update document: %w", err)
	}

	return nil
}

// Delete removes a document by ID
func (s *RocksDBStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(id)
	
	// Check if document exists
	existing, err := s.db.GetCF(s.readOptions, s.cfHandle, key)
	if err != nil {
		return fmt.Errorf("failed to check existence: %w", err)
	}
	if !existing.Exists() {
		existing.Free()
		return ErrDocumentNotFound
	}
	existing.Free()

	if err := s.db.DeleteCF(s.writeOptions, s.cfHandle, key); err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}

	return nil
}

// Find retrieves documents matching a filter
func (s *RocksDBStorage) Find(filter map[string]interface{}) ([]*document.Document, error) {
	var results []*document.Document

	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()
	
	it := s.db.NewIteratorCF(readOptions, s.cfHandle)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		value := it.Value()
		var doc document.Document
		if err := json.Unmarshal(value.Data(), &doc); err != nil {
			continue
		}

		if matchesFilter(&doc, filter) {
			results = append(results, &doc)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return results, nil
}

// FindAll retrieves all documents
func (s *RocksDBStorage) FindAll() ([]*document.Document, error) {
	var results []*document.Document

	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()
	
	it := s.db.NewIteratorCF(readOptions, s.cfHandle)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		value := it.Value()
		var doc document.Document
		if err := json.Unmarshal(value.Data(), &doc); err != nil {
			continue
		}
		results = append(results, &doc)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return results, nil
}

// Count returns the number of documents (approximate for performance)
func (s *RocksDBStorage) Count() int {
	count := 0
	
	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()
	
	it := s.db.NewIteratorCF(readOptions, s.cfHandle)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
	}

	return count
}

// Clear removes all documents from the collection
func (s *RocksDBStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect all keys first
	var keys [][]byte
	
	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()
	
	it := s.db.NewIteratorCF(readOptions, s.cfHandle)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := make([]byte, len(it.Key().Data()))
		copy(key, it.Key().Data())
		keys = append(keys, key)
	}
	it.Close()

	// Delete all keys
	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	
	for _, key := range keys {
		writeBatch.DeleteCF(s.cfHandle, key)
	}

	return s.db.Write(s.writeOptions, writeBatch)
}

// Close closes the storage
func (s *RocksDBStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.cfHandle.Destroy()
	s.db.Close()
	s.writeOptions.Destroy()
	s.readOptions.Destroy()
	s.flushOptions.Destroy()
	return nil
}

// Flush flushes all data to disk
func (s *RocksDBStorage) Flush() error {
	return s.db.Flush(s.flushOptions)
}

// GetRaw retrieves raw bytes by ID (for cache integration)
func (s *RocksDBStorage) GetRaw(id string) ([]byte, error) {
	key := []byte(id)
	
	value, err := s.db.GetCF(s.readOptions, s.cfHandle, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get document: %w", err)
	}
	defer value.Free()

	if !value.Exists() {
		return nil, ErrDocumentNotFound
	}

	// Copy the data since value.Data() is only valid until value.Free()
	data := make([]byte, len(value.Data()))
	copy(data, value.Data())
	return data, nil
}

// PutRaw stores raw bytes by ID (for cache integration)
func (s *RocksDBStorage) PutRaw(id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(id)
	return s.db.PutCF(s.writeOptions, s.cfHandle, key, data)
}

// DeleteRaw removes raw bytes by ID
func (s *RocksDBStorage) DeleteRaw(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(id)
	return s.db.DeleteCF(s.writeOptions, s.cfHandle, key)
}

// ImportDocuments imports multiple documents at once
func (s *RocksDBStorage) ImportDocuments(docs []*document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	for _, doc := range docs {
		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document %s: %w", doc.ID, err)
		}
		writeBatch.PutCF(s.cfHandle, []byte(doc.ID), data)
	}

	return s.db.Write(s.writeOptions, writeBatch)
}

// CompactRange compacts the underlying storage for the given range
func (s *RocksDBStorage) CompactRange(start, end string) error {
	return s.db.CompactRangeCF(s.cfHandle, []byte(start), []byte(end))
}

// GetColumnFamilyName returns the column family name
func (s *RocksDBStorage) GetColumnFamilyName() string {
	return s.cfName
}

// rocksdbCursor implements Cursor for RocksDBStorage
type rocksdbCursor struct {
	storage   *RocksDBStorage
	readOpts  *grocksdb.ReadOptions
	iterator  *grocksdb.Iterator
	current   *document.Document
	err       error
	closed    bool
}

// Cursor returns a streaming iterator over all documents
func (s *RocksDBStorage) Cursor() (Cursor, error) {
	readOpts := grocksdb.NewDefaultReadOptions()
	it := s.db.NewIteratorCF(readOpts, s.cfHandle)
	it.SeekToFirst()

	return &rocksdbCursor{
		storage:  s,
		readOpts: readOpts,
		iterator: it,
	}, nil
}

func (c *rocksdbCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}
	if !c.iterator.Valid() {
		c.current = nil
		return false
	}

	value := c.iterator.Value()
	var doc document.Document
	if err := json.Unmarshal(value.Data(), &doc); err != nil {
		c.err = fmt.Errorf("failed to unmarshal document: %w", err)
		c.current = nil
		return false
	}
	c.current = &doc
	c.iterator.Next()
	return true
}

func (c *rocksdbCursor) Current() *document.Document {
	return c.current
}

func (c *rocksdbCursor) Err() error {
	if c.err != nil {
		return c.err
	}
	if c.iterator != nil {
		return c.iterator.Err()
	}
	return nil
}

func (c *rocksdbCursor) Close() error {
	c.closed = true
	c.current = nil
	if c.iterator != nil {
		c.iterator.Close()
		c.iterator = nil
	}
	if c.readOpts != nil {
		c.readOpts.Destroy()
		c.readOpts = nil
	}
	return nil
}
