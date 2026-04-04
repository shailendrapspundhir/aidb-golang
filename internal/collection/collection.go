package collection

import (
	"aidb/internal/config"
	"aidb/internal/document"
	"aidb/internal/fulltext"
	"aidb/internal/storage"
	"aidb/internal/transaction"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Collection represents a collection of documents (like a table in SQL)
type Collection struct {
	Name    string
	Schema  *document.Schema
	storage storage.Storage
	indexes map[string]storage.Index // field -> index
	manager *Manager                  // Reference to manager for $lookup cross-collection queries
	mu      sync.RWMutex

	// Full-text search
	fulltextIndex  *fulltext.InvertedIndex
	fulltextFields []string // fields to index for full-text search

	// Transaction support
	txManager *transaction.Manager

	// Query statistics collector (optional)
	queryStats *QueryStatisticsCollector
}

// NewCollection creates a new collection with memory storage
func NewCollection(name string, schema *document.Schema) *Collection {
	return &Collection{
		Name:       name,
		Schema:     schema,
		storage:    storage.NewMemoryStorage(),
		indexes:    make(map[string]storage.Index),
		queryStats: NewQueryStatisticsCollector(),
	}
}

// NewCollectionWithStorage creates a new collection with a custom storage backend
func NewCollectionWithStorage(name string, schema *document.Schema, store storage.Storage) *Collection {
	return &Collection{
		Name:       name,
		Schema:     schema,
		storage:    store,
		indexes:    make(map[string]storage.Index),
		queryStats: NewQueryStatisticsCollector(),
	}
}

// CreateIndex creates an index on a field
func (c *Collection) CreateIndex(field string, indexType storage.IndexType) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.indexes[field]; exists {
		return fmt.Errorf("index already exists on field: %s", field)
	}

	var idx storage.Index
	switch indexType {
	case storage.IndexTypeBTree:
		idx = storage.NewBTreeIndex(fmt.Sprintf("%s_%s_idx", c.Name, field), field, 64)
	case storage.IndexTypeHash:
		idx = storage.NewHashIndex(fmt.Sprintf("%s_%s_idx", c.Name, field), field)
	default:
		return fmt.Errorf("unsupported index type: %s", indexType)
	}

	// Build index from existing documents
	docs, err := c.storage.FindAll()
	if err != nil {
		return fmt.Errorf("failed to build index: %w", err)
	}
	for _, doc := range docs {
		if value, exists := doc.Data[field]; exists {
			idx.Insert(value, doc.ID)
		}
	}

	c.indexes[field] = idx
	return nil
}

// DropIndex removes an index
func (c *Collection) DropIndex(field string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.indexes[field]; !exists {
		return fmt.Errorf("index not found on field: %s", field)
	}

	delete(c.indexes, field)
	return nil
}

// GetIndexes returns all indexes
func (c *Collection) GetIndexes() map[string]storage.Index {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]storage.Index)
	for k, v := range c.indexes {
		result[k] = v
	}
	return result
}

// CreateFullTextIndex creates a full-text index on specified fields.
// The index will be built from existing documents and updated on future CRUD operations.
func (c *Collection) CreateFullTextIndex(fields []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(fields) == 0 {
		return fmt.Errorf("at least one field is required for full-text index")
	}

	if c.fulltextIndex == nil {
		c.fulltextIndex = fulltext.NewInvertedIndex()
	}

	c.fulltextFields = make([]string, len(fields))
	copy(c.fulltextFields, fields)

	// Build index from existing documents
	docs, err := c.storage.FindAll()
	if err != nil {
		return fmt.Errorf("failed to build full-text index: %w", err)
	}

	for _, doc := range docs {
		c.indexDocForFullTextLocked(doc)
	}

	return nil
}

// GetFullTextIndex returns the full-text index (nil if not created).
func (c *Collection) GetFullTextIndex() *fulltext.InvertedIndex {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fulltextIndex
}

// GetFullTextFields returns the fields indexed for full-text search.
func (c *Collection) GetFullTextFields() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, len(c.fulltextFields))
	copy(result, c.fulltextFields)
	return result
}

// ClearFullTextIndex removes the full-text index from the collection.
func (c *Collection) ClearFullTextIndex() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fulltextIndex != nil {
		c.fulltextIndex.Clear()
	}
	c.fulltextIndex = nil
	c.fulltextFields = nil
}

// RebuildFullTextIndex rebuilds the full-text index from all documents.
func (c *Collection) RebuildFullTextIndex() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.fulltextFields) == 0 {
		return fmt.Errorf("no full-text index fields configured")
	}

	if c.fulltextIndex == nil {
		c.fulltextIndex = fulltext.NewInvertedIndex()
	} else {
		c.fulltextIndex.Clear()
	}

	docs, err := c.storage.FindAll()
	if err != nil {
		return err
	}

	for _, doc := range docs {
		c.indexDocForFullTextLocked(doc)
	}

	return nil
}

// indexDocForFullTextLocked indexes a document's text fields (caller must hold lock).
func (c *Collection) indexDocForFullTextLocked(doc *document.Document) {
	if c.fulltextIndex == nil || len(c.fulltextFields) == 0 {
		return
	}

	values := make([]string, 0, len(c.fulltextFields))
	for _, field := range c.fulltextFields {
		if val, exists := doc.Data[field]; exists {
			switch v := val.(type) {
			case string:
				values = append(values, v)
			case []interface{}:
				// Concatenate array elements
				for _, elem := range v {
					if s, ok := elem.(string); ok {
						values = append(values, s)
					}
				}
			}
		}
	}
	if len(values) > 0 {
		c.fulltextIndex.IndexDocument(doc.ID, values)
	}
}

// Insert stores a new document in the collection
func (c *Collection) Insert(doc *document.Document) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate against schema if one exists
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	// Insert into storage
	if err := c.storage.Insert(doc); err != nil {
		return err
	}

	// Update indexes
	for field, idx := range c.indexes {
		if value, exists := doc.Data[field]; exists {
			idx.Insert(value, doc.ID)
		}
	}

	// Update full-text index
	c.indexDocForFullTextLocked(doc)

	return nil
}

// Get retrieves a document by ID
func (c *Collection) Get(id string) (*document.Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.Get(id)
}

// Update updates an existing document
func (c *Collection) Update(doc *document.Document) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get old document for index updates
	oldDoc, err := c.storage.Get(doc.ID)
	if err != nil {
		return err
	}

	// Validate against schema if one exists
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	// Update storage
	if err := c.storage.Update(doc); err != nil {
		return err
	}

	// Update indexes
	for field, idx := range c.indexes {
		// Remove old value
		if oldValue, exists := oldDoc.Data[field]; exists {
			idx.Delete(oldValue, doc.ID)
		}
		// Add new value
		if newValue, exists := doc.Data[field]; exists {
			idx.Insert(newValue, doc.ID)
		}
	}

	// Update full-text index (reindex the document)
	if c.fulltextIndex != nil {
		c.indexDocForFullTextLocked(doc)
	}

	return nil
}

// Patch partially updates a document
func (c *Collection) Patch(id string, data map[string]interface{}) (*document.Document, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get existing document
	doc, err := c.storage.Get(id)
	if err != nil {
		return nil, err
	}

	// Store old values for index updates
	oldData := make(map[string]interface{})
	for field := range c.indexes {
		if value, exists := doc.Data[field]; exists {
			oldData[field] = value
		}
	}

	// Merge the data
	doc.MergeData(data)

	// Validate against schema if one exists
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return nil, err
		}
	}

	// Update the document
	if err := c.storage.Update(doc); err != nil {
		return nil, err
	}

	// Update indexes
	for field, idx := range c.indexes {
		// Remove old value
		if oldValue, exists := oldData[field]; exists {
			idx.Delete(oldValue, doc.ID)
		}
		// Add new value
		if newValue, exists := doc.Data[field]; exists {
			idx.Insert(newValue, doc.ID)
		}
	}

	return doc, nil
}

// Delete removes a document by ID
func (c *Collection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get document for index updates
	doc, err := c.storage.Get(id)
	if err != nil {
		return err
	}

	// Delete from storage
	if err := c.storage.Delete(id); err != nil {
		return err
	}

	// Remove from indexes
	for field, idx := range c.indexes {
		if value, exists := doc.Data[field]; exists {
			idx.Delete(value, id)
		}
	}

	// Remove from full-text index
	if c.fulltextIndex != nil {
		c.fulltextIndex.RemoveDocument(id)
	}

	return nil
}

// Find retrieves documents matching a filter (records stats internally)
func (c *Collection) Find(filter map[string]interface{}) ([]*document.Document, error) {
	start := time.Now()
	results, err := c.findInternal(filter)
	duration := time.Since(start)

	// Record stats if collector exists
	if c.queryStats != nil {
		stats := QueryStats{
			CollectionName:   c.Name,
			Filter:           filter,
			DurationMs:       duration.Milliseconds(),
			DocumentsMatched: len(results),
			Timestamp:        start,
		}
		// Determine scan type from result (simplified)
		if len(filter) > 0 {
			stats.ScanType = "full" // default; overridden below
		}
		c.queryStats.RecordQuery(stats)
	}

	return results, err
}

// findInternal is the internal find implementation without stats recording
func (c *Collection) findInternal(filter map[string]interface{}) ([]*document.Document, error) {
	// Check if we can use an index
	for field, value := range filter {
		if idx, exists := c.indexes[field]; exists {
			// Use index to find document IDs
			ids, err := idx.Find(value)
			if err != nil {
				continue
			}
			// Fetch documents by IDs
			results := make([]*document.Document, 0, len(ids))
			for _, id := range ids {
				doc, err := c.storage.Get(id)
				if err == nil {
					// Verify the document still matches the full filter
					if matchesFilter(doc, filter) {
						results = append(results, doc)
					}
				}
			}
			return results, nil
		}
	}

	// Fall back to full scan
	return c.storage.Find(filter)
}

// FindWithStats retrieves documents and returns query statistics
func (c *Collection) FindWithStats(filter map[string]interface{}) ([]*document.Document, QueryStats, error) {
	start := time.Now()
	results, err := c.findInternal(filter)
	duration := time.Since(start)

	stats := QueryStats{
		CollectionName:   c.Name,
		Filter:           filter,
		DurationMs:       duration.Milliseconds(),
		DocumentsMatched: len(results),
		Timestamp:        start,
	}

	// Determine if index was used
	stats.ScanType = "full"
	for field := range filter {
		if idx, exists := c.indexes[field]; exists {
			stats.IndexUsed = idx.Name()
			stats.IndexType = string(idx.Type())
			stats.ScanType = "index"
			break
		}
	}

	// Record stats
	if c.queryStats != nil {
		c.queryStats.RecordQuery(stats)
	}

	return results, stats, err
}

// Explain returns an execution plan for a query without executing it
func (c *Collection) Explain(filter map[string]interface{}) ExplainPlan {
	plan := ExplainPlan{
		CollectionName: c.Name,
		Filter:         filter,
		Notes:          []string{},
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check for index usage
	for field := range filter {
		if idx, exists := c.indexes[field]; exists {
			stats := idx.Stats()
			plan.Strategy = "index_scan"
			plan.IndexUsed = idx.Name()
			plan.IndexType = string(idx.Type())
			plan.ScanType = "exact" // default; could be range/prefix based on value type
			plan.EstimatedRows = int64(stats.Cardinality)
			plan.EstimatedCost = int64(stats.Cardinality) // simple cost model
			plan.Notes = append(plan.Notes, "Using index on field: "+field)
			return plan
		}
	}

	// Full scan
	totalDocs := c.storage.Count()
	plan.Strategy = "full_scan"
	plan.EstimatedRows = int64(totalDocs)
	plan.EstimatedCost = int64(totalDocs)
	plan.Notes = append(plan.Notes, "No suitable index found; falling back to full scan")
	return plan
}

// matchesFilter checks if a document matches all filter criteria
func matchesFilter(doc *document.Document, filter map[string]interface{}) bool {
	for key, value := range filter {
		docValue, exists := doc.Data[key]
		if !exists || docValue != value {
			return false
		}
	}
	return true
}

// FindAll retrieves all documents
func (c *Collection) FindAll() ([]*document.Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.FindAll()
}

// Count returns the number of documents
func (c *Collection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.storage.Count()
}

// Clear removes all documents from the collection
func (c *Collection) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Clear storage
	if err := c.storage.Clear(); err != nil {
		return err
	}

	// Clear indexes
	for _, idx := range c.indexes {
		idx.Clear()
	}

	return nil
}

// SetSchema sets or updates the collection schema
func (c *Collection) SetSchema(schema *document.Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Schema = schema
}

// GetSchema returns the collection schema
func (c *Collection) GetSchema() *document.Schema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Schema
}

// GetQueryStats returns aggregated query statistics for this collection
func (c *Collection) GetQueryStats() QueryStatsSummary {
	if c.queryStats == nil {
		return QueryStatsSummary{}
	}
	return c.queryStats.GetSummary()
}

// GetRecentQueries returns recent query history for this collection
func (c *Collection) GetRecentQueries(limit int) []QueryStats {
	if c.queryStats == nil {
		return nil
	}
	return c.queryStats.GetRecentQueries(c.Name, limit)
}

// GetIndexRecommendations returns auto-index recommendations based on query stats
func (c *Collection) GetIndexRecommendations() []IndexRecommendation {
	advisor := NewIndexAdvisor(DefaultAutoIndexConfig())
	return advisor.Analyze(c)
}

// RecordFieldWrite records a write on a field for cost estimation in auto-indexing
func (c *Collection) RecordFieldWrite(field string) {
	if c.queryStats != nil {
		c.queryStats.RecordWrite(field)
	}
}

// ExplainQuery returns an execution plan for the given filter
func (c *Collection) ExplainQuery(filter map[string]interface{}) ExplainPlan {
	return c.Explain(filter)
}

// collectionMetadata represents persisted collection info
type collectionMetadata struct {
	Name           string           `json:"name"`
	HasSchema      bool             `json:"hasSchema"`
	Schema         *document.Schema `json:"schema,omitempty"`
	Indexes        []IndexMeta      `json:"indexes,omitempty"`
	FulltextFields []string         `json:"fulltextFields,omitempty"` // fields indexed for full-text search
}

// IndexMeta represents index metadata for persistence
type IndexMeta struct {
	Field string            `json:"field"`
	Type  storage.IndexType `json:"type"`
}

// Bucket names
var (
	collectionsBucket = []byte("__collections__")
)

// Manager manages multiple collections
type Manager struct {
	collections map[string]*Collection
	mu          sync.RWMutex
	db          *bolt.DB
	config      *config.Config
	txManager   *transaction.Manager
}

// NewManager creates a new collection manager without persistence
func NewManager() *Manager {
	return &Manager{
		collections: make(map[string]*Collection),
		config:      nil,
		db:          nil,
	}
}

// NewPersistentManager creates a new collection manager with persistence
func NewPersistentManager(cfg *config.Config) (*Manager, error) {
	// Open BoltDB database
	db, err := bolt.Open(cfg.DatabaseFile, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	m := &Manager{
		collections: make(map[string]*Collection),
		config:      cfg,
		db:          db,
	}

	// Load existing collections from database
	if err := m.loadCollections(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load collections: %w", err)
	}

	return m, nil
}

// loadCollections loads all collections from BoltDB
func (m *Manager) loadCollections() error {
	if m.db == nil {
		return nil
	}

	return m.db.View(func(tx *bolt.Tx) error {
		// Get collections metadata bucket
		b := tx.Bucket(collectionsBucket)
		if b == nil {
			return nil // No collections yet
		}

		return b.ForEach(func(k, v []byte) error {
			var meta collectionMetadata
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}

			// Create BoltDB storage for the collection
			store, err := storage.NewBoltDBStorage(m.db, meta.Name)
			if err != nil {
				return fmt.Errorf("failed to create storage for %s: %w", meta.Name, err)
			}

			// Wrap with hybrid storage if cache is enabled
			var finalStore storage.Storage = store
			if m.config != nil && m.config.CacheEnabled {
				finalStore = storage.NewHybridStorageWithBoltDB(store, m.config.CacheSizeMB, m.config.CacheEnabled)
			}

			col := &Collection{
				Name:           meta.Name,
				Schema:         meta.Schema,
				storage:        finalStore,
				indexes:        make(map[string]storage.Index),
				manager:        m,
				fulltextFields: meta.FulltextFields,
			}

			// Recreate full-text index if fields were indexed
			if len(meta.FulltextFields) > 0 {
				col.fulltextIndex = fulltext.NewInvertedIndex()
				docs, err := store.FindAll()
				if err == nil {
					for _, doc := range docs {
						col.indexDocForFullTextLocked(doc)
					}
				}
			}

			// Recreate indexes
			for _, idxMeta := range meta.Indexes {
				var idx storage.Index
				switch idxMeta.Type {
				case storage.IndexTypeBTree:
					idx = storage.NewBTreeIndex(fmt.Sprintf("%s_%s_idx", meta.Name, idxMeta.Field), idxMeta.Field, 64)
				case storage.IndexTypeHash:
					idx = storage.NewHashIndex(fmt.Sprintf("%s_%s_idx", meta.Name, idxMeta.Field), idxMeta.Field)
				}
				if idx != nil {
					// Build index from existing documents
					docs, err := store.FindAll()
					if err == nil {
						for _, doc := range docs {
							if value, exists := doc.Data[idxMeta.Field]; exists {
								idx.Insert(value, doc.ID)
							}
						}
					}
					col.indexes[idxMeta.Field] = idx
				}
			}

			m.collections[meta.Name] = col
			return nil
		})
	})
}

// saveCollections saves collection metadata to BoltDB
func (m *Manager) saveCollections() error {
	if m.db == nil {
		return nil
	}

	return m.db.Update(func(tx *bolt.Tx) error {
		// Create/get collections metadata bucket
		b, err := tx.CreateBucketIfNotExists(collectionsBucket)
		if err != nil {
			return err
		}

		// Clear existing metadata
		keys := make([][]byte, 0)
		b.ForEach(func(k, v []byte) error {
			keys = append(keys, k)
			return nil
		})
		for _, k := range keys {
			b.Delete(k)
		}

		// Save all collections
		for name, col := range m.collections {
			// Collect index metadata
			var indexes []IndexMeta
			for field, idx := range col.indexes {
				indexes = append(indexes, IndexMeta{
					Field: field,
					Type:  idx.Type(),
				})
			}
			
			meta := collectionMetadata{
				Name:           name,
				HasSchema:      col.Schema != nil,
				Schema:         col.Schema,
				Indexes:        indexes,
				FulltextFields: col.GetFullTextFields(),
			}
			data, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(name), data); err != nil {
				return err
			}
		}

		return nil
	})
}

// CreateCollection creates a new collection
func (m *Manager) CreateCollection(name string, schema *document.Schema) (*Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.collections[name]; exists {
		return nil, errors.New("collection already exists: " + name)
	}

	var col *Collection
	
	if m.db != nil {
		// Create with BoltDB storage
		store, err := storage.NewBoltDBStorage(m.db, name)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage: %w", err)
		}
		
		// Wrap with hybrid storage if cache is enabled
		var finalStore storage.Storage = store
		if m.config != nil && m.config.CacheEnabled {
			finalStore = storage.NewHybridStorageWithBoltDB(store, m.config.CacheSizeMB, m.config.CacheEnabled)
		}
		
		col = NewCollectionWithStorage(name, schema, finalStore)
	} else {
		// Create with memory storage
		col = NewCollection(name, schema)
	}

	// Set manager reference for $lookup support
	col.manager = m

	// Set transaction manager if available
	if m.txManager != nil {
		col.SetTransactionManager(m.txManager)
	}

	m.collections[name] = col

	// Save metadata
	if err := m.saveCollections(); err != nil {
		delete(m.collections, name)
		return nil, fmt.Errorf("failed to save collection metadata: %w", err)
	}

	return col, nil
}

// GetCollection retrieves a collection by name
func (m *Manager) GetCollection(name string) (*Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collection, exists := m.collections[name]
	if !exists {
		return nil, errors.New("collection not found: " + name)
	}

	return collection, nil
}

// DropCollection removes a collection
func (m *Manager) DropCollection(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	col, exists := m.collections[name]
	if !exists {
		return errors.New("collection not found: " + name)
	}

	// Close storage if it's HybridStorage
	if hs, ok := col.storage.(*storage.HybridStorage); ok {
		hs.Close()
	}

	// Delete bucket if using BoltDB
	if m.db != nil {
		m.db.Update(func(tx *bolt.Tx) error {
			return tx.DeleteBucket([]byte(name))
		})
	}

	// Delete from memory
	delete(m.collections, name)

	// Update metadata
	if err := m.saveCollections(); err != nil {
		return fmt.Errorf("failed to update collection metadata: %w", err)
	}

	return nil
}

// ListCollections returns a list of all collection names
func (m *Manager) ListCollections() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.collections))
	for name := range m.collections {
		names = append(names, name)
	}
	return names
}

// CollectionExists checks if a collection exists
func (m *Manager) CollectionExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.collections[name]
	return exists
}

// UpdateSchema updates a collection's schema and persists it
func (m *Manager) UpdateSchema(name string, schema *document.Schema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	col, exists := m.collections[name]
	if !exists {
		return errors.New("collection not found: " + name)
	}

	col.Schema = schema

	// Save metadata
	if err := m.saveCollections(); err != nil {
		return fmt.Errorf("failed to save schema: %w", err)
	}

	return nil
}

// Close closes the database connection
func (m *Manager) Close() error {
	// Close all hybrid storage instances
	for _, col := range m.collections {
		if hs, ok := col.storage.(*storage.HybridStorage); ok {
			hs.Close()
		}
	}

	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// GetDB returns the underlying BoltDB instance
func (m *Manager) GetDB() *bolt.DB {
	return m.db
}

// SetTransactionManager sets the transaction manager for all collections
func (m *Manager) SetTransactionManager(tm *transaction.Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.txManager = tm

	// Set transaction manager on all existing collections
	for _, col := range m.collections {
		col.SetTransactionManager(tm)
	}
}

// GetTransactionManager returns the transaction manager
func (m *Manager) GetTransactionManager() *transaction.Manager {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.txManager
}

// ExportData represents the export format for a collection
type ExportData struct {
	Name       string                  `json:"name"`
	HasSchema  bool                    `json:"hasSchema"`
	Schema     *document.Schema        `json:"schema,omitempty"`
	Documents  []*document.Document    `json:"documents"`
	ExportedAt string                  `json:"exportedAt"`
}

// ExportCollection exports a collection to JSON format
func (m *Manager) ExportCollection(name string) (*ExportData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	col, exists := m.collections[name]
	if !exists {
		return nil, errors.New("collection not found: " + name)
	}

	docs, err := col.FindAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get documents: %w", err)
	}

	export := &ExportData{
		Name:       name,
		HasSchema:  col.Schema != nil,
		Schema:     col.Schema,
		Documents:  docs,
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return export, nil
}

// ImportCollection imports a collection from JSON data
func (m *Manager) ImportCollection(data *ExportData, overwrite bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if collection exists
	if _, exists := m.collections[data.Name]; exists {
		if !overwrite {
			return errors.New("collection already exists: " + data.Name)
		}
		// Delete existing collection
		if m.db != nil {
			m.db.Update(func(tx *bolt.Tx) error {
				return tx.DeleteBucket([]byte(data.Name))
			})
		}
		delete(m.collections, data.Name)
	}

	// Create new collection
	var col *Collection
	if m.db != nil {
		store, err := storage.NewBoltDBStorage(m.db, data.Name)
		if err != nil {
			return fmt.Errorf("failed to create storage: %w", err)
		}
		
		// Wrap with hybrid storage if cache is enabled
		var finalStore storage.Storage = store
		if m.config != nil && m.config.CacheEnabled {
			finalStore = storage.NewHybridStorageWithBoltDB(store, m.config.CacheSizeMB, m.config.CacheEnabled)
		}
		
		col = NewCollectionWithStorage(data.Name, data.Schema, finalStore)
	} else {
		col = NewCollection(data.Name, data.Schema)
	}

	// Import documents
	if hybridStore, ok := col.storage.(*storage.HybridStorage); ok {
		if err := hybridStore.ImportDocuments(data.Documents); err != nil {
			return fmt.Errorf("failed to import documents: %w", err)
		}
	} else {
		// Fallback for memory or direct BoltDB storage
		for _, doc := range data.Documents {
			if err := col.storage.Insert(doc); err != nil {
				return fmt.Errorf("failed to import document %s: %w", doc.ID, err)
			}
		}
	}

	// Set manager reference for $lookup support
	col.manager = m

	m.collections[data.Name] = col

	// Save metadata
	if err := m.saveCollections(); err != nil {
		delete(m.collections, data.Name)
		return fmt.Errorf("failed to save collection metadata: %w", err)
	}

	return nil
}

// ImportRawDocuments imports raw documents (for schema-less import)
func (m *Manager) ImportRawDocuments(name string, docs []*document.Document, overwrite bool) error {
	exportData := &ExportData{
		Name:      name,
		HasSchema: false,
		Schema:    nil,
		Documents: docs,
	}
	return m.ImportCollection(exportData, overwrite)
}

// SetTransactionManager sets the transaction manager for this collection
func (c *Collection) SetTransactionManager(tm *transaction.Manager) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txManager = tm
}

// GetTransactionManager returns the transaction manager for this collection
func (c *Collection) GetTransactionManager() *transaction.Manager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.txManager
}

// InsertTx inserts a document within an existing transaction.
// With deferred writes the document is NOT written to storage here;
// it is buffered and flushed only when the transaction commits.
func (c *Collection) InsertTx(tx *transaction.Transaction, doc *document.Document) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !tx.IsActive() {
		return fmt.Errorf("transaction is not active")
	}

	// Validate against schema
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	// Check write buffer for duplicate within this transaction
	if _, deleted, found := tx.GetFromWriteBuffer(c.Name, doc.ID); found {
		if !deleted {
			return storage.ErrDocumentExists
		}
		// Was deleted earlier in this tx — re-insert is allowed
	} else {
		// Not in buffer — check actual storage
		if _, getErr := c.storage.Get(doc.ID); getErr == nil {
			return storage.ErrDocumentExists
		}
	}

	// Record operation in transaction (writes to WAL only)
	op := transaction.Operation{
		Type:       transaction.OpInsert,
		Collection: c.Name,
		DocumentID: doc.ID,
		OldValue:   nil,
		NewValue:   doc,
	}
	if err := tx.AddOperation(op); err != nil {
		return fmt.Errorf("failed to record operation: %w", err)
	}

	// Storage write deferred to commit via StorageApplier
	return nil
}

// UpdateTx updates a document within an existing transaction.
// With deferred writes the update is buffered, not applied to storage.
func (c *Collection) UpdateTx(tx *transaction.Transaction, doc *document.Document) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !tx.IsActive() {
		return fmt.Errorf("transaction is not active")
	}

	// Get old document — check write buffer first, then storage
	var oldDoc *document.Document
	if bufferedDoc, deleted, found := tx.GetFromWriteBuffer(c.Name, doc.ID); found {
		if deleted {
			return storage.ErrDocumentNotFound
		}
		oldDoc = bufferedDoc // already a deep copy
	} else {
		var err error
		oldDoc, err = c.storage.Get(doc.ID)
		if err != nil {
			return err
		}
	}

	// Validate against schema
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	// Record deferred operation (WAL only)
	op := transaction.Operation{
		Type:       transaction.OpUpdate,
		Collection: c.Name,
		DocumentID: doc.ID,
		OldValue:   oldDoc,
		NewValue:   doc,
	}
	if err := tx.AddOperation(op); err != nil {
		return fmt.Errorf("failed to record operation: %w", err)
	}

	// Storage write deferred to commit
	return nil
}

// DeleteTx deletes a document within an existing transaction.
// With deferred writes the delete is buffered, not applied to storage.
func (c *Collection) DeleteTx(tx *transaction.Transaction, id string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !tx.IsActive() {
		return fmt.Errorf("transaction is not active")
	}

	// Get document for undo info — check write buffer first, then storage
	var doc *document.Document
	if bufferedDoc, deleted, found := tx.GetFromWriteBuffer(c.Name, id); found {
		if deleted {
			return storage.ErrDocumentNotFound
		}
		doc = bufferedDoc
	} else {
		var err error
		doc, err = c.storage.Get(id)
		if err != nil {
			return err
		}
	}

	// Record deferred delete (WAL only)
	op := transaction.Operation{
		Type:       transaction.OpDelete,
		Collection: c.Name,
		DocumentID: id,
		OldValue:   doc,
		NewValue:   nil,
	}
	if err := tx.AddOperation(op); err != nil {
		return fmt.Errorf("failed to record operation: %w", err)
	}

	// Storage write deferred to commit
	return nil
}

// InsertWithAutoTx inserts a document with automatic transaction wrapping
// If the operation fails, all changes are rolled back (ACID compliant)
func (c *Collection) InsertWithAutoTx(doc *document.Document) error {
	if c.txManager == nil {
		// Fallback to non-transactional insert
		return c.Insert(doc)
	}

	return c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		return c.InsertTx(tx, doc)
	})
}

// UpdateWithAutoTx updates a document with automatic transaction wrapping
// If the operation fails, all changes are rolled back (ACID compliant)
func (c *Collection) UpdateWithAutoTx(doc *document.Document) error {
	if c.txManager == nil {
		// Fallback to non-transactional update
		return c.Update(doc)
	}

	return c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		return c.UpdateTx(tx, doc)
	})
}

// DeleteWithAutoTx deletes a document with automatic transaction wrapping
// If the operation fails, all changes are rolled back (ACID compliant)
func (c *Collection) DeleteWithAutoTx(id string) error {
	if c.txManager == nil {
		// Fallback to non-transactional delete
		return c.Delete(id)
	}

	return c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		return c.DeleteTx(tx, id)
	})
}

// PatchWithAutoTx patches a document with automatic transaction wrapping.
// With deferred writes the patch is buffered, not applied to storage.
func (c *Collection) PatchWithAutoTx(id string, data map[string]interface{}) (*document.Document, error) {
	if c.txManager == nil {
		return c.Patch(id, data)
	}

	var result *document.Document
	err := c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		c.mu.RLock()
		defer c.mu.RUnlock()

		if !tx.IsActive() {
			return fmt.Errorf("transaction is not active")
		}

		// Get existing document — check write buffer first, then storage
		var doc *document.Document
		if bufferedDoc, deleted, found := tx.GetFromWriteBuffer(c.Name, id); found {
			if deleted {
				return storage.ErrDocumentNotFound
			}
			doc = bufferedDoc // already a deep copy
		} else {
			var err error
			doc, err = c.storage.Get(id)
			if err != nil {
				return err
			}
		}

		// Create a copy for the old value before merging
		oldDocCopy := &document.Document{
			ID:        doc.ID,
			CreatedAt: doc.CreatedAt,
			UpdatedAt: doc.UpdatedAt,
			Data:      make(map[string]interface{}),
		}
		for k, v := range doc.Data {
			oldDocCopy.Data[k] = v
		}

		// Merge the new data
		doc.MergeData(data)

		// Validate against schema
		if c.Schema != nil {
			if err := c.Schema.Validate(doc); err != nil {
				return err
			}
		}

		// Record deferred operation (WAL only)
		op := transaction.Operation{
			Type:       transaction.OpUpdate,
			Collection: c.Name,
			DocumentID: doc.ID,
			OldValue:   oldDocCopy,
			NewValue:   doc,
		}
		if err := tx.AddOperation(op); err != nil {
			return fmt.Errorf("failed to record operation: %w", err)
		}

		// Storage write deferred to commit
		result = doc
		return nil
	})

	return result, err
}

// BulkInsertResult represents the result of a bulk insert operation
type BulkInsertResult struct {
	InsertedIDs []string `json:"insertedIds"`
	Count       int      `json:"count"`
	Errors      []error  `json:"errors,omitempty"`
}

// BulkInsert inserts multiple documents in a single transaction.
// With deferred writes all inserts are buffered and flushed at commit.
func (c *Collection) BulkInsert(docs []*document.Document) (*BulkInsertResult, error) {
	if c.txManager == nil {
		return c.bulkInsertNonTransactional(docs)
	}

	var result *BulkInsertResult
	err := c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		c.mu.RLock()
		defer c.mu.RUnlock()

		if !tx.IsActive() {
			return fmt.Errorf("transaction is not active")
		}

		result = &BulkInsertResult{
			InsertedIDs: make([]string, 0, len(docs)),
			Errors:      make([]error, 0),
		}

		// Validate all documents first
		for i, doc := range docs {
			if c.Schema != nil {
				if err := c.Schema.Validate(doc); err != nil {
					return fmt.Errorf("document %d validation failed: %w", i, err)
				}
			}
		}

		// Buffer all inserts (deferred writes)
		for i, doc := range docs {
			// Check for duplicates in write buffer (catches dupes within the batch)
			if _, deleted, found := tx.GetFromWriteBuffer(c.Name, doc.ID); found && !deleted {
				return fmt.Errorf("duplicate document ID in batch: %s", doc.ID)
			}
			// Check for existing in storage
			if _, getErr := c.storage.Get(doc.ID); getErr == nil {
				return fmt.Errorf("document %d (id=%s) already exists", i, doc.ID)
			}

			op := transaction.Operation{
				Type:       transaction.OpInsert,
				Collection: c.Name,
				DocumentID: doc.ID,
				OldValue:   nil,
				NewValue:   doc,
			}
			if err := tx.AddOperation(op); err != nil {
				return fmt.Errorf("failed to record operation for doc %d: %w", i, err)
			}

			result.InsertedIDs = append(result.InsertedIDs, doc.ID)
			result.Count++
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// bulkInsertNonTransactional performs bulk insert without transactions
func (c *Collection) bulkInsertNonTransactional(docs []*document.Document) (*BulkInsertResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := &BulkInsertResult{
		InsertedIDs: make([]string, 0, len(docs)),
		Errors:      make([]error, 0),
	}

	for i, doc := range docs {
		if c.Schema != nil {
			if err := c.Schema.Validate(doc); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("document %d: %w", i, err))
				continue
			}
		}

		if err := c.storage.Insert(doc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("document %d: %w", i, err))
			continue
		}

		// Update indexes
		for field, idx := range c.indexes {
			if value, exists := doc.Data[field]; exists {
				idx.Insert(value, doc.ID)
			}
		}

		c.indexDocForFullTextLocked(doc)
		result.InsertedIDs = append(result.InsertedIDs, doc.ID)
		result.Count++
	}

	return result, nil
}

// BulkUpdateResult represents the result of a bulk update operation
type BulkUpdateResult struct {
	UpdatedIDs []string `json:"updatedIds"`
	Count      int      `json:"count"`
	Errors     []error  `json:"errors,omitempty"`
}

// BulkUpdateRequest represents a single document update in a bulk operation
type BulkUpdateRequest struct {
	ID   string                 `json:"_id"`
	Data map[string]interface{} `json:"data"`
}

// BulkUpdate updates multiple documents in a single transaction.
// With deferred writes all updates are buffered and flushed at commit.
func (c *Collection) BulkUpdate(updates []*BulkUpdateRequest) (*BulkUpdateResult, error) {
	if c.txManager == nil {
		return c.bulkUpdateNonTransactional(updates)
	}

	var result *BulkUpdateResult
	err := c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		c.mu.RLock()
		defer c.mu.RUnlock()

		if !tx.IsActive() {
			return fmt.Errorf("transaction is not active")
		}

		result = &BulkUpdateResult{
			UpdatedIDs: make([]string, 0, len(updates)),
			Errors:     make([]error, 0),
		}

		for i, update := range updates {
			// Get existing doc — check write buffer first, then storage
			var oldDoc *document.Document
			if bufferedDoc, deleted, found := tx.GetFromWriteBuffer(c.Name, update.ID); found {
				if deleted {
					return fmt.Errorf("document %d (id=%s) was deleted in this transaction", i, update.ID)
				}
				oldDoc = bufferedDoc
			} else {
				var err error
				oldDoc, err = c.storage.Get(update.ID)
				if err != nil {
					return fmt.Errorf("document %d (id=%s) not found: %w", i, update.ID, err)
				}
			}

			// Build new document
			newDoc := &document.Document{
				ID:        oldDoc.ID,
				CreatedAt: oldDoc.CreatedAt,
				UpdatedAt: oldDoc.UpdatedAt,
				Data:      make(map[string]interface{}),
			}
			for k, v := range oldDoc.Data {
				newDoc.Data[k] = v
			}
			for k, v := range update.Data {
				newDoc.Data[k] = v
			}
			newDoc.Update(newDoc.Data)

			// Validate
			if c.Schema != nil {
				if err := c.Schema.Validate(newDoc); err != nil {
					return fmt.Errorf("document %d (id=%s) validation failed: %w", i, update.ID, err)
				}
			}

			// Record deferred update (WAL only)
			op := transaction.Operation{
				Type:       transaction.OpUpdate,
				Collection: c.Name,
				DocumentID: newDoc.ID,
				OldValue:   oldDoc,
				NewValue:   newDoc,
			}
			if err := tx.AddOperation(op); err != nil {
				return fmt.Errorf("failed to record operation for doc %d: %w", i, err)
			}

			result.UpdatedIDs = append(result.UpdatedIDs, newDoc.ID)
			result.Count++
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// bulkUpdateNonTransactional performs bulk update without transactions
func (c *Collection) bulkUpdateNonTransactional(updates []*BulkUpdateRequest) (*BulkUpdateResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := &BulkUpdateResult{
		UpdatedIDs: make([]string, 0, len(updates)),
		Errors:     make([]error, 0),
	}

	for i, update := range updates {
		oldDoc, err := c.storage.Get(update.ID)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("document %d (id=%s): %w", i, update.ID, err))
			continue
		}

		newDoc := &document.Document{
			ID:        oldDoc.ID,
			CreatedAt: oldDoc.CreatedAt,
			UpdatedAt: oldDoc.UpdatedAt,
			Data:      make(map[string]interface{}),
		}
		for k, v := range oldDoc.Data {
			newDoc.Data[k] = v
		}
		for k, v := range update.Data {
			newDoc.Data[k] = v
		}
		newDoc.Update(newDoc.Data)

		if c.Schema != nil {
			if err := c.Schema.Validate(newDoc); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("document %d: %w", i, err))
				continue
			}
		}

		if err := c.storage.Update(newDoc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("document %d: %w", i, err))
			continue
		}

		// Update indexes
		for field, idx := range c.indexes {
			if oldValue, exists := oldDoc.Data[field]; exists {
				idx.Delete(oldValue, newDoc.ID)
			}
			if newValue, exists := newDoc.Data[field]; exists {
				idx.Insert(newValue, newDoc.ID)
			}
		}

		if c.fulltextIndex != nil {
			c.indexDocForFullTextLocked(newDoc)
		}

		result.UpdatedIDs = append(result.UpdatedIDs, newDoc.ID)
		result.Count++
	}

	return result, nil
}

// BulkDeleteResult represents the result of a bulk delete operation
type BulkDeleteResult struct {
	DeletedIDs []string `json:"deletedIds"`
	Count      int      `json:"count"`
	Errors     []error  `json:"errors,omitempty"`
}

// BulkDelete deletes multiple documents in a single transaction.
// With deferred writes all deletes are buffered and flushed at commit.
func (c *Collection) BulkDelete(ids []string) (*BulkDeleteResult, error) {
	if c.txManager == nil {
		return c.bulkDeleteNonTransactional(ids)
	}

	var result *BulkDeleteResult
	err := c.txManager.AutoTransaction(func(tx *transaction.Transaction) error {
		c.mu.RLock()
		defer c.mu.RUnlock()

		if !tx.IsActive() {
			return fmt.Errorf("transaction is not active")
		}

		result = &BulkDeleteResult{
			DeletedIDs: make([]string, 0, len(ids)),
			Errors:     make([]error, 0),
		}

		for i, id := range ids {
			// Get doc for undo info — check buffer first, then storage
			var doc *document.Document
			if bufferedDoc, deleted, found := tx.GetFromWriteBuffer(c.Name, id); found {
				if deleted {
					return fmt.Errorf("document %d (id=%s) already deleted in this transaction", i, id)
				}
				doc = bufferedDoc
			} else {
				var err error
				doc, err = c.storage.Get(id)
				if err != nil {
					return fmt.Errorf("document %d (id=%s) not found: %w", i, id, err)
				}
			}

			// Record deferred delete (WAL only)
			op := transaction.Operation{
				Type:       transaction.OpDelete,
				Collection: c.Name,
				DocumentID: id,
				OldValue:   doc,
				NewValue:   nil,
			}
			if err := tx.AddOperation(op); err != nil {
				return fmt.Errorf("failed to record operation for doc %d: %w", i, err)
			}

			result.DeletedIDs = append(result.DeletedIDs, id)
			result.Count++
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// bulkDeleteNonTransactional performs bulk delete without transactions
func (c *Collection) bulkDeleteNonTransactional(ids []string) (*BulkDeleteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := &BulkDeleteResult{
		DeletedIDs: make([]string, 0, len(ids)),
		Errors:     make([]error, 0),
	}

	for i, id := range ids {
		doc, err := c.storage.Get(id)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("document %d (id=%s): %w", i, id, err))
			continue
		}

		if err := c.storage.Delete(id); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("document %d: %w", i, err))
			continue
		}

		// Remove from indexes
		for field, idx := range c.indexes {
			if value, exists := doc.Data[field]; exists {
				idx.Delete(value, id)
			}
		}

		if c.fulltextIndex != nil {
			c.fulltextIndex.RemoveDocument(id)
		}

		result.DeletedIDs = append(result.DeletedIDs, id)
		result.Count++
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// Deferred-write commit helpers (called by StorageApplier during commit)
// ---------------------------------------------------------------------------

// ApplyOperation applies a single transaction operation to storage, indexes,
// and full-text. Called during commit to flush deferred writes.
// Acquires c.mu internally — caller must NOT hold it.
func (c *Collection) ApplyOperation(op transaction.Operation) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch op.Type {
	case transaction.OpInsert:
		if err := c.storage.Insert(op.NewValue); err != nil {
			return err
		}
		for field, idx := range c.indexes {
			if value, exists := op.NewValue.Data[field]; exists {
				idx.Insert(value, op.NewValue.ID)
			}
		}
		c.indexDocForFullTextLocked(op.NewValue)

	case transaction.OpUpdate:
		if err := c.storage.Update(op.NewValue); err != nil {
			return err
		}
		for field, idx := range c.indexes {
			if op.OldValue != nil {
				if oldVal, exists := op.OldValue.Data[field]; exists {
					idx.Delete(oldVal, op.NewValue.ID)
				}
			}
			if newVal, exists := op.NewValue.Data[field]; exists {
				idx.Insert(newVal, op.NewValue.ID)
			}
		}
		if c.fulltextIndex != nil {
			c.indexDocForFullTextLocked(op.NewValue)
		}

	case transaction.OpDelete:
		if err := c.storage.Delete(op.DocumentID); err != nil {
			return err
		}
		if op.OldValue != nil {
			for field, idx := range c.indexes {
				if value, exists := op.OldValue.Data[field]; exists {
					idx.Delete(value, op.DocumentID)
				}
			}
			if c.fulltextIndex != nil {
				c.fulltextIndex.RemoveDocument(op.DocumentID)
			}
		}
	}

	return nil
}

// UndoOperation reverses a previously applied operation (best-effort).
// Used when a commit fails partway through flushing to storage.
func (c *Collection) UndoOperation(op transaction.Operation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch op.Type {
	case transaction.OpInsert:
		// Undo insert → delete
		_ = c.storage.Delete(op.DocumentID)
		if op.NewValue != nil {
			for field, idx := range c.indexes {
				if value, exists := op.NewValue.Data[field]; exists {
					idx.Delete(value, op.DocumentID)
				}
			}
			if c.fulltextIndex != nil {
				c.fulltextIndex.RemoveDocument(op.DocumentID)
			}
		}

	case transaction.OpUpdate:
		// Undo update → restore old value
		if op.OldValue != nil {
			_ = c.storage.Update(op.OldValue)
			for field, idx := range c.indexes {
				if op.NewValue != nil {
					if newVal, exists := op.NewValue.Data[field]; exists {
						idx.Delete(newVal, op.DocumentID)
					}
				}
				if oldVal, exists := op.OldValue.Data[field]; exists {
					idx.Insert(oldVal, op.DocumentID)
				}
			}
			if c.fulltextIndex != nil {
				c.indexDocForFullTextLocked(op.OldValue)
			}
		}

	case transaction.OpDelete:
		// Undo delete → re-insert old value
		if op.OldValue != nil {
			_ = c.storage.Insert(op.OldValue)
			for field, idx := range c.indexes {
				if value, exists := op.OldValue.Data[field]; exists {
					idx.Insert(value, op.DocumentID)
				}
			}
			if c.fulltextIndex != nil {
				c.indexDocForFullTextLocked(op.OldValue)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// StorageApplier — implements transaction.StorageApplier
// ---------------------------------------------------------------------------

type appliedOp struct {
	collection *Collection
	op         transaction.Operation
}

// collectionStorageApplier implements transaction.StorageApplier using Manager.
type collectionStorageApplier struct {
	manager *Manager
}

// NewStorageApplier creates a transaction.StorageApplier backed by the collection manager.
func NewStorageApplier(m *Manager) transaction.StorageApplier {
	return &collectionStorageApplier{manager: m}
}

func isDocumentOp(t transaction.OperationType) bool {
	return t == transaction.OpInsert || t == transaction.OpUpdate || t == transaction.OpDelete
}

func (a *collectionStorageApplier) ApplyOperations(ops []transaction.Operation) error {
	applied := make([]appliedOp, 0, len(ops))

	for _, op := range ops {
		if !isDocumentOp(op.Type) {
			continue
		}
		col, err := a.manager.GetCollection(op.Collection)
		if err != nil {
			a.undoApplied(applied)
			return fmt.Errorf("collection %s not found: %w", op.Collection, err)
		}
		if err := col.ApplyOperation(op); err != nil {
			a.undoApplied(applied)
			return fmt.Errorf("apply %s on %s/%s: %w", op.Type, op.Collection, op.DocumentID, err)
		}
		applied = append(applied, appliedOp{collection: col, op: op})
	}
	return nil
}

func (a *collectionStorageApplier) UndoOperations(ops []transaction.Operation) {
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		if !isDocumentOp(op.Type) {
			continue
		}
		col, err := a.manager.GetCollection(op.Collection)
		if err != nil {
			continue
		}
		col.UndoOperation(op)
	}
}

func (a *collectionStorageApplier) undoApplied(applied []appliedOp) {
	for i := len(applied) - 1; i >= 0; i-- {
		applied[i].collection.UndoOperation(applied[i].op)
	}
}

// ---------------------------------------------------------------------------
// RecoveryApplier — implements wal.RecoveryStorageApplier for crash recovery
// ---------------------------------------------------------------------------

// RecoveryApplier applies WAL entries to storage during crash recovery.
// All methods are idempotent: duplicate applies are safe.
type RecoveryApplier struct {
	manager *Manager
}

// NewRecoveryApplier creates a RecoveryApplier backed by the collection manager.
func NewRecoveryApplier(m *Manager) *RecoveryApplier {
	return &RecoveryApplier{manager: m}
}

func (ra *RecoveryApplier) ApplyRecoveryInsert(collectionName string, doc *document.Document) error {
	col, err := ra.manager.GetCollection(collectionName)
	if err != nil {
		log.Printf("[Recovery] collection %s not found, skipping insert for %s", collectionName, doc.ID)
		return nil
	}
	col.mu.Lock()
	defer col.mu.Unlock()

	// Idempotent: if doc already exists, update to ensure correct state
	if existing, getErr := col.storage.Get(doc.ID); getErr == nil {
		_ = col.storage.Update(doc)
		for field, idx := range col.indexes {
			if existing != nil {
				if oldVal, ok := existing.Data[field]; ok {
					idx.Delete(oldVal, doc.ID)
				}
			}
			if newVal, ok := doc.Data[field]; ok {
				idx.Insert(newVal, doc.ID)
			}
		}
		return nil
	}

	if err := col.storage.Insert(doc); err != nil {
		return err
	}
	for field, idx := range col.indexes {
		if value, ok := doc.Data[field]; ok {
			idx.Insert(value, doc.ID)
		}
	}
	col.indexDocForFullTextLocked(doc)
	return nil
}

func (ra *RecoveryApplier) ApplyRecoveryUpdate(collectionName string, doc *document.Document) error {
	col, err := ra.manager.GetCollection(collectionName)
	if err != nil {
		log.Printf("[Recovery] collection %s not found, skipping update for %s", collectionName, doc.ID)
		return nil
	}
	col.mu.Lock()
	defer col.mu.Unlock()

	existing, _ := col.storage.Get(doc.ID)
	if existing != nil {
		if err := col.storage.Update(doc); err != nil {
			return err
		}
		for field, idx := range col.indexes {
			if oldVal, ok := existing.Data[field]; ok {
				idx.Delete(oldVal, doc.ID)
			}
			if newVal, ok := doc.Data[field]; ok {
				idx.Insert(newVal, doc.ID)
			}
		}
	} else {
		// Doc missing — re-insert
		if err := col.storage.Insert(doc); err != nil {
			return err
		}
		for field, idx := range col.indexes {
			if value, ok := doc.Data[field]; ok {
				idx.Insert(value, doc.ID)
			}
		}
	}

	if col.fulltextIndex != nil {
		col.indexDocForFullTextLocked(doc)
	}
	return nil
}

func (ra *RecoveryApplier) ApplyRecoveryDelete(collectionName string, docID string) error {
	col, err := ra.manager.GetCollection(collectionName)
	if err != nil {
		log.Printf("[Recovery] collection %s not found, skipping delete for %s", collectionName, docID)
		return nil
	}
	col.mu.Lock()
	defer col.mu.Unlock()

	existing, _ := col.storage.Get(docID)
	if existing == nil {
		return nil // Already deleted — idempotent
	}

	if err := col.storage.Delete(docID); err != nil {
		return err
	}
	for field, idx := range col.indexes {
		if value, ok := existing.Data[field]; ok {
			idx.Delete(value, docID)
		}
	}
	if col.fulltextIndex != nil {
		col.fulltextIndex.RemoveDocument(docID)
	}
	return nil
}
