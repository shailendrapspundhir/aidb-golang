package collection

import (
	"aidb/internal/config"
	"aidb/internal/document"
	"aidb/internal/storage"
	"encoding/json"
	"errors"
	"fmt"
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
}

// NewCollection creates a new collection with memory storage
func NewCollection(name string, schema *document.Schema) *Collection {
	return &Collection{
		Name:    name,
		Schema:  schema,
		storage: storage.NewMemoryStorage(),
		indexes: make(map[string]storage.Index),
	}
}

// NewCollectionWithStorage creates a new collection with a custom storage backend
func NewCollectionWithStorage(name string, schema *document.Schema, store storage.Storage) *Collection {
	return &Collection{
		Name:    name,
		Schema:  schema,
		storage: store,
		indexes: make(map[string]storage.Index),
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

	return nil
}

// Find retrieves documents matching a filter
func (c *Collection) Find(filter map[string]interface{}) ([]*document.Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

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

// collectionMetadata represents persisted collection info
type collectionMetadata struct {
	Name      string           `json:"name"`
	HasSchema bool             `json:"hasSchema"`
	Schema    *document.Schema `json:"schema,omitempty"`
	Indexes   []IndexMeta      `json:"indexes,omitempty"`
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
				Name:    meta.Name,
				Schema:  meta.Schema,
				storage: finalStore,
				indexes: make(map[string]storage.Index),
				manager: m,
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
				Name:      name,
				HasSchema: col.Schema != nil,
				Schema:    col.Schema,
				Indexes:   indexes,
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
