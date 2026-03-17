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
	mu      sync.RWMutex
}

// NewCollection creates a new collection with memory storage
func NewCollection(name string, schema *document.Schema) *Collection {
	return &Collection{
		Name:    name,
		Schema:  schema,
		storage: storage.NewMemoryStorage(),
	}
}

// NewCollectionWithStorage creates a new collection with a custom storage backend
func NewCollectionWithStorage(name string, schema *document.Schema, store storage.Storage) *Collection {
	return &Collection{
		Name:    name,
		Schema:  schema,
		storage: store,
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

	return c.storage.Insert(doc)
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

	// Validate against schema if one exists
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	return c.storage.Update(doc)
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

	return doc, nil
}

// Delete removes a document by ID
func (c *Collection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.storage.Delete(id)
}

// Find retrieves documents matching a filter
func (c *Collection) Find(filter map[string]interface{}) ([]*document.Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.Find(filter)
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
	return c.storage.Clear()
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

// NewPersistentManager creates a new collection manager with BoltDB persistence
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

			col := &Collection{
				Name:    meta.Name,
				Schema:  meta.Schema,
				storage: store,
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
			meta := collectionMetadata{
				Name:      name,
				HasSchema: col.Schema != nil,
				Schema:    col.Schema,
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
		col = NewCollectionWithStorage(name, schema, store)
	} else {
		// Create with memory storage
		col = NewCollection(name, schema)
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

	if _, exists := m.collections[name]; !exists {
		return errors.New("collection not found: " + name)
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
	if m.db != nil {
		return m.db.Close()
	}
	return nil
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
		col = NewCollectionWithStorage(data.Name, data.Schema, store)
	} else {
		col = NewCollection(data.Name, data.Schema)
	}

	// Import documents
	if boltStore, ok := col.storage.(*storage.BoltDBStorage); ok {
		if err := boltStore.ImportDocuments(data.Documents); err != nil {
			return fmt.Errorf("failed to import documents: %w", err)
		}
	} else {
		// Fallback for memory storage
		for _, doc := range data.Documents {
			if err := col.storage.Insert(doc); err != nil {
				return fmt.Errorf("failed to import document %s: %w", doc.ID, err)
			}
		}
	}

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
