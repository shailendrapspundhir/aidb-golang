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

// UnifiedManager manages multiple unified collections
type UnifiedManager struct {
	collections map[string]*UnifiedCollection
	mu          sync.RWMutex
	db          *bolt.DB
	config      *config.Config
}

// NewUnifiedManager creates a new unified collection manager without persistence
func NewUnifiedManager() *UnifiedManager {
	return &UnifiedManager{
		collections: make(map[string]*UnifiedCollection),
		config:      nil,
		db:          nil,
	}
}

// NewPersistentUnifiedManager creates a new unified collection manager with persistence
// DEPRECATED: Use NewUnifiedManagerWithDB to share the existing database connection
func NewPersistentUnifiedManager(cfg *config.Config) (*UnifiedManager, error) {
	// Open BoltDB database
	db, err := bolt.Open(cfg.DatabaseFile, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	m := &UnifiedManager{
		collections: make(map[string]*UnifiedCollection),
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

// NewUnifiedManagerWithDB creates a new unified collection manager using an existing database connection
func NewUnifiedManagerWithDB(cfg *config.Config, db *bolt.DB) (*UnifiedManager, error) {
	m := &UnifiedManager{
		collections: make(map[string]*UnifiedCollection),
		config:      cfg,
		db:          db,
	}

	// Load existing collections from database
	if err := m.loadCollections(); err != nil {
		return nil, fmt.Errorf("failed to load collections: %w", err)
	}

	return m, nil
}

// loadCollections loads all collections from BoltDB
func (m *UnifiedManager) loadCollections() error {
	if m.db == nil {
		return nil
	}

	return m.db.View(func(tx *bolt.Tx) error {
		// Get collections metadata bucket
		b := tx.Bucket(unifiedCollectionsBucket)
		if b == nil {
			return nil // No collections yet
		}

		return b.ForEach(func(k, v []byte) error {
			var meta unifiedCollectionMetadata
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

			// Create unified collection
			col, err := NewUnifiedCollection(UnifiedCollectionConfig{
				Name:    meta.Name,
				Schema:  meta.Schema,
				Storage: finalStore,
			})
			if err != nil {
				return fmt.Errorf("failed to create collection %s: %w", meta.Name, err)
			}

			m.collections[meta.Name] = col
			return nil
		})
	})
}

// saveCollections saves collection metadata to BoltDB
func (m *UnifiedManager) saveCollections() error {
	if m.db == nil {
		return nil
	}

	return m.db.Update(func(tx *bolt.Tx) error {
		// Create/get collections metadata bucket
		b, err := tx.CreateBucketIfNotExists(unifiedCollectionsBucket)
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
			meta := unifiedCollectionMetadata{
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

// CreateCollection creates a new unified collection
func (m *UnifiedManager) CreateCollection(name string, schema *document.UnifiedSchema) (*UnifiedCollection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.collections[name]; exists {
		return nil, errors.New("collection already exists: " + name)
	}

	var col *UnifiedCollection

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

		col, err = NewUnifiedCollection(UnifiedCollectionConfig{
			Name:    name,
			Schema:  schema,
			Storage: finalStore,
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Create with memory storage
		col, _ = NewUnifiedCollection(UnifiedCollectionConfig{
			Name:    name,
			Schema:  schema,
			Storage: storage.NewMemoryStorage(),
		})
	}

	m.collections[name] = col

	// Save metadata
	if err := m.saveCollections(); err != nil {
		delete(m.collections, name)
		return nil, fmt.Errorf("failed to save collection metadata: %w", err)
	}

	return col, nil
}

// GetCollection retrieves a unified collection by name
func (m *UnifiedManager) GetCollection(name string) (*UnifiedCollection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collection, exists := m.collections[name]
	if !exists {
		return nil, errors.New("collection not found: " + name)
	}

	return collection, nil
}

// DropCollection removes a unified collection
func (m *UnifiedManager) DropCollection(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	col, exists := m.collections[name]
	if !exists {
		return errors.New("collection not found: " + name)
	}

	// Close storage if it's HybridStorage
	if hs, ok := col.docStorage.(*storage.HybridStorage); ok {
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
func (m *UnifiedManager) ListCollections() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.collections))
	for name := range m.collections {
		names = append(names, name)
	}
	return names
}

// CollectionExists checks if a collection exists
func (m *UnifiedManager) CollectionExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.collections[name]
	return exists
}

// UpdateSchema updates a collection's schema and persists it
func (m *UnifiedManager) UpdateSchema(name string, schema *document.UnifiedSchema) error {
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

// Close closes the manager (does not close the shared DB)
func (m *UnifiedManager) Close() error {
	// Close all hybrid storage instances
	for _, col := range m.collections {
		if hs, ok := col.docStorage.(*storage.HybridStorage); ok {
			hs.Close()
		}
	}

	// Don't close the shared DB - it's managed by the collection manager
	return nil
}

// GetDB returns the underlying BoltDB instance
func (m *UnifiedManager) GetDB() *bolt.DB {
	return m.db
}

// ExportData represents the export format for a unified collection
type UnifiedExportData struct {
	Name       string                       `json:"name"`
	HasSchema  bool                         `json:"hasSchema"`
	Schema     *document.UnifiedSchema      `json:"schema,omitempty"`
	Documents  []*document.UnifiedDocument  `json:"documents"`
	ExportedAt string                       `json:"exportedAt"`
}

// ExportCollection exports a unified collection to JSON format
func (m *UnifiedManager) ExportCollection(name string) (*UnifiedExportData, error) {
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

	// Load vectors for all documents
	for _, doc := range docs {
		for fieldName, vecStore := range col.vectorStores {
			if vec, err := vecStore.Get(doc.ID); err == nil {
				doc.SetVector(fieldName, vec)
			}
		}
	}

	export := &UnifiedExportData{
		Name:       name,
		HasSchema:  col.Schema != nil,
		Schema:     col.Schema,
		Documents:  docs,
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return export, nil
}

// ImportCollection imports a unified collection from JSON data
func (m *UnifiedManager) ImportCollection(data *UnifiedExportData, overwrite bool) error {
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
	var col *UnifiedCollection
	var err error

	if m.db != nil {
		store, err := storage.NewBoltDBStorage(m.db, data.Name)
		if err != nil {
			return fmt.Errorf("failed to create storage: %w", err)
		}

		var finalStore storage.Storage = store
		if m.config != nil && m.config.CacheEnabled {
			finalStore = storage.NewHybridStorageWithBoltDB(store, m.config.CacheSizeMB, m.config.CacheEnabled)
		}

		col, err = NewUnifiedCollection(UnifiedCollectionConfig{
			Name:    data.Name,
			Schema:  data.Schema,
			Storage: finalStore,
		})
	} else {
		col, err = NewUnifiedCollection(UnifiedCollectionConfig{
			Name:    data.Name,
			Schema:  data.Schema,
			Storage: storage.NewMemoryStorage(),
		})
	}

	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	// Import documents
	for _, doc := range data.Documents {
		if err := col.Insert(doc); err != nil {
			continue // Skip duplicates/errors
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

// unifiedCollectionMetadata represents persisted unified collection info
type unifiedCollectionMetadata struct {
	Name      string                    `json:"name"`
	HasSchema bool                      `json:"hasSchema"`
	Schema    *document.UnifiedSchema   `json:"schema,omitempty"`
}

// Bucket names
var unifiedCollectionsBucket = []byte("__unified_collections__")
