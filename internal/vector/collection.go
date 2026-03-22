package vector

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// VectorCollection represents a collection of vector documents
type VectorCollection struct {
	Name    string
	Config  *VectorConfig
	storage VectorStorage
	mu      sync.RWMutex
}

// NewVectorCollection creates a new vector collection with HNSW storage
func NewVectorCollection(name string, config *VectorConfig) (*VectorCollection, error) {
	// Use HNSW storage by default for better scalability
	storage, err := NewHNSWVectorStorage(config)
	if err != nil {
		return nil, err
	}

	return &VectorCollection{
		Name:    name,
		Config:  config,
		storage: storage,
	}, nil
}

// NewVectorCollectionWithStorage creates a new vector collection with custom storage
func NewVectorCollectionWithStorage(name string, config *VectorConfig, storage VectorStorage) *VectorCollection {
	return &VectorCollection{
		Name:    name,
		Config:  config,
		storage: storage,
	}
}

// Insert stores a new vector document in the collection
func (c *VectorCollection) Insert(doc *VectorDocument) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate dimensions
	if len(doc.Vector) != c.Config.Dimensions {
		return ErrDimensionMismatch
	}

	return c.storage.Insert(doc)
}

// Get retrieves a vector document by ID
func (c *VectorCollection) Get(id string) (*VectorDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.Get(id)
}

// Update updates an existing vector document
func (c *VectorCollection) Update(doc *VectorDocument) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate dimensions
	if len(doc.Vector) != c.Config.Dimensions {
		return ErrDimensionMismatch
	}

	return c.storage.Update(doc)
}

// Patch partially updates a vector document's metadata
func (c *VectorCollection) Patch(id string, metadata map[string]interface{}) (*VectorDocument, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	doc, err := c.storage.Get(id)
	if err != nil {
		return nil, err
	}

	doc.UpdateMetadata(metadata)
	if err := c.storage.Update(doc); err != nil {
		return nil, err
	}

	return doc, nil
}

// PatchVector updates only the vector of a document
func (c *VectorCollection) PatchVector(id string, vector []float32) (*VectorDocument, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(vector) != c.Config.Dimensions {
		return nil, ErrDimensionMismatch
	}

	doc, err := c.storage.Get(id)
	if err != nil {
		return nil, err
	}

	doc.UpdateVector(vector)
	if err := c.storage.Update(doc); err != nil {
		return nil, err
	}

	return doc, nil
}

// Delete removes a vector document by ID
func (c *VectorCollection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.storage.Delete(id)
}

// Search performs similarity search
func (c *VectorCollection) Search(req *SearchRequest) (*SearchResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(req.Vector) != c.Config.Dimensions {
		return nil, ErrDimensionMismatch
	}

	topK := req.TopK
	if topK <= 0 {
		topK = 10
	}

	var results []*SearchResult
	var err error

	if req.Filter != nil {
		results, err = c.storage.SearchWithFilter(req.Vector, topK, req.MinScore, req.Filter)
	} else {
		results, err = c.storage.Search(req.Vector, topK, req.MinScore)
	}

	if err != nil {
		return nil, err
	}

	// Remove vectors from results if not requested
	if !req.IncludeVector {
		for _, r := range results {
			r.Document = &VectorDocument{
				ID:         r.Document.ID,
				Dimensions: r.Document.Dimensions,
				Metadata:   r.Document.Metadata,
				CreatedAt:  r.Document.CreatedAt,
				UpdatedAt:  r.Document.UpdatedAt,
			}
		}
	}

	return &SearchResponse{
		Results: results,
		Count:   len(results),
	}, nil
}

// Find retrieves documents matching a metadata filter
func (c *VectorCollection) Find(filter map[string]interface{}) ([]*VectorDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.Find(filter)
}

// FindAll retrieves all vector documents
func (c *VectorCollection) FindAll() ([]*VectorDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.FindAll()
}

// Count returns the number of vector documents
func (c *VectorCollection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.storage.Count()
}

// Clear removes all vector documents from the collection
func (c *VectorCollection) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.storage.Clear()
}

// vectorCollectionMetadata represents persisted vector collection info
type vectorCollectionMetadata struct {
	Name     string        `json:"name"`
	Config   *VectorConfig `json:"config"`
}

// Bucket name for vector collections
var vectorCollectionsBucket = []byte("__vector_collections__")

// VectorManager manages multiple vector collections
type VectorManager struct {
	collections map[string]*VectorCollection
	mu          sync.RWMutex
	db          *bolt.DB
}

// NewVectorManager creates a new vector collection manager without persistence
func NewVectorManager() *VectorManager {
	return &VectorManager{
		collections: make(map[string]*VectorCollection),
		db:          nil,
	}
}

// NewPersistentVectorManager creates a new vector collection manager with BoltDB persistence
func NewPersistentVectorManager(db *bolt.DB) (*VectorManager, error) {
	m := &VectorManager{
		collections: make(map[string]*VectorCollection),
		db:          db,
	}

	// Load existing collections from database
	if err := m.loadCollections(); err != nil {
		return nil, fmt.Errorf("failed to load vector collections: %w", err)
	}

	return m, nil
}

// loadCollections loads all vector collections from BoltDB
func (m *VectorManager) loadCollections() error {
	if m.db == nil {
		return nil
	}

	return m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(vectorCollectionsBucket)
		if b == nil {
			return nil // No collections yet
		}

		return b.ForEach(func(k, v []byte) error {
			var meta vectorCollectionMetadata
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}

			// Create memory storage for the collection
			col, err := NewVectorCollection(meta.Name, meta.Config)
			if err != nil {
				return fmt.Errorf("failed to create vector collection %s: %w", meta.Name, err)
			}

			// Load vectors from their bucket
			vectorBucket := tx.Bucket([]byte("_vectors_" + meta.Name))
			if vectorBucket != nil {
				vectorBucket.ForEach(func(k, v []byte) error {
					doc, err := FromJSON(v)
					if err != nil {
						return err
					}
					col.storage.Insert(doc)
					return nil
				})
			}

			m.collections[meta.Name] = col
			return nil
		})
	})
}

// saveCollectionMetadata saves collection metadata to BoltDB
func (m *VectorManager) saveCollectionMetadata(name string, config *VectorConfig) error {
	if m.db == nil {
		return nil
	}

	return m.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(vectorCollectionsBucket)
		if err != nil {
			return err
		}

		meta := vectorCollectionMetadata{
			Name:   name,
			Config: config,
		}
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		return b.Put([]byte(name), data)
	})
}

// CreateCollection creates a new vector collection
func (m *VectorManager) CreateCollection(name string, config *VectorConfig) (*VectorCollection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.collections[name]; exists {
		return nil, errors.New("vector collection already exists: " + name)
	}

	if config.Dimensions <= 0 {
		return nil, ErrInvalidDimensions
	}

	col, err := NewVectorCollection(name, config)
	if err != nil {
		return nil, err
	}

	m.collections[name] = col

	// Save metadata
	if err := m.saveCollectionMetadata(name, config); err != nil {
		delete(m.collections, name)
		return nil, fmt.Errorf("failed to save vector collection metadata: %w", err)
	}

	// Create vector bucket if using BoltDB
	if m.db != nil {
		m.db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("_vectors_" + name))
			return err
		})
	}

	return col, nil
}

// GetCollection retrieves a vector collection by name
func (m *VectorManager) GetCollection(name string) (*VectorCollection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collection, exists := m.collections[name]
	if !exists {
		return nil, errors.New("vector collection not found: " + name)
	}

	return collection, nil
}

// DropCollection removes a vector collection
func (m *VectorManager) DropCollection(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.collections[name]; !exists {
		return errors.New("vector collection not found: " + name)
	}

	// Delete vector bucket if using BoltDB
	if m.db != nil {
		m.db.Update(func(tx *bolt.Tx) error {
			tx.DeleteBucket([]byte("_vectors_" + name))
			return nil
		})
		// Also delete metadata
		m.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(vectorCollectionsBucket)
			if b != nil {
				return b.Delete([]byte(name))
			}
			return nil
		})
	}

	delete(m.collections, name)
	return nil
}

// ListCollections returns a list of all vector collection names
func (m *VectorManager) ListCollections() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.collections))
	for name := range m.collections {
		names = append(names, name)
	}
	return names
}

// CollectionExists checks if a vector collection exists
func (m *VectorManager) CollectionExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.collections[name]
	return exists
}

// PersistDocument persists a single vector document to BoltDB
func (m *VectorManager) PersistDocument(collectionName string, doc *VectorDocument) error {
	if m.db == nil {
		return nil
	}

	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("_vectors_" + collectionName))
		if b == nil {
			return errors.New("collection bucket not found")
		}

		data, err := doc.ToJSON()
		if err != nil {
			return err
		}

		return b.Put([]byte(doc.ID), data)
	})
}

// DeleteDocument removes a vector document from BoltDB
func (m *VectorManager) DeleteDocument(collectionName string, id string) error {
	if m.db == nil {
		return nil
	}

	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("_vectors_" + collectionName))
		if b == nil {
			return errors.New("collection bucket not found")
		}

		return b.Delete([]byte(id))
	})
}

// VectorExportData represents the export format for a vector collection
type VectorExportData struct {
	Name       string            `json:"name"`
	Config     *VectorConfig     `json:"config"`
	Documents  []*VectorDocument `json:"documents"`
	ExportedAt string            `json:"exportedAt"`
}

// ExportCollection exports a vector collection to JSON format
func (m *VectorManager) ExportCollection(name string) (*VectorExportData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	col, exists := m.collections[name]
	if !exists {
		return nil, errors.New("vector collection not found: " + name)
	}

	docs, err := col.FindAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get vector documents: %w", err)
	}

	export := &VectorExportData{
		Name:       name,
		Config:     col.Config,
		Documents:  docs,
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return export, nil
}

// ImportCollection imports a vector collection from JSON data
func (m *VectorManager) ImportCollection(data *VectorExportData, overwrite bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if collection exists
	if _, exists := m.collections[data.Name]; exists {
		if !overwrite {
			return errors.New("vector collection already exists: " + data.Name)
		}
		// Delete existing collection
		if m.db != nil {
			m.db.Update(func(tx *bolt.Tx) error {
				tx.DeleteBucket([]byte("_vectors_" + data.Name))
				return nil
			})
		}
		delete(m.collections, data.Name)
	}

	// Create new collection
	col, err := NewVectorCollection(data.Name, data.Config)
	if err != nil {
		return fmt.Errorf("failed to create vector collection: %w", err)
	}

	// Import documents
	for _, doc := range data.Documents {
		if err := col.storage.Insert(doc); err != nil {
			continue // Skip duplicates
		}
		
		// Persist to BoltDB
		if m.db != nil {
			m.db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("_vectors_" + data.Name))
				if err != nil {
					return err
				}
				d, err := doc.ToJSON()
				if err != nil {
					return err
				}
				return b.Put([]byte(doc.ID), d)
			})
		}
	}

	m.collections[data.Name] = col

	// Save metadata
	if err := m.saveCollectionMetadata(data.Name, data.Config); err != nil {
		delete(m.collections, data.Name)
		return fmt.Errorf("failed to save vector collection metadata: %w", err)
	}

	return nil
}
