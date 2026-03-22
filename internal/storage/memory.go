package storage

import (
	"aidb/internal/document"
	"sync"
)

// MemoryStorage is an in-memory implementation of the Storage interface
type MemoryStorage struct {
	documents map[string]*document.Document
	mu        sync.RWMutex
}

// NewMemoryStorage creates a new in-memory storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		documents: make(map[string]*document.Document),
	}
}

// Insert stores a new document
func (s *MemoryStorage) Insert(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[doc.ID]; exists {
		return ErrDocumentExists
	}

	// Create a copy of the document to store
	copy := &document.Document{
		ID:        doc.ID,
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
		Data:      make(map[string]interface{}),
	}
	for k, v := range doc.Data {
		copy.Data[k] = v
	}

	s.documents[doc.ID] = copy
	return nil
}

// Get retrieves a document by ID
func (s *MemoryStorage) Get(id string) (*document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, exists := s.documents[id]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	// Return a copy to prevent external modifications
	return s.copyDocument(doc), nil
}

// Update updates an existing document
func (s *MemoryStorage) Update(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[doc.ID]; !exists {
		return ErrDocumentNotFound
	}

	// Create a copy of the document to store
	copy := &document.Document{
		ID:        doc.ID,
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
		Data:      make(map[string]interface{}),
	}
	for k, v := range doc.Data {
		copy.Data[k] = v
	}

	s.documents[doc.ID] = copy
	return nil
}

// Delete removes a document by ID
func (s *MemoryStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[id]; !exists {
		return ErrDocumentNotFound
	}

	delete(s.documents, id)
	return nil
}

// Find retrieves documents matching a filter
func (s *MemoryStorage) Find(filter map[string]interface{}) ([]*document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*document.Document

	for _, doc := range s.documents {
		if matchesFilter(doc, filter) {
			results = append(results, s.copyDocument(doc))
		}
	}

	return results, nil
}

// FindAll retrieves all documents
func (s *MemoryStorage) FindAll() ([]*document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*document.Document, 0, len(s.documents))
	for _, doc := range s.documents {
		results = append(results, s.copyDocument(doc))
	}

	return results, nil
}

// Count returns the number of documents
func (s *MemoryStorage) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.documents)
}

// Clear removes all documents
func (s *MemoryStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents = make(map[string]*document.Document)
	return nil
}

// Close closes the storage (no-op for memory storage)
func (s *MemoryStorage) Close() error {
	return nil
}

// Flush flushes data to disk (no-op for memory storage)
func (s *MemoryStorage) Flush() error {
	return nil
}

// GetRaw retrieves raw bytes by ID (not supported for memory storage)
func (s *MemoryStorage) GetRaw(id string) ([]byte, error) {
	return nil, ErrDocumentNotFound
}

// PutRaw stores raw bytes by ID (not supported for memory storage)
func (s *MemoryStorage) PutRaw(id string, data []byte) error {
	return nil
}

// DeleteRaw removes raw bytes by ID (not supported for memory storage)
func (s *MemoryStorage) DeleteRaw(id string) error {
	return nil
}

// ImportDocuments imports multiple documents at once
func (s *MemoryStorage) ImportDocuments(docs []*document.Document) error {
	for _, doc := range docs {
		if err := s.Insert(doc); err != nil {
			return err
		}
	}
	return nil
}

// CompactRange compacts the storage (no-op for memory storage)
func (s *MemoryStorage) CompactRange(start, end string) error {
	return nil
}

// copyDocument creates a deep copy of a document
func (s *MemoryStorage) copyDocument(doc *document.Document) *document.Document {
	copy := &document.Document{
		ID:        doc.ID,
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
		Data:      make(map[string]interface{}),
	}
	for k, v := range doc.Data {
		copy.Data[k] = v
	}
	return copy
}

// matchesFilter checks if a document matches the given filter criteria
func matchesFilter(doc *document.Document, filter map[string]interface{}) bool {
	if len(filter) == 0 {
		return true
	}

	for key, value := range filter {
		docValue, exists := doc.Data[key]
		if !exists || docValue != value {
			return false
		}
	}

	return true
}

// memoryCursor implements Cursor for MemoryStorage
type memoryCursor struct {
	storage   *MemoryStorage
	keys      []string
	index     int
	current   *document.Document
	err       error
	closed    bool
}

// Cursor returns a streaming iterator over all documents
func (s *MemoryStorage) Cursor() (Cursor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.documents))
	for k := range s.documents {
		keys = append(keys, k)
	}

	return &memoryCursor{
		storage: s,
		keys:    keys,
		index:   -1,
	}, nil
}

func (c *memoryCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}
	c.index++
	if c.index >= len(c.keys) {
		c.current = nil
		return false
	}

	c.storage.mu.RLock()
	doc, exists := c.storage.documents[c.keys[c.index]]
	c.storage.mu.RUnlock()

	if !exists {
		c.current = nil
		return c.Next() // skip deleted
	}
	c.current = c.storage.copyDocument(doc)
	return true
}

func (c *memoryCursor) Current() *document.Document {
	return c.current
}

func (c *memoryCursor) Err() error {
	return c.err
}

func (c *memoryCursor) Close() error {
	c.closed = true
	c.current = nil
	return nil
}
