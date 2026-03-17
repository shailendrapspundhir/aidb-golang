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
