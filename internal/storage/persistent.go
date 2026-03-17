package storage

import (
	"aidb/internal/document"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistentStorage is a file-backed implementation of the Storage interface
type PersistentStorage struct {
	documents  map[string]*document.Document
	mu         sync.RWMutex
	dataFile   string
	collection string
}

// NewPersistentStorage creates a new persistent storage
func NewPersistentStorage(dataFile, collection string) (*PersistentStorage, error) {
	ps := &PersistentStorage{
		documents:  make(map[string]*document.Document),
		dataFile:   dataFile,
		collection: collection,
	}

	// Load existing data if available
	if err := ps.load(); err != nil {
		return nil, fmt.Errorf("failed to load persistent storage: %w", err)
	}

	return ps, nil
}

// load reads documents from the data file
func (s *PersistentStorage) load() error {
	data, err := os.ReadFile(s.dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, that's okay
			return nil
		}
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var docs []*document.Document
	if err := json.Unmarshal(data, &docs); err != nil {
		return fmt.Errorf("failed to unmarshal data file: %w", err)
	}

	for _, doc := range docs {
		s.documents[doc.ID] = doc
	}

	return nil
}

// save writes all documents to the data file
func (s *PersistentStorage) save() error {
	// Ensure directory exists
	dir := filepath.Dir(s.dataFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	docs := make([]*document.Document, 0, len(s.documents))
	for _, doc := range s.documents {
		docs = append(docs, doc)
	}

	data, err := json.MarshalIndent(docs, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.dataFile, data, 0644)
}

// Insert stores a new document
func (s *PersistentStorage) Insert(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[doc.ID]; exists {
		return ErrDocumentExists
	}

	// Create a copy of the document to store
	copy := s.copyDocument(doc)
	s.documents[doc.ID] = copy

	if err := s.save(); err != nil {
		delete(s.documents, doc.ID) // Rollback
		return NewStorageError("insert", "failed to persist document", err)
	}

	return nil
}

// Get retrieves a document by ID
func (s *PersistentStorage) Get(id string) (*document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, exists := s.documents[id]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	return s.copyDocument(doc), nil
}

// Update updates an existing document
func (s *PersistentStorage) Update(doc *document.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[doc.ID]; !exists {
		return ErrDocumentNotFound
	}

	// Save old document for rollback
	oldDoc := s.documents[doc.ID]

	// Update with new document
	copy := s.copyDocument(doc)
	s.documents[doc.ID] = copy

	if err := s.save(); err != nil {
		s.documents[doc.ID] = oldDoc // Rollback
		return NewStorageError("update", "failed to persist document", err)
	}

	return nil
}

// Delete removes a document by ID
func (s *PersistentStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[id]; !exists {
		return ErrDocumentNotFound
	}

	// Save for rollback
	oldDoc := s.documents[id]
	delete(s.documents, id)

	if err := s.save(); err != nil {
		s.documents[id] = oldDoc // Rollback
		return NewStorageError("delete", "failed to persist deletion", err)
	}

	return nil
}

// Find retrieves documents matching a filter
func (s *PersistentStorage) Find(filter map[string]interface{}) ([]*document.Document, error) {
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
func (s *PersistentStorage) FindAll() ([]*document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*document.Document, 0, len(s.documents))
	for _, doc := range s.documents {
		results = append(results, s.copyDocument(doc))
	}

	return results, nil
}

// Count returns the number of documents
func (s *PersistentStorage) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.documents)
}

// Clear removes all documents
func (s *PersistentStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Save for rollback
	oldDocs := s.documents
	s.documents = make(map[string]*document.Document)

	if err := s.save(); err != nil {
		s.documents = oldDocs // Rollback
		return NewStorageError("clear", "failed to persist clear operation", err)
	}

	return nil
}

// copyDocument creates a copy of a document
func (s *PersistentStorage) copyDocument(doc *document.Document) *document.Document {
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
