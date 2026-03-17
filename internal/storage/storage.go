package storage

import (
	"aidb/internal/document"
)

// Storage defines the interface for document storage engines
type Storage interface {
	// Insert stores a new document
	Insert(doc *document.Document) error

	// Get retrieves a document by ID
	Get(id string) (*document.Document, error)

	// Update updates an existing document
	Update(doc *document.Document) error

	// Delete removes a document by ID
	Delete(id string) error

	// Find retrieves documents matching a filter
	Find(filter map[string]interface{}) ([]*document.Document, error)

	// FindAll retrieves all documents
	FindAll() ([]*document.Document, error)

	// Count returns the number of documents
	Count() int

	// Clear removes all documents
	Clear() error
}

// StorageError represents an error from the storage engine
type StorageError struct {
	Operation string
	Message   string
	Err       error
}

func (e *StorageError) Error() string {
	if e.Err != nil {
		return "storage error during " + e.Operation + ": " + e.Message + ": " + e.Err.Error()
	}
	return "storage error during " + e.Operation + ": " + e.Message
}

func NewStorageError(operation, message string, err error) *StorageError {
	return &StorageError{
		Operation: operation,
		Message:   message,
		Err:       err,
	}
}

// ErrDocumentNotFound is returned when a document is not found
var ErrDocumentNotFound = &StorageError{
	Operation: "get",
	Message:   "document not found",
}

// ErrDocumentExists is returned when trying to insert a document with an existing ID
var ErrDocumentExists = &StorageError{
	Operation: "insert",
	Message:   "document with this ID already exists",
}
