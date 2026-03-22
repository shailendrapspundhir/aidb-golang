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

	// Close closes the storage
	Close() error

	// Flush flushes data to disk
	Flush() error

	// GetRaw retrieves raw bytes by ID
	GetRaw(id string) ([]byte, error)

	// PutRaw stores raw bytes by ID
	PutRaw(id string, data []byte) error

	// DeleteRaw removes raw bytes by ID
	DeleteRaw(id string) error

	// ImportDocuments imports multiple documents at once
	ImportDocuments(docs []*document.Document) error

	// CompactRange compacts the storage for a range
	CompactRange(start, end string) error

	// Cursor returns an iterator over all documents (streaming, memory-safe)
	Cursor() (Cursor, error)
}

// Cursor provides streaming iteration over documents without loading all into memory
type Cursor interface {
	// Next advances to the next document; returns false when done or on error
	Next() bool
	// Current returns the current document (valid only after Next returns true)
	Current() *document.Document
	// Err returns any error encountered during iteration
	Err() error
	// Close releases resources held by the cursor
	Close() error
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
