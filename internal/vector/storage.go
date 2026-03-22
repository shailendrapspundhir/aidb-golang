package vector

// VectorStorage defines the interface for vector storage engines
type VectorStorage interface {
	// Insert stores a new vector document
	Insert(doc *VectorDocument) error

	// Get retrieves a vector document by ID
	Get(id string) (*VectorDocument, error)

	// Update updates an existing vector document
	Update(doc *VectorDocument) error

	// Delete removes a vector document by ID
	Delete(id string) error

	// Search performs similarity search and returns top-k results
	Search(query []float32, topK int, minScore float32) ([]*SearchResult, error)

	// SearchWithFilter performs similarity search with metadata filtering
	SearchWithFilter(query []float32, topK int, minScore float32, filter map[string]interface{}) ([]*SearchResult, error)

	// Find retrieves documents matching a metadata filter
	Find(filter map[string]interface{}) ([]*VectorDocument, error)

	// FindAll retrieves all vector documents
	FindAll() ([]*VectorDocument, error)

	// Count returns the number of vector documents
	Count() int

	// Clear removes all vector documents
	Clear() error

	// GetConfig returns the vector storage configuration
	GetConfig() *VectorConfig
}

// VectorStorageError represents an error from the vector storage engine
type VectorStorageError struct {
	Operation string
	Message   string
	Err       error
}

func (e *VectorStorageError) Error() string {
	if e.Err != nil {
		return "vector storage error during " + e.Operation + ": " + e.Message + ": " + e.Err.Error()
	}
	return "vector storage error during " + e.Operation + ": " + e.Message
}

func NewVectorStorageError(operation, message string, err error) *VectorStorageError {
	return &VectorStorageError{
		Operation: operation,
		Message:   message,
		Err:       err,
	}
}
