package vector

import (
	"container/heap"
	"sync"
)

// MemoryVectorStorage is an in-memory implementation of VectorStorage
type MemoryVectorStorage struct {
	documents map[string]*VectorDocument
	config    *VectorConfig
	distance  DistanceFunc
	mu        sync.RWMutex
}

// NewMemoryVectorStorage creates a new in-memory vector storage
func NewMemoryVectorStorage(config *VectorConfig) (*MemoryVectorStorage, error) {
	if config.Dimensions <= 0 {
		return nil, ErrInvalidDimensions
	}

	distance, err := GetDistanceFunc(config.DistanceMetric)
	if err != nil {
		return nil, err
	}

	return &MemoryVectorStorage{
		documents: make(map[string]*VectorDocument),
		config:    config,
		distance:  distance,
	}, nil
}

// Insert stores a new vector document
func (s *MemoryVectorStorage) Insert(doc *VectorDocument) error {
	if len(doc.Vector) == 0 {
		return ErrInvalidVector
	}

	if len(doc.Vector) != s.config.Dimensions {
		return ErrDimensionMismatch
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[doc.ID]; exists {
		return ErrVectorExists
	}

	s.documents[doc.ID] = doc
	return nil
}

// Get retrieves a vector document by ID
func (s *MemoryVectorStorage) Get(id string) (*VectorDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, exists := s.documents[id]
	if !exists {
		return nil, ErrVectorNotFound
	}

	// Return a copy to prevent modification of stored document
	docCopy := *doc
	return &docCopy, nil
}

// Update updates an existing vector document
func (s *MemoryVectorStorage) Update(doc *VectorDocument) error {
	if len(doc.Vector) == 0 {
		return ErrInvalidVector
	}

	if len(doc.Vector) != s.config.Dimensions {
		return ErrDimensionMismatch
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[doc.ID]; !exists {
		return ErrVectorNotFound
	}

	s.documents[doc.ID] = doc
	return nil
}

// Delete removes a vector document by ID
func (s *MemoryVectorStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.documents[id]; !exists {
		return ErrVectorNotFound
	}

	delete(s.documents, id)
	return nil
}

// Search performs similarity search and returns top-k results
func (s *MemoryVectorStorage) Search(query []float32, topK int, minScore float32) ([]*SearchResult, error) {
	return s.SearchWithFilter(query, topK, minScore, nil)
}

// SearchWithFilter performs similarity search with metadata filtering
func (s *MemoryVectorStorage) SearchWithFilter(query []float32, topK int, minScore float32, filter map[string]interface{}) ([]*SearchResult, error) {
	if len(query) != s.config.Dimensions {
		return nil, ErrDimensionMismatch
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Use a max-heap to keep top-k results
	h := &resultHeap{}
	heap.Init(h)

	for _, doc := range s.documents {
		// Apply filter if provided
		if filter != nil && !matchesMetadataFilter(doc.Metadata, filter) {
			continue
		}

		distance := s.distance(query, doc.Vector)
		
		// Convert distance to similarity score based on metric
		var score float32
		switch s.config.DistanceMetric {
		case DistanceCosine:
			score = 1 - distance // cosine similarity
		case DistanceEuclidean:
			score = 1 / (1 + distance) // convert to similarity-like score
		case DistanceDotProduct:
			score = -distance // negative distance = dot product
		}

		if score < minScore {
			continue
		}

		// Return a copy to prevent modification of stored document
		docCopy := *doc
		result := &SearchResult{
			Document: &docCopy,
			Score:    score,
		}

		if h.Len() < topK {
			heap.Push(h, result)
		} else if score > (*h)[0].Score {
			heap.Pop(h)
			heap.Push(h, result)
		}
	}

	// Extract results from heap (sorted by score descending)
	results := make([]*SearchResult, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		results[i] = heap.Pop(h).(*SearchResult)
	}

	return results, nil
}

// Find retrieves documents matching a metadata filter
func (s *MemoryVectorStorage) Find(filter map[string]interface{}) ([]*VectorDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*VectorDocument
	for _, doc := range s.documents {
		if matchesMetadataFilter(doc.Metadata, filter) {
			// Return a copy to prevent modification of stored document
			docCopy := *doc
			results = append(results, &docCopy)
		}
	}

	return results, nil
}

// FindAll retrieves all vector documents
func (s *MemoryVectorStorage) FindAll() ([]*VectorDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*VectorDocument, 0, len(s.documents))
	for _, doc := range s.documents {
		// Return a copy to prevent modification of stored document
		docCopy := *doc
		results = append(results, &docCopy)
	}

	return results, nil
}

// Count returns the number of vector documents
func (s *MemoryVectorStorage) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.documents)
}

// Clear removes all vector documents
func (s *MemoryVectorStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents = make(map[string]*VectorDocument)
	return nil
}

// GetConfig returns the vector storage configuration
func (s *MemoryVectorStorage) GetConfig() *VectorConfig {
	return s.config
}

// resultHeap implements a max-heap for search results
type resultHeap []*SearchResult

func (h resultHeap) Len() int           { return len(h) }
func (h resultHeap) Less(i, j int) bool { return h[i].Score < h[j].Score } // min-heap by score
func (h resultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *resultHeap) Push(x interface{}) {
	*h = append(*h, x.(*SearchResult))
}

func (h *resultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// matchesMetadataFilter checks if metadata matches the filter
func matchesMetadataFilter(metadata, filter map[string]interface{}) bool {
	if filter == nil {
		return true
	}

	for key, value := range filter {
		metaValue, exists := metadata[key]
		if !exists || metaValue != value {
			return false
		}
	}

	return true
}
