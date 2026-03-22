package vector

import (
	"aidb/internal/storage"
	"math"
	"sort"
	"sync"
)

// HNSWVectorStorage is a vector storage backed by HNSW index
type HNSWVectorStorage struct {
	config *VectorConfig
	index  *storage.HNSWIndex
	docs   map[string]*VectorDocument // ID -> document
	mu     sync.RWMutex
}

// NewHNSWVectorStorage creates a new HNSW-backed vector storage
func NewHNSWVectorStorage(config *VectorConfig) (*HNSWVectorStorage, error) {
	hnswConfig := storage.HNSWConfig{
		M:              config.HNSW_M,
		MMax:           config.HNSW_MMax,
		EfConstruction: config.HNSW_EfConstruction,
		DistanceMetric: string(config.DistanceMetric),
	}

	if hnswConfig.M <= 0 {
		hnswConfig.M = 16
	}
	if hnswConfig.MMax <= 0 {
		hnswConfig.MMax = 32
	}
	if hnswConfig.EfConstruction <= 0 {
		hnswConfig.EfConstruction = 200
	}

	return &HNSWVectorStorage{
		config: config,
		index:  storage.NewHNSWIndex(hnswConfig),
		docs:   make(map[string]*VectorDocument),
	}, nil
}

// Insert stores a new vector document
func (s *HNSWVectorStorage) Insert(doc *VectorDocument) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.docs[doc.ID]; exists {
		return ErrDocumentExists
	}

	// Convert float32 to float64 for HNSW
	vector := make([]float64, len(doc.Vector))
	for i, v := range doc.Vector {
		vector[i] = float64(v)
	}

	s.index.Insert(doc.ID, vector, doc.Metadata)
	s.docs[doc.ID] = doc
	return nil
}

// Get retrieves a vector document by ID
func (s *HNSWVectorStorage) Get(id string) (*VectorDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, exists := s.docs[id]
	if !exists {
		return nil, ErrDocumentNotFound
	}
	return doc, nil
}

// Update updates an existing vector document
func (s *HNSWVectorStorage) Update(doc *VectorDocument) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.docs[doc.ID]; !exists {
		return ErrDocumentNotFound
	}

	// Remove old entry from index
	s.index.Delete(doc.ID)

	// Convert float32 to float64 for HNSW
	vector := make([]float64, len(doc.Vector))
	for i, v := range doc.Vector {
		vector[i] = float64(v)
	}

	// Add updated entry
	s.index.Insert(doc.ID, vector, doc.Metadata)
	s.docs[doc.ID] = doc
	return nil
}

// Delete removes a vector document by ID
func (s *HNSWVectorStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.docs[id]; !exists {
		return ErrDocumentNotFound
	}

	s.index.Delete(id)
	delete(s.docs, id)
	return nil
}

// Search performs similarity search
func (s *HNSWVectorStorage) Search(query []float32, topK int, minScore float32) ([]*SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// For small datasets, use brute-force search for accuracy
	if len(s.docs) <= 100 {
		return s.bruteForceSearch(query, topK, minScore, nil)
	}

	// Convert float32 to float64 for HNSW
	queryVector := make([]float64, len(query))
	for i, v := range query {
		queryVector[i] = float64(v)
	}

	ef := max(topK*2, 50)
	results := s.index.Search(queryVector, topK, ef)

	searchResults := make([]*SearchResult, 0, len(results))
	for _, r := range results {
		// Convert distance to score (for cosine, lower distance = higher similarity)
		score := float32(1.0 - r.Distance)
		if score < minScore {
			continue
		}

		doc := s.docs[r.ID]
		if doc != nil {
			searchResults = append(searchResults, &SearchResult{
				Document: doc,
				Score:    score,
			})
		}
	}

	return searchResults, nil
}

// SearchWithFilter performs similarity search with metadata filtering
func (s *HNSWVectorStorage) SearchWithFilter(query []float32, topK int, minScore float32, filter map[string]interface{}) ([]*SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// For small datasets, use brute-force search for accuracy
	if len(s.docs) <= 100 {
		return s.bruteForceSearch(query, topK, minScore, filter)
	}

	// Convert float32 to float64 for HNSW
	queryVector := make([]float64, len(query))
	for i, v := range query {
		queryVector[i] = float64(v)
	}

	ef := max(topK*10, 100) // More candidates for filtering
	results := s.index.SearchWithFilter(queryVector, topK, ef, filter)

	searchResults := make([]*SearchResult, 0, len(results))
	for _, r := range results {
		score := float32(1.0 - r.Distance)
		if score < minScore {
			continue
		}

		doc := s.docs[r.ID]
		if doc != nil {
			searchResults = append(searchResults, &SearchResult{
				Document: doc,
				Score:    score,
			})
		}
	}

	return searchResults, nil
}

// bruteForceSearch performs exact nearest neighbor search
func (s *HNSWVectorStorage) bruteForceSearch(query []float32, topK int, minScore float32, filter map[string]interface{}) ([]*SearchResult, error) {
	// Get distance function
	var distanceFunc func(a, b []float64) float64
	switch s.config.DistanceMetric {
	case DistanceEuclidean:
		distanceFunc = euclideanDistanceFloat64
	case DistanceDotProduct:
		distanceFunc = dotProductDistanceFloat64
	default: // cosine
		distanceFunc = cosineDistanceFloat64
	}

	// Convert query to float64
	queryVector := make([]float64, len(query))
	for i, v := range query {
		queryVector[i] = float64(v)
	}

	// Calculate distances for all documents
	type result struct {
		doc  *VectorDocument
		dist float64
	}
	var results []result

	for _, doc := range s.docs {
		// Apply filter if provided
		if filter != nil && !matchesFilter(doc.Metadata, filter) {
			continue
		}

		// Convert doc vector to float64
		docVector := make([]float64, len(doc.Vector))
		for i, v := range doc.Vector {
			docVector[i] = float64(v)
		}

		dist := distanceFunc(queryVector, docVector)
		results = append(results, result{doc: doc, dist: dist})
	}

	// Sort by distance (ascending)
	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

	// Take top K
	searchResults := make([]*SearchResult, 0, topK)
	for i := 0; i < len(results) && i < topK; i++ {
		// Convert distance to score
		var score float32
		switch s.config.DistanceMetric {
		case DistanceCosine:
			score = float32(1.0 - results[i].dist)
		case DistanceEuclidean:
			score = float32(1.0 / (1.0 + results[i].dist))
		case DistanceDotProduct:
			score = float32(-results[i].dist)
		}

		if score < minScore {
			continue
		}

		searchResults = append(searchResults, &SearchResult{
			Document: results[i].doc,
			Score:    score,
		})
	}

	return searchResults, nil
}

// Distance functions for float64 vectors
func cosineDistanceFloat64(a, b []float64) float64 {
	if len(a) != len(b) {
		return 1.0
	}

	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	cosineSimilarity := dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
	return 1.0 - cosineSimilarity
}

func euclideanDistanceFloat64(a, b []float64) float64 {
	if len(a) != len(b) {
		return math.Inf(1)
	}

	var sum float64
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return math.Sqrt(sum)
}

func dotProductDistanceFloat64(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct float64
	for i := range a {
		dotProduct += a[i] * b[i]
	}

	return -dotProduct // Negative because we want higher dot product = lower distance
}

// Find retrieves documents matching a metadata filter
func (s *HNSWVectorStorage) Find(filter map[string]interface{}) ([]*VectorDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*VectorDocument
	for _, doc := range s.docs {
		if matchesFilter(doc.Metadata, filter) {
			results = append(results, doc)
		}
	}
	return results, nil
}

// FindAll retrieves all vector documents
func (s *HNSWVectorStorage) FindAll() ([]*VectorDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	docs := make([]*VectorDocument, 0, len(s.docs))
	for _, doc := range s.docs {
		docs = append(docs, doc)
	}
	return docs, nil
}

// Count returns the number of vector documents
func (s *HNSWVectorStorage) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.docs)
}

// Clear removes all vector documents
func (s *HNSWVectorStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.index.Clear()
	s.docs = make(map[string]*VectorDocument)
	return nil
}

// GetConfig returns the vector storage configuration
func (s *HNSWVectorStorage) GetConfig() *VectorConfig {
	return s.config
}

// Stats returns HNSW index statistics
func (s *HNSWVectorStorage) Stats() storage.HNSWStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Stats()
}

// Helper functions

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func matchesFilter(metadata map[string]interface{}, filter map[string]interface{}) bool {
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
