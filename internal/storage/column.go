package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// VectorColumnStore stores vectors separately from main documents
// Uses memory-mapped files for efficient vector access
type VectorColumnStore struct {
	mu sync.RWMutex

	// Configuration
	dims          int
	distanceMetric string // "cosine", "euclidean", "dot"

	// In-memory storage (for now, can be extended to mmap)
	vectors map[string][]float32

	// HNSW index for fast ANN search
	hnswIndex *HNSWIndex

	// Statistics
	count int
}

// VectorColumnConfig contains configuration for vector column store
type VectorColumnConfig struct {
	Dimensions     int
	DistanceMetric string
	HNSW_M         int // HNSW M parameter
	HNSW_Ef        int // HNSW ef parameter
}

// NewVectorColumnStore creates a new vector column store
func NewVectorColumnStore(config VectorColumnConfig) (*VectorColumnStore, error) {
	if config.Dimensions <= 0 {
		return nil, errors.New("dimensions must be positive")
	}

	if config.DistanceMetric == "" {
		config.DistanceMetric = "cosine"
	}

	if config.HNSW_M <= 0 {
		config.HNSW_M = 16
	}
	if config.HNSW_Ef <= 0 {
		config.HNSW_Ef = 200
	}

	hnswConfig := HNSWConfig{
		M:              config.HNSW_M,
		MMax:           2 * config.HNSW_M,
		EfConstruction: config.HNSW_Ef,
		DistanceMetric: config.DistanceMetric,
	}

	return &VectorColumnStore{
		dims:          config.Dimensions,
		distanceMetric: config.DistanceMetric,
		vectors:       make(map[string][]float32),
		hnswIndex:     NewHNSWIndex(hnswConfig),
	}, nil
}

// Insert adds a vector to the column store
func (s *VectorColumnStore) Insert(id string, vector []float32) error {
	if len(vector) != s.dims {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", s.dims, len(vector))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists
	if _, exists := s.vectors[id]; exists {
		return nil // Already exists, skip
	}

	// Store vector
	s.vectors[id] = vector

	// Add to HNSW index
	float64Vec := make([]float64, len(vector))
	for i, v := range vector {
		float64Vec[i] = float64(v)
	}
	s.hnswIndex.Insert(id, float64Vec, nil)

	s.count++
	return nil
}

// Update updates a vector in the column store
func (s *VectorColumnStore) Update(id string, vector []float32) error {
	if len(vector) != s.dims {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", s.dims, len(vector))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.vectors[id]; !exists {
		return ErrDocumentNotFound
	}

	// Remove from HNSW and re-add
	s.hnswIndex.Delete(id)

	float64Vec := make([]float64, len(vector))
	for i, v := range vector {
		float64Vec[i] = float64(v)
	}
	s.hnswIndex.Insert(id, float64Vec, nil)

	s.vectors[id] = vector
	return nil
}

// Delete removes a vector from the column store
func (s *VectorColumnStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.vectors[id]; !exists {
		return nil // Already gone
	}

	s.hnswIndex.Delete(id)
	delete(s.vectors, id)
	s.count--
	return nil
}

// Get retrieves a vector by ID
func (s *VectorColumnStore) Get(id string) ([]float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vec, exists := s.vectors[id]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	// Return a copy
	result := make([]float32, len(vec))
	copy(result, vec)
	return result, nil
}

// Search performs similarity search
func (s *VectorColumnStore) Search(query []float32, k int, minScore float32) ([]VectorSearchResult, error) {
	if len(query) != s.dims {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", s.dims, len(query))
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert query to float64
	queryFloat64 := make([]float64, len(query))
	for i, v := range query {
		queryFloat64[i] = float64(v)
	}

	ef := max(k*2, 50)
	results := s.hnswIndex.Search(queryFloat64, k*2, ef)

	// Convert to our result type with score calculation
	searchResults := make([]VectorSearchResult, 0, k)
	for _, r := range results {
		// Convert distance to score
		score := float32(1.0 - r.Distance)
		if score < minScore {
			continue
		}

		searchResults = append(searchResults, VectorSearchResult{
			ID:    r.ID,
			Score: score,
		})

		if len(searchResults) >= k {
			break
		}
	}

	return searchResults, nil
}

// SearchWithFilter performs similarity search with metadata filter
func (s *VectorColumnStore) SearchWithFilter(query []float32, k int, minScore float32, filter map[string]interface{}, docFilterFunc func(string) bool) ([]VectorSearchResult, error) {
	if len(query) != s.dims {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", s.dims, len(query))
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	queryFloat64 := make([]float64, len(query))
	for i, v := range query {
		queryFloat64[i] = float64(v)
	}

	// Get more candidates to account for filtering
	ef := max(k*10, 100)
	results := s.hnswIndex.Search(queryFloat64, k*10, ef)

	searchResults := make([]VectorSearchResult, 0, k)
	for _, r := range results {
		// Apply filter using provided function
		if docFilterFunc != nil && !docFilterFunc(r.ID) {
			continue
		}

		score := float32(1.0 - r.Distance)
		if score < minScore {
			continue
		}

		searchResults = append(searchResults, VectorSearchResult{
			ID:    r.ID,
			Score: score,
		})

		if len(searchResults) >= k {
			break
		}
	}

	return searchResults, nil
}

// Count returns the number of vectors
func (s *VectorColumnStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// Clear removes all vectors
func (s *VectorColumnStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors = make(map[string][]float32)
	s.hnswIndex.Clear()
	s.count = 0
}

// GetDimensions returns the vector dimensions
func (s *VectorColumnStore) GetDimensions() int {
	return s.dims
}

// GetDistanceMetric returns the distance metric
func (s *VectorColumnStore) GetDistanceMetric() string {
	return s.distanceMetric
}

// Stats returns statistics about the vector store
func (s *VectorColumnStore) Stats() VectorColumnStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hnswStats := s.hnswIndex.Stats()
	return VectorColumnStats{
		VectorCount:    s.count,
		Dimensions:     s.dims,
		DistanceMetric: s.distanceMetric,
		HNSWStats:      hnswStats,
	}
}

// VectorColumnStats contains statistics about the vector column store
type VectorColumnStats struct {
	VectorCount    int
	Dimensions     int
	DistanceMetric string
	HNSWStats      HNSWStats
}

// VectorSearchResult represents a vector search result
type VectorSearchResult struct {
	ID    string
	Score float32
}

// FullTextColumnStore stores text data for full-text search
type FullTextColumnStore struct {
	mu sync.RWMutex

	// Inverted index for text search
	index *InvertedTextIndex

	// Text content storage
	texts map[string]string

	// Statistics
	count int
}

// FullTextColumnConfig contains configuration for full-text column store
type FullTextColumnConfig struct {
	Analyzer string // "standard", "simple", "whitespace"
}

// NewFullTextColumnStore creates a new full-text column store
func NewFullTextColumnStore(config FullTextColumnConfig) *FullTextColumnStore {
	return &FullTextColumnStore{
		index: NewInvertedTextIndex(config.Analyzer),
		texts: make(map[string]string),
	}
}

// Insert adds text to the full-text column store
func (s *FullTextColumnStore) Insert(id string, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove old if exists
	if _, exists := s.texts[id]; exists {
		s.index.RemoveDocument(id)
	}

	s.texts[id] = text
	s.index.IndexDocument(id, text)
	s.count++
	return nil
}

// Update updates text in the full-text column store
func (s *FullTextColumnStore) Update(id string, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.texts[id]; !exists {
		return ErrDocumentNotFound
	}

	s.texts[id] = text
	s.index.RemoveDocument(id)
	s.index.IndexDocument(id, text)
	return nil
}

// Delete removes text from the full-text column store
func (s *FullTextColumnStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.texts[id]; !exists {
		return nil
	}

	s.index.RemoveDocument(id)
	delete(s.texts, id)
	s.count--
	return nil
}

// Get retrieves text by ID
func (s *FullTextColumnStore) Get(id string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	text, exists := s.texts[id]
	if !exists {
		return "", ErrDocumentNotFound
	}
	return text, nil
}

// Search performs full-text search
func (s *FullTextColumnStore) Search(query string, limit int) ([]TextSearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := s.index.Search(query, limit)
	return results, nil
}

// Count returns the number of documents
func (s *FullTextColumnStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// Clear removes all data
func (s *FullTextColumnStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.texts = make(map[string]string)
	s.index.Clear()
	s.count = 0
}

// TextSearchResult represents a full-text search result
type TextSearchResult struct {
	ID    string
	Score float64
}

// InvertedTextIndex is a simple inverted index for text search
type InvertedTextIndex struct {
	mu        sync.RWMutex
	postings  map[string]map[string]int // term -> docID -> frequency
	docLength map[string]int            // docID -> number of terms
	totalDocs int
	analyzer  TextAnalyzer
}

// TextAnalyzer analyzes text into terms
type TextAnalyzer interface {
	Analyze(text string) []string
}

// NewInvertedTextIndex creates a new inverted text index
func NewInvertedTextIndex(analyzerType string) *InvertedTextIndex {
	var analyzer TextAnalyzer
	switch analyzerType {
	case "simple":
		analyzer = &SimpleAnalyzer{}
	case "whitespace":
		analyzer = &WhitespaceAnalyzer{}
	default:
		analyzer = &StandardAnalyzer{}
	}

	return &InvertedTextIndex{
		postings:  make(map[string]map[string]int),
		docLength: make(map[string]int),
		analyzer:  analyzer,
	}
}

// IndexDocument indexes a document's text
func (idx *InvertedTextIndex) IndexDocument(id string, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove old postings
	idx.removeDocLocked(id)

	// Analyze text
	terms := idx.analyzer.Analyze(text)
	if len(terms) == 0 {
		return
	}

	// Count term frequencies
	termFreq := make(map[string]int)
	for _, term := range terms {
		termFreq[term]++
	}

	// Add to postings
	for term, freq := range termFreq {
		if idx.postings[term] == nil {
			idx.postings[term] = make(map[string]int)
		}
		idx.postings[term][id] = freq
	}

	idx.docLength[id] = len(terms)
	idx.totalDocs++
}

// RemoveDocument removes a document from the index
func (idx *InvertedTextIndex) RemoveDocument(id string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeDocLocked(id)
}

func (idx *InvertedTextIndex) removeDocLocked(id string) {
	_, exists := idx.docLength[id]
	if !exists {
		return
	}

	// Remove from all postings
	for term, postings := range idx.postings {
		delete(postings, id)
		if len(postings) == 0 {
			delete(idx.postings, term)
		}
	}

	delete(idx.docLength, id)
	idx.totalDocs--
}

// Search performs full-text search with BM25 scoring
func (idx *InvertedTextIndex) Search(query string, limit int) []TextSearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Analyze query
	queryTerms := idx.analyzer.Analyze(query)
	if len(queryTerms) == 0 {
		return nil
	}

	// Calculate scores
	scores := make(map[string]float64)
	avgDocLen := idx.avgDocLength()
	k1 := 1.5
	b := 0.75

	for _, term := range queryTerms {
		postings, exists := idx.postings[term]
		if !exists {
			continue
		}

		// IDF
		n := len(postings)
		idf := math.Log(float64(idx.totalDocs-n)/float64(n) + 1.0)

		// Score each document
		for docID, tf := range postings {
			docLen := idx.docLength[docID]
			if docLen == 0 {
				continue
			}

			// BM25 score
			tfNorm := float64(tf) * (k1 + 1) / (float64(tf) + k1*(1-b+b*float64(docLen)/avgDocLen))
			scores[docID] += idf * tfNorm
		}
	}

	// Sort by score
	type scoredDoc struct {
		id    string
		score float64
	}
	docs := make([]scoredDoc, 0, len(scores))
	for id, score := range scores {
		docs = append(docs, scoredDoc{id: id, score: score})
	}
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].score > docs[j].score
	})

	// Return top results
	results := make([]TextSearchResult, 0, limit)
	for i := 0; i < len(docs) && i < limit; i++ {
		results = append(results, TextSearchResult{
			ID:    docs[i].id,
			Score: docs[i].score,
		})
	}

	return results
}

func (idx *InvertedTextIndex) avgDocLength() float64 {
	if idx.totalDocs == 0 {
		return 0
	}
	var total int
	for _, l := range idx.docLength {
		total += l
	}
	return float64(total) / float64(idx.totalDocs)
}

// Clear clears the index
func (idx *InvertedTextIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.postings = make(map[string]map[string]int)
	idx.docLength = make(map[string]int)
	idx.totalDocs = 0
}

// StandardAnalyzer is a basic text analyzer
type StandardAnalyzer struct{}

// Analyze analyzes text into lowercase terms
func (a *StandardAnalyzer) Analyze(text string) []string {
	// Simple tokenization: split on whitespace and punctuation
	terms := make([]string, 0)
	start := -1

	for i, r := range text {
		if isAlphaNumeric(r) {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				term := toLower(text[start:i])
				if len(term) > 0 && !isStopWord(term) {
					terms = append(terms, term)
				}
				start = -1
			}
		}
	}
	if start != -1 {
		term := toLower(text[start:])
		if len(term) > 0 && !isStopWord(term) {
			terms = append(terms, term)
		}
	}

	return terms
}

// SimpleAnalyzer is a simple text analyzer
type SimpleAnalyzer struct{}

// Analyze analyzes text into lowercase terms
func (a *SimpleAnalyzer) Analyze(text string) []string {
	terms := make([]string, 0)
	start := -1

	for i, r := range text {
		if isAlphaNumeric(r) {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				term := toLower(text[start:i])
				if len(term) > 0 {
					terms = append(terms, term)
				}
				start = -1
			}
		}
	}
	if start != -1 {
		term := toLower(text[start:])
		if len(term) > 0 {
			terms = append(terms, term)
		}
	}

	return terms
}

// WhitespaceAnalyzer splits on whitespace only
type WhitespaceAnalyzer struct{}

// Analyze analyzes text by splitting on whitespace
func (a *WhitespaceAnalyzer) Analyze(text string) []string {
	terms := make([]string, 0)
	start := -1

	for i, r := range text {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				terms = append(terms, text[start:i])
				start = -1
			}
		}
	}
	if start != -1 {
		terms = append(terms, text[start:])
	}

	return terms
}

// Helper functions
func isAlphaNumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func toLower(s string) string {
	result := make([]rune, len(s))
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			result[i] = r + ('a' - 'A')
		} else {
			result[i] = r
		}
	}
	return string(result)
}

// Common English stop words
var stopWords = map[string]bool{
	"a": true, "an": true, "the": true, "and": true, "or": true, "but": true,
	"in": true, "on": true, "at": true, "to": true, "for": true, "of": true,
	"with": true, "by": true, "from": true, "is": true, "are": true, "was": true,
	"were": true, "be": true, "been": true, "being": true, "have": true, "has": true,
	"had": true, "do": true, "does": true, "did": true, "will": true, "would": true,
	"could": true, "should": true, "may": true, "might": true, "must": true,
	"this": true, "that": true, "these": true, "those": true, "it": true,
	"its": true, "as": true, "if": true, "then": true, "than": true, "so": true,
}

func isStopWord(term string) bool {
	return stopWords[term]
}

// VectorFileStorage provides file-based storage for vectors (optional persistence)
type VectorFileStorage struct {
	filePath string
	file     *os.File
	mu       sync.RWMutex
}

// NewVectorFileStorage creates a new file-based vector storage
func NewVectorFileStorage(path string) (*VectorFileStorage, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &VectorFileStorage{
		filePath: path,
		file:     file,
	}, nil
}

// WriteVector writes a vector to the file
func (s *VectorFileStorage) WriteVector(id string, vector []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Format: [id_length(4)] [id] [dims(4)] [vector_data]
	idBytes := []byte(id)
	idLen := uint32(len(idBytes))
	dims := uint32(len(vector))

	// Write id length
	if err := binary.Write(s.file, binary.LittleEndian, idLen); err != nil {
		return err
	}

	// Write id
	if _, err := s.file.Write(idBytes); err != nil {
		return err
	}

	// Write dims
	if err := binary.Write(s.file, binary.LittleEndian, dims); err != nil {
		return err
	}

	// Write vector data
	for _, v := range vector {
		if err := binary.Write(s.file, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	return s.file.Sync()
}

// ReadAllVectors reads all vectors from the file
func (s *VectorFileStorage) ReadAllVectors() (map[string][]float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vectors := make(map[string][]float32)

	// Seek to beginning
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	for {
		// Read id length
		var idLen uint32
		if err := binary.Read(s.file, binary.LittleEndian, &idLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read id
		idBytes := make([]byte, idLen)
		if _, err := io.ReadFull(s.file, idBytes); err != nil {
			return nil, err
		}
		id := string(idBytes)

		// Read dims
		var dims uint32
		if err := binary.Read(s.file, binary.LittleEndian, &dims); err != nil {
			return nil, err
		}

		// Read vector
		vector := make([]float32, dims)
		for i := range vector {
			if err := binary.Read(s.file, binary.LittleEndian, &vector[i]); err != nil {
				return nil, err
			}
		}

		vectors[id] = vector
	}

	return vectors, nil
}

// Close closes the file
func (s *VectorFileStorage) Close() error {
	return s.file.Close()
}
