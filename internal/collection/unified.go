package collection

import (
	"aidb/internal/document"
	"aidb/internal/storage"
	"fmt"
	"sort"
	"sync"
)

// UnifiedCollection represents a collection that supports all data types
type UnifiedCollection struct {
	Name   string
	Schema *document.UnifiedSchema

	// Main document storage (lightweight, without vectors)
	docStorage storage.Storage

	// Column stores for large fields
	vectorStores map[string]*storage.VectorColumnStore  // field -> store
	textStores   map[string]*storage.FullTextColumnStore // field -> store

	// Scalar indexes
	scalarIndexes map[string]storage.Index // field -> index

	// Query planner
	queryPlanner *QueryPlanner

	mu sync.RWMutex
}

// UnifiedCollectionConfig contains configuration for unified collection
type UnifiedCollectionConfig struct {
	Name      string
	Schema    *document.UnifiedSchema
	Storage   storage.Storage
}

// NewUnifiedCollection creates a new unified collection
func NewUnifiedCollection(config UnifiedCollectionConfig) (*UnifiedCollection, error) {
	col := &UnifiedCollection{
		Name:          config.Name,
		Schema:        config.Schema,
		docStorage:    config.Storage,
		vectorStores:  make(map[string]*storage.VectorColumnStore),
		textStores:    make(map[string]*storage.FullTextColumnStore),
		scalarIndexes: make(map[string]storage.Index),
		queryPlanner:  NewQueryPlanner(),
	}

	// Initialize column stores based on schema
	if config.Schema != nil {
		// Create vector stores
		for fieldName, fieldSchema := range config.Schema.GetVectorFields() {
			vecStore, err := storage.NewVectorColumnStore(storage.VectorColumnConfig{
				Dimensions:     fieldSchema.Dimensions,
				DistanceMetric: fieldSchema.DistanceMetric,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create vector store for %s: %w", fieldName, err)
			}
			col.vectorStores[fieldName] = vecStore
		}

		// Create text stores
		for fieldName, fieldSchema := range config.Schema.GetFullTextFields() {
			textStore := storage.NewFullTextColumnStore(storage.FullTextColumnConfig{
				Analyzer: fieldSchema.Analyzer,
			})
			col.textStores[fieldName] = textStore
		}

		// Create scalar indexes
		for fieldName, fieldSchema := range config.Schema.GetIndexedFields() {
			var idx storage.Index
			if fieldSchema.IndexType == "hash" {
				idx = storage.NewHashIndex(fmt.Sprintf("%s_%s_idx", config.Name, fieldName), fieldName)
			} else {
				idx = storage.NewBTreeIndex(fmt.Sprintf("%s_%s_idx", config.Name, fieldName), fieldName, 64)
			}
			col.scalarIndexes[fieldName] = idx
		}
	}

	return col, nil
}

// Insert stores a new document
func (c *UnifiedCollection) Insert(doc *document.UnifiedDocument) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate against schema
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	// Extract and store vectors separately
	for fieldName, vecStore := range c.vectorStores {
		if fv, ok := doc.Fields[fieldName]; ok && len(fv.Vector) > 0 {
			if err := vecStore.Insert(doc.ID, fv.Vector); err != nil {
				return fmt.Errorf("failed to insert vector for %s: %w", fieldName, err)
			}
		}
	}

	// Extract and store text separately
	for fieldName, textStore := range c.textStores {
		if fv, ok := doc.Fields[fieldName]; ok {
			if text, ok := fv.Scalar.(string); ok {
				if err := textStore.Insert(doc.ID, text); err != nil {
					return fmt.Errorf("failed to insert text for %s: %w", fieldName, err)
				}
			}
		}
	}

	// Store lightweight document (without vectors)
	lightDoc := c.createLightDocument(doc)
	if err := c.docStorage.Insert(lightDoc.ConvertToLegacyDocument()); err != nil {
		// Rollback column stores
		for fieldName := range c.vectorStores {
			c.vectorStores[fieldName].Delete(doc.ID)
		}
		for fieldName := range c.textStores {
			c.textStores[fieldName].Delete(doc.ID)
		}
		return err
	}

	// Update scalar indexes
	for fieldName, idx := range c.scalarIndexes {
		if fv, ok := doc.Fields[fieldName]; ok && fv.Scalar != nil {
			idx.Insert(fv.Scalar, doc.ID)
		}
	}

	return nil
}

// Get retrieves a document by ID
func (c *UnifiedCollection) Get(id string) (*document.UnifiedDocument, error) {
	return c.GetWithProjection(id, nil)
}

// GetWithProjection retrieves a document with optional field projection
func (c *UnifiedCollection) GetWithProjection(id string, projection []string) (*document.UnifiedDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get lightweight document
	doc, err := c.docStorage.Get(id)
	if err != nil {
		return nil, err
	}

	unified := document.ConvertFromLegacyDocument(doc)

	// Lazy load vectors if requested
	projectionSet := make(map[string]bool)
	for _, f := range projection {
		projectionSet[f] = true
	}

	for fieldName, vecStore := range c.vectorStores {
		// Only load if in projection or projection is empty (load all)
		if len(projection) == 0 || projectionSet[fieldName] {
			if vec, err := vecStore.Get(id); err == nil {
				unified.SetVector(fieldName, vec)
			}
		}
	}

	return unified, nil
}

// Update updates an existing document
func (c *UnifiedCollection) Update(doc *document.UnifiedDocument) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate against schema
	if c.Schema != nil {
		if err := c.Schema.Validate(doc); err != nil {
			return err
		}
	}

	// Get old document for index updates
	oldDoc, err := c.docStorage.Get(doc.ID)
	if err != nil {
		return err
	}
	oldUnified := document.ConvertFromLegacyDocument(oldDoc)

	// Update vector stores
	for fieldName, vecStore := range c.vectorStores {
		if fv, ok := doc.Fields[fieldName]; ok && len(fv.Vector) > 0 {
			if err := vecStore.Update(doc.ID, fv.Vector); err != nil {
				return fmt.Errorf("failed to update vector for %s: %w", fieldName, err)
			}
		}
	}

	// Update text stores
	for fieldName, textStore := range c.textStores {
		if fv, ok := doc.Fields[fieldName]; ok {
			if text, ok := fv.Scalar.(string); ok {
				if err := textStore.Update(doc.ID, text); err != nil {
					return fmt.Errorf("failed to update text for %s: %w", fieldName, err)
				}
			}
		}
	}

	// Update scalar indexes
	for fieldName, idx := range c.scalarIndexes {
		// Remove old value
		if oldFv, ok := oldUnified.Fields[fieldName]; ok && oldFv.Scalar != nil {
			idx.Delete(oldFv.Scalar, doc.ID)
		}
		// Add new value
		if fv, ok := doc.Fields[fieldName]; ok && fv.Scalar != nil {
			idx.Insert(fv.Scalar, doc.ID)
		}
	}

	// Update document storage
	lightDoc := c.createLightDocument(doc)
	return c.docStorage.Update(lightDoc.ConvertToLegacyDocument())
}

// Delete removes a document by ID
func (c *UnifiedCollection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get document for index updates
	doc, err := c.docStorage.Get(id)
	if err != nil {
		return err
	}
	unified := document.ConvertFromLegacyDocument(doc)

	// Delete from scalar indexes
	for fieldName, idx := range c.scalarIndexes {
		if fv, ok := unified.Fields[fieldName]; ok && fv.Scalar != nil {
			idx.Delete(fv.Scalar, id)
		}
	}

	// Delete from vector stores
	for _, vecStore := range c.vectorStores {
		vecStore.Delete(id)
	}

	// Delete from text stores
	for _, textStore := range c.textStores {
		textStore.Delete(id)
	}

	// Delete from document storage
	return c.docStorage.Delete(id)
}

// Find retrieves documents matching a filter
func (c *UnifiedCollection) Find(filter map[string]interface{}) ([]*document.UnifiedDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Try to use index
	for field, value := range filter {
		if idx, ok := c.scalarIndexes[field]; ok {
			ids, err := idx.Find(value)
			if err == nil && len(ids) > 0 {
				return c.loadDocumentsByIDs(ids)
			}
		}
	}

	// Fall back to full scan
	docs, err := c.docStorage.FindAll()
	if err != nil {
		return nil, err
	}

	results := make([]*document.UnifiedDocument, 0)
	for _, doc := range docs {
		unified := document.ConvertFromLegacyDocument(doc)
		if c.matchesFilter(unified, filter) {
			results = append(results, unified)
		}
	}

	return results, nil
}

// FindAll retrieves all documents
func (c *UnifiedCollection) FindAll() ([]*document.UnifiedDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	docs, err := c.docStorage.FindAll()
	if err != nil {
		return nil, err
	}

	results := make([]*document.UnifiedDocument, len(docs))
	for i, doc := range docs {
		results[i] = document.ConvertFromLegacyDocument(doc)
	}

	return results, nil
}

// Count returns the number of documents
func (c *UnifiedCollection) Count() int {
	return c.docStorage.Count()
}

// Clear removes all documents
func (c *UnifiedCollection) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear all stores
	if err := c.docStorage.Clear(); err != nil {
		return err
	}

	for _, vecStore := range c.vectorStores {
		vecStore.Clear()
	}

	for _, textStore := range c.textStores {
		textStore.Clear()
	}

	for _, idx := range c.scalarIndexes {
		idx.Clear()
	}

	return nil
}

// createLightDocument creates a document without vector data for storage
func (c *UnifiedCollection) createLightDocument(doc *document.UnifiedDocument) *document.UnifiedDocument {
	light := document.NewUnifiedDocumentWithID(doc.ID)
	light.CreatedAt = doc.CreatedAt
	light.UpdatedAt = doc.UpdatedAt

	for fieldName, fv := range doc.Fields {
		// Skip vectors - they're stored separately
		if _, isVector := c.vectorStores[fieldName]; isVector {
			continue
		}
		light.Fields[fieldName] = fv
	}

	return light
}

// loadDocumentsByIDs loads multiple documents by IDs
func (c *UnifiedCollection) loadDocumentsByIDs(ids []string) ([]*document.UnifiedDocument, error) {
	results := make([]*document.UnifiedDocument, 0, len(ids))
	for _, id := range ids {
		doc, err := c.docStorage.Get(id)
		if err == nil {
			results = append(results, document.ConvertFromLegacyDocument(doc))
		}
	}
	return results, nil
}

// matchesFilter checks if a document matches a filter
func (c *UnifiedCollection) matchesFilter(doc *document.UnifiedDocument, filter map[string]interface{}) bool {
	for key, value := range filter {
		fv, ok := doc.Fields[key]
		if !ok || fv == nil {
			return false
		}
		if fv.Scalar != value {
			return false
		}
	}
	return true
}

// --- Search Operations ---

// VectorSearch performs vector similarity search
func (c *UnifiedCollection) VectorSearch(field string, query []float32, k int, minScore float32) ([]*UnifiedSearchResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vecStore, ok := c.vectorStores[field]
	if !ok {
		return nil, fmt.Errorf("vector field %s not found", field)
	}

	results, err := vecStore.Search(query, k, minScore)
	if err != nil {
		return nil, err
	}

	// Convert to unified results
	unifiedResults := make([]*UnifiedSearchResult, len(results))
	for i, r := range results {
		unifiedResults[i] = &UnifiedSearchResult{
			ID:    r.ID,
			Score: r.Score,
			Type:  "vector",
		}
	}

	return unifiedResults, nil
}

// TextSearch performs full-text search
func (c *UnifiedCollection) TextSearch(field string, query string, limit int) ([]*UnifiedSearchResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	textStore, ok := c.textStores[field]
	if !ok {
		return nil, fmt.Errorf("text field %s not found", field)
	}

	results, err := textStore.Search(query, limit)
	if err != nil {
		return nil, err
	}

	// Convert to unified results
	unifiedResults := make([]*UnifiedSearchResult, len(results))
	for i, r := range results {
		unifiedResults[i] = &UnifiedSearchResult{
			ID:    r.ID,
			Score: float32(r.Score),
			Type:  "text",
		}
	}

	return unifiedResults, nil
}

// UnifiedSearchResult represents a search result from any search type
type UnifiedSearchResult struct {
	ID    string
	Score float32
	Type  string // "vector", "text", "filter", "hybrid"
}

// --- Unified Query ---

// UnifiedQuery represents a query that can combine multiple search types
type UnifiedQuery struct {
	// Filter conditions (exact match)
	Filter map[string]interface{} `json:"filter,omitempty"`

	// Vector similarity search
	Vector      []float32 `json:"vector,omitempty"`
	VectorField string    `json:"vectorField,omitempty"`
	TopK        int       `json:"topK,omitempty"`
	MinScore    float32   `json:"minScore,omitempty"`

	// Full-text search
	TextQuery  string `json:"textQuery,omitempty"`
	TextField  string `json:"textField,omitempty"`
	TextLimit  int    `json:"textLimit,omitempty"`

	// Hybrid scoring
	Scoring *ScoringConfig `json:"scoring,omitempty"`

	// Projection (fields to return)
	Projection []string `json:"projection,omitempty"`

	// Pagination
	Offset int `json:"offset,omitempty"`
	Limit  int `json:"limit,omitempty"`
}

// ScoringConfig defines how to combine scores from different search types
type ScoringConfig struct {
	Method string             `json:"method"` // "weighted", "rrf" (reciprocal rank fusion)
	Weights map[string]float32 `json:"weights,omitempty"` // "vector", "text", "filter"
}

// UnifiedQueryResult represents the result of a unified query
type UnifiedQueryResult struct {
	Documents []*document.UnifiedDocument `json:"documents"`
	Scores    []float32                  `json:"scores,omitempty"`
	Total     int                        `json:"total"`
}

// Query executes a unified query
func (c *UnifiedCollection) Query(q *UnifiedQuery) (*UnifiedQueryResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.queryPlanner.Execute(c, q)
}

// QueryPlanner plans and executes unified queries
type QueryPlanner struct{}

// NewQueryPlanner creates a new query planner
func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{}
}

// Execute executes a unified query
func (p *QueryPlanner) Execute(col *UnifiedCollection, q *UnifiedQuery) (*UnifiedQueryResult, error) {
	// Phase 1: Gather candidate IDs from each search type
	candidates := make(map[string]map[string]float64) // searchType -> docID -> score

	// Execute filter queries
	if q.Filter != nil {
		ids := p.executeFilter(col, q.Filter)
		candidates["filter"] = make(map[string]float64)
		for _, id := range ids {
			candidates["filter"][id] = 1.0
		}
	}

	// Execute vector search
	if q.Vector != nil && q.VectorField != "" {
		results := p.executeVectorSearch(col, q.VectorField, q.Vector, q.TopK)
		candidates["vector"] = results
	}

	// Execute text search
	if q.TextQuery != "" && q.TextField != "" {
		results := p.executeTextSearch(col, q.TextField, q.TextQuery, q.TextLimit)
		candidates["text"] = results
	}

	// Phase 2: Merge and score candidates
	merged := p.mergeCandidates(candidates, q.Scoring)

	// Phase 3: Apply pagination
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 10
	}

	if offset >= len(merged) {
		return &UnifiedQueryResult{Documents: []*document.UnifiedDocument{}, Total: len(merged)}, nil
	}

	end := offset + limit
	if end > len(merged) {
		end = len(merged)
	}

	// Phase 4: Load documents
	pagedIDs := merged[offset:end]
	docs := make([]*document.UnifiedDocument, len(pagedIDs))
	scores := make([]float32, len(pagedIDs))

	for i, item := range pagedIDs {
		doc, err := col.docStorage.Get(item.ID)
		if err != nil {
			continue
		}
		unified := document.ConvertFromLegacyDocument(doc)

		// Lazy load vectors if requested
		if len(q.Projection) > 0 {
			projectionSet := make(map[string]bool)
			for _, f := range q.Projection {
				projectionSet[f] = true
			}
			for fieldName, vecStore := range col.vectorStores {
				if projectionSet[fieldName] {
					if vec, err := vecStore.Get(item.ID); err == nil {
						unified.SetVector(fieldName, vec)
					}
				}
			}
		}

		docs[i] = unified
		scores[i] = item.Score
	}

	return &UnifiedQueryResult{
		Documents: docs,
		Scores:    scores,
		Total:     len(merged),
	}, nil
}

// executeFilter executes filter queries using indexes when available
func (p *QueryPlanner) executeFilter(col *UnifiedCollection, filter map[string]interface{}) []string {
	// Try to use index for first matching field
	for field, value := range filter {
		if idx, ok := col.scalarIndexes[field]; ok {
			ids, err := idx.Find(value)
			if err == nil && len(ids) > 0 {
				// Verify all filter conditions
				result := make([]string, 0)
				for _, id := range ids {
					doc, err := col.docStorage.Get(id)
					if err != nil {
						continue
					}
					unified := document.ConvertFromLegacyDocument(doc)
					if col.matchesFilter(unified, filter) {
						result = append(result, id)
					}
				}
				return result
			}
		}
	}

	// Fall back to full scan
	docs, err := col.docStorage.FindAll()
	if err != nil {
		return nil
	}

	result := make([]string, 0)
	for _, doc := range docs {
		unified := document.ConvertFromLegacyDocument(doc)
		if col.matchesFilter(unified, filter) {
			result = append(result, doc.ID)
		}
	}

	return result
}

// executeVectorSearch executes vector similarity search
func (p *QueryPlanner) executeVectorSearch(col *UnifiedCollection, field string, vector []float32, k int) map[string]float64 {
	vecStore, ok := col.vectorStores[field]
	if !ok {
		return nil
	}

	results, err := vecStore.Search(vector, k, 0)
	if err != nil {
		return nil
	}

	scores := make(map[string]float64)
	for _, r := range results {
		scores[r.ID] = float64(r.Score)
	}

	return scores
}

// ScoredID represents a document ID with its score
type ScoredID struct {
	ID    string
	Score float32
}

// mergeCandidates merges candidates from different search types
func (p *QueryPlanner) mergeCandidates(candidates map[string]map[string]float64, scoring *ScoringConfig) []ScoredID {
	if len(candidates) == 0 {
		return nil
	}

	// Collect all unique IDs
	allIDs := make(map[string]bool)
	for _, typeCandidates := range candidates {
		for id := range typeCandidates {
			allIDs[id] = true
		}
	}

	// Calculate final scores
	results := make([]ScoredID, 0, len(allIDs))

	for id := range allIDs {
		var score float64

		if scoring != nil && scoring.Method == "weighted" && scoring.Weights != nil {
			// Weighted sum
			for searchType, weight := range scoring.Weights {
				if s, ok := candidates[searchType][id]; ok {
					score += float64(weight) * s
				}
			}
		} else {
			// Default: sum all scores
			for _, typeCandidates := range candidates {
				if s, ok := typeCandidates[id]; ok {
					score += s
				}
			}
		}

		results = append(results, ScoredID{ID: id, Score: float32(score)})
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results
}

// executeTextSearch executes full-text search
func (p *QueryPlanner) executeTextSearch(col *UnifiedCollection, field string, query string, limit int) map[string]float64 {
	textStore, ok := col.textStores[field]
	if !ok {
		return nil
	}

	if limit <= 0 {
		limit = 10
	}

	results, err := textStore.Search(query, limit)
	if err != nil {
		return nil
	}

	scores := make(map[string]float64)
	for _, r := range results {
		scores[r.ID] = r.Score
	}

	return scores
}

// --- Index Management ---

// CreateScalarIndex creates a scalar index on a field
func (c *UnifiedCollection) CreateScalarIndex(field string, indexType string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.scalarIndexes[field]; exists {
		return fmt.Errorf("index already exists on field: %s", field)
	}

	var idx storage.Index
	if indexType == "hash" {
		idx = storage.NewHashIndex(fmt.Sprintf("%s_%s_idx", c.Name, field), field)
	} else {
		idx = storage.NewBTreeIndex(fmt.Sprintf("%s_%s_idx", c.Name, field), field, 64)
	}

	// Build index from existing documents
	docs, err := c.docStorage.FindAll()
	if err != nil {
		return fmt.Errorf("failed to build index: %w", err)
	}

	for _, doc := range docs {
		unified := document.ConvertFromLegacyDocument(doc)
		if fv, ok := unified.Fields[field]; ok && fv.Scalar != nil {
			idx.Insert(fv.Scalar, doc.ID)
		}
	}

	c.scalarIndexes[field] = idx
	return nil
}

// DropIndex drops an index
func (c *UnifiedCollection) DropIndex(field string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.scalarIndexes[field]; !exists {
		return fmt.Errorf("index not found on field: %s", field)
	}

	delete(c.scalarIndexes, field)
	return nil
}

// GetIndexes returns all scalar indexes
func (c *UnifiedCollection) GetIndexes() map[string]storage.Index {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]storage.Index)
	for k, v := range c.scalarIndexes {
		result[k] = v
	}
	return result
}

// GetVectorStore returns a vector store by field name
func (c *UnifiedCollection) GetVectorStore(field string) *storage.VectorColumnStore {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vectorStores[field]
}

// GetTextStore returns a text store by field name
func (c *UnifiedCollection) GetTextStore(field string) *storage.FullTextColumnStore {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.textStores[field]
}

// GetSchema returns the collection schema
func (c *UnifiedCollection) GetSchema() *document.UnifiedSchema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Schema
}

// SetSchema sets the collection schema
func (c *UnifiedCollection) SetSchema(schema *document.UnifiedSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Schema = schema
}

// Stats returns collection statistics
func (c *UnifiedCollection) Stats() UnifiedCollectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorStats := make(map[string]storage.VectorColumnStats)
	for field, store := range c.vectorStores {
		vectorStats[field] = store.Stats()
	}

	return UnifiedCollectionStats{
		Name:           c.Name,
		DocumentCount:  c.docStorage.Count(),
		IndexCount:     len(c.scalarIndexes),
		VectorFields:   len(c.vectorStores),
		TextFields:     len(c.textStores),
		VectorStats:    vectorStats,
		HasSchema:      c.Schema != nil,
	}
}

// UnifiedCollectionStats contains statistics about a unified collection
type UnifiedCollectionStats struct {
	Name           string
	DocumentCount  int
	IndexCount     int
	VectorFields   int
	TextFields     int
	VectorStats    map[string]storage.VectorColumnStats
	HasSchema      bool
}

// --- Utility functions ---

// min returns the minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
