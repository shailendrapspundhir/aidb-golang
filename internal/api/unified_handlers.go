package api

import (
	"aidb/internal/auth"
	"aidb/internal/collection"
	"aidb/internal/document"
	"aidb/internal/rbac"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// UnifiedHandler holds the unified API handlers
type UnifiedHandler struct {
	manager     *collection.UnifiedManager
	authService *auth.Service
	enforcer    *rbac.Enforcer
}

// NewUnifiedHandler creates a new unified API handler
func NewUnifiedHandler(manager *collection.UnifiedManager, authService *auth.Service, enforcer *rbac.Enforcer) *UnifiedHandler {
	return &UnifiedHandler{
		manager:     manager,
		authService: authService,
		enforcer:    enforcer,
	}
}

// RegisterUnifiedRoutes registers all unified API routes
func (h *UnifiedHandler) RegisterUnifiedRoutes(mux *http.ServeMux, protected func(http.HandlerFunc) http.Handler) {
	// Collection routes
	mux.Handle("GET /api/v2/collections", protected(h.ListUnifiedCollections))
	mux.Handle("POST /api/v2/collections", protected(h.CreateUnifiedCollection))
	mux.Handle("GET /api/v2/collections/{name}", protected(h.GetUnifiedCollectionInfo))
	mux.Handle("DELETE /api/v2/collections/{name}", protected(h.DropUnifiedCollection))

	// Document routes
	mux.Handle("POST /api/v2/collections/{name}/documents", protected(h.InsertUnifiedDocument))
	mux.Handle("GET /api/v2/collections/{name}/documents", protected(h.FindUnifiedDocuments))
	mux.Handle("GET /api/v2/collections/{name}/documents/{id}", protected(h.GetUnifiedDocument))
	mux.Handle("PUT /api/v2/collections/{name}/documents/{id}", protected(h.UpdateUnifiedDocument))
	mux.Handle("PATCH /api/v2/collections/{name}/documents/{id}", protected(h.PatchUnifiedDocument))
	mux.Handle("DELETE /api/v2/collections/{name}/documents/{id}", protected(h.DeleteUnifiedDocument))

	// Query routes
	mux.Handle("POST /api/v2/collections/{name}/query", protected(h.ExecuteUnifiedQuery))
	mux.Handle("POST /api/v2/collections/{name}/search/vector", protected(h.VectorSearch))
	mux.Handle("POST /api/v2/collections/{name}/search/text", protected(h.TextSearch))

	// Schema routes
	mux.Handle("GET /api/v2/collections/{name}/schema", protected(h.GetUnifiedSchema))
	mux.Handle("PUT /api/v2/collections/{name}/schema", protected(h.SetUnifiedSchema))

	// Index routes
	mux.Handle("POST /api/v2/collections/{name}/indexes", protected(h.CreateUnifiedIndex))
	mux.Handle("GET /api/v2/collections/{name}/indexes", protected(h.ListUnifiedIndexes))
	mux.Handle("DELETE /api/v2/collections/{name}/indexes/{field}", protected(h.DropUnifiedIndex))

	// Export/Import routes
	mux.Handle("GET /api/v2/collections/{name}/export", protected(h.ExportUnifiedCollection))
	mux.Handle("POST /api/v2/collections/{name}/import", protected(h.ImportUnifiedCollection))

	// Stats routes
	mux.Handle("GET /api/v2/collections/{name}/stats", protected(h.GetUnifiedCollectionStats))

	// Aggregation route
	mux.Handle("POST /api/v2/collections/{name}/aggregate", protected(h.ExecuteUnifiedAggregation))
}

// --- Collection Handlers ---

// ListUnifiedCollections lists all unified collections
func (h *UnifiedHandler) ListUnifiedCollections(w http.ResponseWriter, r *http.Request) {
	collections := h.manager.ListCollections()
	writeSuccess(w, map[string]interface{}{
		"collections": collections,
		"count":       len(collections),
	})
}

// CreateUnifiedCollectionRequest represents the request to create a unified collection
type CreateUnifiedCollectionRequest struct {
	Name   string                    `json:"name"`
	Schema *document.UnifiedSchema   `json:"schema,omitempty"`
}

// CreateUnifiedCollection creates a new unified collection
func (h *UnifiedHandler) CreateUnifiedCollection(w http.ResponseWriter, r *http.Request) {
	var req CreateUnifiedCollectionRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "collection name is required")
		return
	}

	col, err := h.manager.CreateCollection(req.Name, req.Schema)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, Response{
		Success: true,
		Data: map[string]interface{}{
			"name":      col.Name,
			"hasSchema": col.Schema != nil,
		},
	})
}

// GetUnifiedCollectionInfo returns information about a unified collection
func (h *UnifiedHandler) GetUnifiedCollectionInfo(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	col, err := h.manager.GetCollection(name)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	stats := col.Stats()
	writeSuccess(w, map[string]interface{}{
		"name":          col.Name,
		"hasSchema":     col.Schema != nil,
		"documentCount": stats.DocumentCount,
		"vectorFields":  stats.VectorFields,
		"textFields":    stats.TextFields,
		"indexCount":    stats.IndexCount,
	})
}

// DropUnifiedCollection deletes a unified collection
func (h *UnifiedHandler) DropUnifiedCollection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.manager.DropCollection(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "collection deleted",
		"name":    name,
	})
}

// --- Document Handlers ---

// InsertUnifiedDocumentRequest represents the request to insert a unified document
type InsertUnifiedDocumentRequest struct {
	ID     string                 `json:"_id,omitempty"`
	Fields map[string]interface{} `json:"fields"`
}

// InsertUnifiedDocument inserts a new document into a unified collection
func (h *UnifiedHandler) InsertUnifiedDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req InsertUnifiedDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Create unified document
	var doc *document.UnifiedDocument
	if req.ID != "" {
		doc = document.NewUnifiedDocumentWithID(req.ID)
	} else {
		doc = document.NewUnifiedDocument()
	}
	doc.FromMap(req.Fields)

	if err := col.Insert(doc); err != nil {
		var validationErr *document.ValidationError
		if errors.As(err, &validationErr) {
			writeError(w, http.StatusBadRequest, "validation error: "+err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to insert document: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, Response{
		Success: true,
		Data:    doc,
	})
}

// GetUnifiedDocument retrieves a document by ID
func (h *UnifiedHandler) GetUnifiedDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse projection from query params
	var projection []string
	if proj := r.URL.Query().Get("projection"); proj != "" {
		if err := json.Unmarshal([]byte(proj), &projection); err != nil {
			projection = nil
		}
	}

	doc, err := col.GetWithProjection(docID, projection)
	if err != nil {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}

	writeSuccess(w, doc)
}

// UpdateUnifiedDocumentRequest represents the request to update a unified document
type UpdateUnifiedDocumentRequest struct {
	Fields map[string]interface{} `json:"fields"`
}

// UpdateUnifiedDocument updates an entire document
func (h *UnifiedHandler) UpdateUnifiedDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req UpdateUnifiedDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Get existing document
	doc, err := col.Get(docID)
	if err != nil {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}

	// Update fields
	doc.FromMap(req.Fields)

	if err := col.Update(doc); err != nil {
		var validationErr *document.ValidationError
		if errors.As(err, &validationErr) {
			writeError(w, http.StatusBadRequest, "validation error: "+err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update document: "+err.Error())
		return
	}

	writeSuccess(w, doc)
}

// PatchUnifiedDocument partially updates a document
func (h *UnifiedHandler) PatchUnifiedDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req UpdateUnifiedDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Get existing document
	doc, err := col.Get(docID)
	if err != nil {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}

	// Merge fields
	doc.FromMap(req.Fields)

	if err := col.Update(doc); err != nil {
		var validationErr *document.ValidationError
		if errors.As(err, &validationErr) {
			writeError(w, http.StatusBadRequest, "validation error: "+err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to patch document: "+err.Error())
		return
	}

	writeSuccess(w, doc)
}

// DeleteUnifiedDocument deletes a document by ID
func (h *UnifiedHandler) DeleteUnifiedDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := col.Delete(docID); err != nil {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}

	writeSuccess(w, map[string]string{
		"message": "document deleted",
		"id":      docID,
	})
}

// FindUnifiedDocuments finds documents matching a filter
func (h *UnifiedHandler) FindUnifiedDocuments(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse filter from query params
	filter := make(map[string]interface{})
	if filterStr := r.URL.Query().Get("filter"); filterStr != "" {
		if err := json.Unmarshal([]byte(filterStr), &filter); err != nil {
			writeError(w, http.StatusBadRequest, "invalid filter")
			return
		}
	}

	var documents []*document.UnifiedDocument
	if len(filter) > 0 {
		documents, err = col.Find(filter)
	} else {
		documents, err = col.FindAll()
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to find documents: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"documents": documents,
		"count":     len(documents),
	})
}

// --- Query Handlers ---

// ExecuteUnifiedQuery executes a unified query
func (h *UnifiedHandler) ExecuteUnifiedQuery(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var query collection.UnifiedQuery
	if err := parseJSONBody(r, &query); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	result, err := col.Query(&query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed: "+err.Error())
		return
	}

	writeSuccess(w, result)
}

// VectorSearchRequest represents a vector search request
type VectorSearchRequest struct {
	Vector   []float32 `json:"vector"`
	Field    string    `json:"field"`
	TopK     int       `json:"topK"`
	MinScore float32   `json:"minScore,omitempty"`
}

// VectorSearch performs vector similarity search
func (h *UnifiedHandler) VectorSearch(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req VectorSearchRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Vector) == 0 {
		writeError(w, http.StatusBadRequest, "vector is required")
		return
	}

	if req.Field == "" {
		req.Field = "embedding" // default field
	}

	if req.TopK <= 0 {
		req.TopK = 10
	}

	results, err := col.VectorSearch(req.Field, req.Vector, req.TopK, req.MinScore)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "vector search failed: "+err.Error())
		return
	}

	// Load documents for results
	docs := make([]*document.UnifiedDocument, len(results))
	for i, r := range results {
		doc, err := col.Get(r.ID)
		if err == nil {
			docs[i] = doc
		}
	}

	writeSuccess(w, map[string]interface{}{
		"results":   results,
		"documents": docs,
		"count":     len(results),
	})
}

// TextSearchRequest represents a text search request
type TextSearchRequest struct {
	Query string `json:"query"`
	Field string `json:"field"`
	Limit int    `json:"limit"`
}

// TextSearch performs full-text search
func (h *UnifiedHandler) TextSearch(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req TextSearchRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Query == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return
	}

	if req.Field == "" {
		req.Field = "content" // default field
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	results, err := col.TextSearch(req.Field, req.Query, req.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "text search failed: "+err.Error())
		return
	}

	// Load documents for results
	docs := make([]*document.UnifiedDocument, len(results))
	for i, r := range results {
		doc, err := col.Get(r.ID)
		if err == nil {
			docs[i] = doc
		}
	}

	writeSuccess(w, map[string]interface{}{
		"results":   results,
		"documents": docs,
		"count":     len(results),
	})
}

// --- Schema Handlers ---

// GetUnifiedSchema returns the schema for a collection
func (h *UnifiedHandler) GetUnifiedSchema(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	schema := col.GetSchema()
	if schema == nil {
		writeSuccess(w, map[string]interface{}{
			"hasSchema": false,
			"message":   "collection is schemaless",
		})
		return
	}

	writeSuccess(w, map[string]interface{}{
		"hasSchema": true,
		"schema":    schema,
	})
}

// SetUnifiedSchemaRequest represents the request to set a schema
type SetUnifiedSchemaRequest struct {
	Schema *document.UnifiedSchema `json:"schema"`
}

// SetUnifiedSchema sets or updates the schema for a collection
func (h *UnifiedHandler) SetUnifiedSchema(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	var req SetUnifiedSchemaRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := h.manager.UpdateSchema(collectionName, req.Schema); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update schema: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":   "schema updated",
		"hasSchema": req.Schema != nil,
	})
}

// --- Index Handlers ---

// CreateUnifiedIndexRequest represents the request to create an index
type CreateUnifiedIndexRequest struct {
	Field     string `json:"field"`
	IndexType string `json:"indexType"` // "btree" or "hash"
}

// CreateUnifiedIndex creates an index on a field
func (h *UnifiedHandler) CreateUnifiedIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req CreateUnifiedIndexRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Field == "" {
		writeError(w, http.StatusBadRequest, "field is required")
		return
	}

	if err := col.CreateScalarIndex(req.Field, req.IndexType); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create index: "+err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "index created",
		"field":   req.Field,
	})
}

// ListUnifiedIndexes lists all indexes for a collection
func (h *UnifiedHandler) ListUnifiedIndexes(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	indexes := col.GetIndexes()
	indexList := make([]map[string]interface{}, 0, len(indexes))
	for field, idx := range indexes {
		indexList = append(indexList, map[string]interface{}{
			"field": field,
			"name":  idx.Name(),
			"type":  idx.Type(),
			"count": idx.Count(),
		})
	}

	writeSuccess(w, map[string]interface{}{
		"indexes": indexList,
		"count":   len(indexList),
	})
}

// DropUnifiedIndex drops an index
func (h *UnifiedHandler) DropUnifiedIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	field := r.PathValue("field")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := col.DropIndex(field); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "index dropped",
		"field":   field,
	})
}

// --- Export/Import Handlers ---

// ExportUnifiedCollection exports a collection to JSON
func (h *UnifiedHandler) ExportUnifiedCollection(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	export, err := h.manager.ExportCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check if download is requested
	if r.URL.Query().Get("download") == "true" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename="+collectionName+".export.json")
		json.NewEncoder(w).Encode(export)
		return
	}

	writeSuccess(w, export)
}

// ImportUnifiedCollection imports a collection from JSON
func (h *UnifiedHandler) ImportUnifiedCollection(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	// Parse request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body: "+err.Error())
		return
	}
	defer r.Body.Close()

	var importData collection.UnifiedExportData
	if err := json.Unmarshal(body, &importData); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON format: "+err.Error())
		return
	}

	// If name is not set, use the URL parameter
	if importData.Name == "" {
		importData.Name = collectionName
	}

	// Check for overwrite parameter
	overwrite := r.URL.Query().Get("overwrite") == "true"

	// Import the collection
	if err := h.manager.ImportCollection(&importData, overwrite); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to import collection: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":       "collection imported successfully",
		"name":          importData.Name,
		"documentCount": len(importData.Documents),
	})
}

// --- Stats Handlers ---

// GetUnifiedCollectionStats returns statistics for a collection
func (h *UnifiedHandler) GetUnifiedCollectionStats(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	stats := col.Stats()
	writeSuccess(w, stats)
}

// UnifiedAggregationRequest represents an aggregation pipeline request
type UnifiedAggregationRequest struct {
	Pipeline []collection.UnifiedPipelineStage `json:"pipeline"`
	Explain  bool                              `json:"explain,omitempty"`
}

// ExecuteUnifiedAggregation executes an aggregation pipeline on a unified collection
func (h *UnifiedHandler) ExecuteUnifiedAggregation(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.manager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req UnifiedAggregationRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Pipeline) == 0 {
		writeError(w, http.StatusBadRequest, "pipeline is required and cannot be empty")
		return
	}

	// Create the aggregation pipeline
	pipeline := collection.NewUnifiedAggregationPipeline(req.Pipeline...)

	// Execute the pipeline
	results, err := col.ExecuteAggregation(pipeline)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "aggregation failed: "+err.Error())
		return
	}

	// Return results
	writeSuccess(w, map[string]interface{}{
		"results": results,
		"count":   len(results),
	})
}
