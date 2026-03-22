package api

import (
	"aidb/internal/auth"
	"aidb/internal/rbac"
	"aidb/internal/vector"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// VectorHandler holds the vector API handlers and dependencies
type VectorHandler struct {
	vectorManager *vector.VectorManager
	authService   *auth.Service
	enforcer      *rbac.Enforcer
}

// NewVectorHandler creates a new vector API handler
func NewVectorHandler(vm *vector.VectorManager, authService *auth.Service, enforcer *rbac.Enforcer) *VectorHandler {
	return &VectorHandler{
		vectorManager: vm,
		authService:   authService,
		enforcer:      enforcer,
	}
}

// RegisterVectorRoutes registers all vector API routes
func (h *VectorHandler) RegisterVectorRoutes(mux *http.ServeMux, protected func(http.HandlerFunc) http.Handler) {
	// Vector Collection routes
	mux.Handle("GET /api/v1/vectors", protected(h.ListVectorCollections))
	mux.Handle("POST /api/v1/vectors", protected(h.CreateVectorCollection))
	mux.Handle("GET /api/v1/vectors/{name}", protected(h.GetVectorCollectionInfo))
	mux.Handle("DELETE /api/v1/vectors/{name}", protected(h.DropVectorCollection))

	// Vector Document routes
	mux.Handle("POST /api/v1/vectors/{name}/documents", protected(h.InsertVectorDocument))
	mux.Handle("GET /api/v1/vectors/{name}/documents", protected(h.FindVectorDocuments))
	mux.Handle("GET /api/v1/vectors/{name}/documents/{id}", protected(h.GetVectorDocument))
	mux.Handle("PUT /api/v1/vectors/{name}/documents/{id}", protected(h.UpdateVectorDocument))
	mux.Handle("PATCH /api/v1/vectors/{name}/documents/{id}", protected(h.PatchVectorDocument))
	mux.Handle("DELETE /api/v1/vectors/{name}/documents/{id}", protected(h.DeleteVectorDocument))

	// Vector Search route
	mux.Handle("POST /api/v1/vectors/{name}/search", protected(h.SearchVectors))

	// Export/Import routes
	mux.Handle("GET /api/v1/vectors/{name}/export", protected(h.ExportVectorCollection))
	mux.Handle("POST /api/v1/vectors/{name}/import", protected(h.ImportVectorCollection))
}

// CreateVectorCollectionRequest represents the request to create a vector collection
type CreateVectorCollectionRequest struct {
	Name          string                `json:"name"`
	Dimensions    int                   `json:"dimensions"`
	DistanceMetric vector.DistanceMetric `json:"distanceMetric,omitempty"`
}

// ListVectorCollections lists all vector collections
func (h *VectorHandler) ListVectorCollections(w http.ResponseWriter, r *http.Request) {
	collections := h.vectorManager.ListCollections()
	writeSuccess(w, map[string]interface{}{
		"collections": collections,
		"count":       len(collections),
	})
}

// CreateVectorCollection creates a new vector collection
func (h *VectorHandler) CreateVectorCollection(w http.ResponseWriter, r *http.Request) {
	var req CreateVectorCollectionRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "collection name is required")
		return
	}

	if req.Dimensions <= 0 {
		writeError(w, http.StatusBadRequest, "dimensions must be positive")
		return
	}

	// Default to cosine distance if not specified
	metric := req.DistanceMetric
	if metric == "" {
		metric = vector.DistanceCosine
	}

	// Validate distance metric
	validMetrics := map[vector.DistanceMetric]bool{
		vector.DistanceCosine:    true,
		vector.DistanceEuclidean: true,
		vector.DistanceDotProduct: true,
	}
	if !validMetrics[metric] {
		writeError(w, http.StatusBadRequest, "invalid distance metric: must be one of 'cosine', 'euclidean', or 'dot'")
		return
	}

	config := vector.NewVectorConfig(req.Dimensions, metric)

	col, err := h.vectorManager.CreateCollection(req.Name, config)
	if err != nil {
		if errors.Is(err, vector.ErrInvalidDimensions) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, vector.ErrInvalidDistanceMetric) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, Response{
		Success: true,
		Data: map[string]interface{}{
			"name":           col.Name,
			"dimensions":     col.Config.Dimensions,
			"distanceMetric": col.Config.DistanceMetric,
		},
	})
}

// GetVectorCollectionInfo returns information about a vector collection
func (h *VectorHandler) GetVectorCollectionInfo(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	col, err := h.vectorManager.GetCollection(name)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"name":           col.Name,
		"dimensions":     col.Config.Dimensions,
		"distanceMetric": col.Config.DistanceMetric,
		"count":          col.Count(),
	})
}

// DropVectorCollection deletes a vector collection
func (h *VectorHandler) DropVectorCollection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.vectorManager.DropCollection(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "vector collection deleted",
		"name":    name,
	})
}

// InsertVectorDocumentRequest represents the request to insert a vector document
type InsertVectorDocumentRequest struct {
	ID       string                 `json:"_id,omitempty"`
	Vector   []float32              `json:"vector"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// InsertVectorDocument inserts a new vector document into a collection
func (h *VectorHandler) InsertVectorDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req InsertVectorDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Vector) == 0 {
		writeError(w, http.StatusBadRequest, "vector is required")
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionCreate,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	var doc *vector.VectorDocument
	if req.ID != "" {
		doc = vector.NewVectorDocumentWithID(req.ID, req.Vector, req.Metadata)
	} else {
		doc = vector.NewVectorDocument(req.Vector, req.Metadata)
	}

	if err := col.Insert(doc); err != nil {
		if errors.Is(err, vector.ErrDimensionMismatch) {
			writeError(w, http.StatusBadRequest, "vector dimension mismatch: expected "+string(rune(col.Config.Dimensions)))
			return
		}
		if errors.Is(err, vector.ErrVectorExists) {
			writeError(w, http.StatusConflict, "vector document with this ID already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to insert vector document: "+err.Error())
		return
	}

	// Persist to disk
	h.vectorManager.PersistDocument(collectionName, doc)

	writeJSON(w, http.StatusCreated, Response{
		Success: true,
		Data:    doc,
	})
}

// GetVectorDocument retrieves a vector document by ID
func (h *VectorHandler) GetVectorDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	doc, err := col.Get(docID)
	if err != nil {
		if errors.Is(err, vector.ErrVectorNotFound) {
			writeError(w, http.StatusNotFound, "vector document not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get vector document: "+err.Error())
		return
	}

	// Check if vector should be included
	includeVector := r.URL.Query().Get("includeVector") == "true"
	if !includeVector {
		doc = &vector.VectorDocument{
			ID:         doc.ID,
			Dimensions: doc.Dimensions,
			Metadata:   doc.Metadata,
			CreatedAt:  doc.CreatedAt,
			UpdatedAt:  doc.UpdatedAt,
		}
	}

	writeSuccess(w, doc)
}

// UpdateVectorDocumentRequest represents the request to update a vector document
type UpdateVectorDocumentRequest struct {
	Vector   []float32              `json:"vector"`
	Metadata map[string]interface{} `json:"metadata"`
}

// UpdateVectorDocument updates an entire vector document
func (h *VectorHandler) UpdateVectorDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req UpdateVectorDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Vector) == 0 {
		writeError(w, http.StatusBadRequest, "vector is required")
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionUpdate,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	// Get existing document to preserve metadata if not provided
	existingDoc, err := col.Get(docID)
	if err != nil {
		if errors.Is(err, vector.ErrVectorNotFound) {
			writeError(w, http.StatusNotFound, "vector document not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get vector document: "+err.Error())
		return
	}

	// Update the document
	if req.Metadata != nil {
		existingDoc.Update(req.Vector, req.Metadata)
	} else {
		existingDoc.UpdateVector(req.Vector)
	}

	if err := col.Update(existingDoc); err != nil {
		if errors.Is(err, vector.ErrDimensionMismatch) {
			writeError(w, http.StatusBadRequest, "vector dimension mismatch")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update vector document: "+err.Error())
		return
	}

	// Persist to disk
	h.vectorManager.PersistDocument(collectionName, existingDoc)

	writeSuccess(w, existingDoc)
}

// PatchVectorDocumentRequest represents the request to patch a vector document
type PatchVectorDocumentRequest struct {
	Vector   []float32              `json:"vector,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// PatchVectorDocument partially updates a vector document
func (h *VectorHandler) PatchVectorDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req PatchVectorDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionUpdate,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	var doc *vector.VectorDocument
	if len(req.Vector) > 0 && req.Metadata != nil {
		doc, err = col.Patch(docID, req.Metadata)
		if err == nil {
			doc.UpdateVector(req.Vector)
			col.Update(doc)
		}
	} else if len(req.Vector) > 0 {
		doc, err = col.PatchVector(docID, req.Vector)
	} else if req.Metadata != nil {
		doc, err = col.Patch(docID, req.Metadata)
	} else {
		writeError(w, http.StatusBadRequest, "either vector or metadata must be provided")
		return
	}

	if err != nil {
		if errors.Is(err, vector.ErrVectorNotFound) {
			writeError(w, http.StatusNotFound, "vector document not found")
			return
		}
		if errors.Is(err, vector.ErrDimensionMismatch) {
			writeError(w, http.StatusBadRequest, "vector dimension mismatch")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to patch vector document: "+err.Error())
		return
	}

	// Persist to disk
	h.vectorManager.PersistDocument(collectionName, doc)

	writeSuccess(w, doc)
}

// DeleteVectorDocument deletes a vector document by ID
func (h *VectorHandler) DeleteVectorDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionDelete,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	if err := col.Delete(docID); err != nil {
		if errors.Is(err, vector.ErrVectorNotFound) {
			writeError(w, http.StatusNotFound, "vector document not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete vector document: "+err.Error())
		return
	}

	// Delete from disk
	h.vectorManager.DeleteDocument(collectionName, docID)

	writeSuccess(w, map[string]string{
		"message": "vector document deleted",
		"id":      docID,
	})
}

// FindVectorDocuments finds vector documents matching a metadata filter
func (h *VectorHandler) FindVectorDocuments(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	// Parse filter from query parameters
	filter := make(map[string]interface{})
	query := r.URL.Query()
	for key, values := range query {
		if key == "filter" {
			var jsonFilter map[string]interface{}
			if err := json.Unmarshal([]byte(values[0]), &jsonFilter); err == nil {
				filter = jsonFilter
			}
		}
	}

	var documents []*vector.VectorDocument
	if len(filter) > 0 {
		documents, err = col.Find(filter)
	} else {
		documents, err = col.FindAll()
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to find vector documents: "+err.Error())
		return
	}

	// Check if vectors should be included
	includeVector := r.URL.Query().Get("includeVector") == "true"
	if !includeVector {
		for _, doc := range documents {
			doc.Vector = nil
		}
	}

	writeSuccess(w, map[string]interface{}{
		"documents": documents,
		"count":     len(documents),
	})
}

// SearchVectorsRequest represents the request to search vectors
type SearchVectorsRequest struct {
	Vector        []float32              `json:"vector"`
	TopK          int                    `json:"topK"`
	MinScore      float32                `json:"minScore,omitempty"`
	Filter        map[string]interface{} `json:"filter,omitempty"`
	IncludeVector bool                   `json:"includeVector,omitempty"`
}

// SearchVectors performs similarity search on vectors
func (h *VectorHandler) SearchVectors(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.vectorManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req SearchVectorsRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Vector) == 0 {
		writeError(w, http.StatusBadRequest, "vector is required")
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	searchReq := &vector.SearchRequest{
		Vector:        req.Vector,
		TopK:          req.TopK,
		MinScore:      req.MinScore,
		Filter:        req.Filter,
		IncludeVector: req.IncludeVector,
	}

	response, err := col.Search(searchReq)
	if err != nil {
		if errors.Is(err, vector.ErrDimensionMismatch) {
			writeError(w, http.StatusBadRequest, "vector dimension mismatch")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to search vectors: "+err.Error())
		return
	}

	writeSuccess(w, response)
}

// ExportVectorCollection exports a vector collection to JSON
func (h *VectorHandler) ExportVectorCollection(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	export, err := h.vectorManager.ExportCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check if download is requested
	if r.URL.Query().Get("download") == "true" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename="+collectionName+".vectors.json")
		json.NewEncoder(w).Encode(export)
		return
	}

	writeSuccess(w, export)
}

// ImportVectorCollection imports a vector collection from JSON
func (h *VectorHandler) ImportVectorCollection(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionCreate,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	// Parse request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body: "+err.Error())
		return
	}
	defer r.Body.Close()

	var importData vector.VectorExportData
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
	if err := h.vectorManager.ImportCollection(&importData, overwrite); err != nil {
		if err.Error() == "vector collection already exists: "+importData.Name {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to import vector collection: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":       "vector collection imported successfully",
		"name":          importData.Name,
		"documentCount": len(importData.Documents),
		"dimensions":    importData.Config.Dimensions,
	})
}
