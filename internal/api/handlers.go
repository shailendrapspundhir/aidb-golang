package api

import (
	"aidb/internal/auth"
	"aidb/internal/collection"
	"aidb/internal/document"
	"aidb/internal/fulltext"
	"aidb/internal/rbac"
	"aidb/internal/storage"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// Handler holds the API handlers and dependencies
type Handler struct {
	collectionManager *collection.Manager
	authService       *auth.Service
	enforcer          *rbac.Enforcer
}

// NewHandler creates a new API handler
func NewHandler(cm *collection.Manager, authService *auth.Service, enforcer *rbac.Enforcer) *Handler {
	return &Handler{
		collectionManager: cm,
		authService:       authService,
		enforcer:          enforcer,
	}
}

// RegisterRoutes registers all API routes
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Public routes
	mux.HandleFunc("POST /api/v1/login", h.Login)
	mux.HandleFunc("POST /api/v1/register", h.Register)
	mux.HandleFunc("GET /api/v1/health", h.HealthCheck)

	// Swagger documentation routes (public)
	mux.HandleFunc("GET /api/v1/swagger.json", HandleSwaggerJSON)
	mux.HandleFunc("GET /api/v1/docs", HandleSwaggerUI)

	// Protected routes
	// We wrap these with AuthMiddleware and RBACMiddleware
	
	// Helper to chain middleware
	protected := func(handlerFunc http.HandlerFunc) http.Handler {
		return h.AuthMiddleware(h.RBACMiddleware(http.HandlerFunc(handlerFunc)))
	}

	// Collection routes
	mux.Handle("GET /api/v1/collections", protected(h.ListCollections))
	mux.Handle("POST /api/v1/collections", protected(h.CreateCollection))
	mux.Handle("GET /api/v1/collections/{name}", protected(h.GetCollectionInfo))
	mux.Handle("DELETE /api/v1/collections/{name}", protected(h.DropCollection))

	// Document routes
	mux.Handle("POST /api/v1/collections/{name}/documents", protected(h.InsertDocument))
	mux.Handle("GET /api/v1/collections/{name}/documents", protected(h.FindDocuments))
	mux.Handle("GET /api/v1/collections/{name}/documents/{id}", protected(h.GetDocument))
	mux.Handle("PUT /api/v1/collections/{name}/documents/{id}", protected(h.UpdateDocument))
	mux.Handle("PATCH /api/v1/collections/{name}/documents/{id}", protected(h.PatchDocument))
	mux.Handle("DELETE /api/v1/collections/{name}/documents/{id}", protected(h.DeleteDocument))

	// Schema routes
	mux.Handle("GET /api/v1/collections/{name}/schema", protected(h.GetSchema))
	mux.Handle("PUT /api/v1/collections/{name}/schema", protected(h.SetSchema))

	// Index routes
	mux.Handle("POST /api/v1/collections/{name}/indexes", protected(h.CreateIndex))
	mux.Handle("GET /api/v1/collections/{name}/indexes", protected(h.ListIndexes))
	mux.Handle("DELETE /api/v1/collections/{name}/indexes/{field}", protected(h.DropIndex))

	// Full-text search routes
	mux.Handle("GET /api/v1/collections/{name}/fulltext-index", protected(h.GetFullTextIndex))
	mux.Handle("POST /api/v1/collections/{name}/fulltext-index", protected(h.CreateFullTextIndex))
	mux.Handle("DELETE /api/v1/collections/{name}/fulltext-index", protected(h.DeleteFullTextIndex))
	mux.Handle("POST /api/v1/collections/{name}/fulltext-index/rebuild", protected(h.RebuildFullTextIndex))
	mux.Handle("POST /api/v1/collections/{name}/search", protected(h.FullTextSearch))

	// Aggregation routes
	mux.Handle("POST /api/v1/collections/{name}/aggregate", protected(h.handleAggregation))
	mux.Handle("GET /api/v1/collections/{name}/distinct/{field}", protected(h.handleDistinct))
	mux.Handle("GET /api/v1/collections/{name}/stats", protected(h.handleStats))

	// Export/Import routes
	mux.Handle("GET /api/v1/collections/{name}/export", protected(h.ExportCollection))
	mux.Handle("POST /api/v1/collections/{name}/import", protected(h.ImportCollection))
	
	// Auth & RBAC Management Routes
	mux.Handle("POST /api/v1/apikeys", protected(h.CreateAPIKey))
	mux.Handle("POST /api/v1/roles", protected(h.CreateRole))
	mux.Handle("POST /api/v1/users/roles", protected(h.AssignRole))
	
	// Hierarchy Routes
	mux.Handle("POST /api/v1/tenants", protected(h.CreateTenant))
	mux.Handle("GET /api/v1/tenants", protected(h.ListTenants))
	mux.Handle("DELETE /api/v1/tenants/{id}", protected(h.DeleteTenant))

	mux.Handle("POST /api/v1/regions", protected(h.CreateRegion))
	mux.Handle("GET /api/v1/regions", protected(h.ListRegions))
	mux.Handle("DELETE /api/v1/regions/{id}", protected(h.DeleteRegion))

	mux.Handle("POST /api/v1/environments", protected(h.CreateEnvironment))
	mux.Handle("GET /api/v1/environments", protected(h.ListEnvironments))
	mux.Handle("DELETE /api/v1/environments/{id}", protected(h.DeleteEnvironment))
}

// Auth Handlers

func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	var req auth.LoginRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	resp, err := h.authService.Login(req)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	writeSuccess(w, resp)
}

func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	var req auth.RegisterRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	user, err := h.authService.Register(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeSuccess(w, user)
}

type CreateAPIKeyRequest struct {
	Name      string `json:"name"`
	ExpiresIn int64  `json:"expiresIn"` // Seconds
}

func (h *Handler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(UserContextKey).(*auth.User)
	
	var req CreateAPIKeyRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	var expiry *time.Time
	if req.ExpiresIn > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
		expiry = &t
	}

	apiKey, rawKey, err := h.authService.CreateAPIKey(user.ID, req.Name, expiry)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"apiKey": apiKey,
		"key":    rawKey, // Only shown once
	})
}

type CreateRoleRequest struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Policies    []rbac.Policy `json:"policies"`
}

func (h *Handler) CreateRole(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(UserContextKey).(*auth.User)
	
	var req CreateRoleRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if err := h.enforcer.CreateRole(req.Name, user.TenantID, req.Description, req.Policies); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeSuccess(w, map[string]string{"message": "role created"})
}

type AssignRoleRequest struct {
	UserID string `json:"userId"`
	RoleID string `json:"roleId"`
}

func (h *Handler) AssignRole(w http.ResponseWriter, r *http.Request) {
	// Only super admin or tenant admin should be able to assign roles
	// RBAC middleware handles general access, but granular check might be needed?
	// For now rely on RBAC policy for "users/roles" resource.
	// Resource path: collection/users/roles? No.
	// URL: /api/v1/users/roles
	// RBAC Middleware infers resource from URL.
	// pathParts[3] = "users".
	// So resource = "users".
	// Action = "create".
	
	var req AssignRoleRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if err := h.authService.AssignRole(req.UserID, req.RoleID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeSuccess(w, map[string]string{"message": "role assigned"})
}


// Response types
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type CreateCollectionRequest struct {
	Name   string                  `json:"name"`
	Schema *document.Schema       `json:"schema,omitempty"`
	Strict bool                    `json:"strict,omitempty"`
}

type InsertDocumentRequest struct {
	ID   string                 `json:"_id,omitempty"`
	Data map[string]interface{} `json:"data"`
}

type UpdateDocumentRequest struct {
	Data map[string]interface{} `json:"data"`
}

type PatchDocumentRequest struct {
	Data map[string]interface{} `json:"data"`
}

type FindDocumentsRequest struct {
	Filter map[string]interface{} `json:"filter,omitempty"`
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// writeSuccess writes a success response
func writeSuccess(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    data,
	})
}

// writeError writes an error response
func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, Response{
		Success: false,
		Error:   message,
	})
}

// parseJSONBody parses a JSON request body
func parseJSONBody(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	return json.Unmarshal(body, v)
}

// HealthCheck returns the health status of the API
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeSuccess(w, map[string]string{
		"status": "healthy",
	})
}

// ListCollections lists all collections
func (h *Handler) ListCollections(w http.ResponseWriter, r *http.Request) {
	collections := h.collectionManager.ListCollections()
	writeSuccess(w, map[string]interface{}{
		"collections": collections,
		"count":      len(collections),
	})
}

// CreateCollection creates a new collection
func (h *Handler) CreateCollection(w http.ResponseWriter, r *http.Request) {
	var req CreateCollectionRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "collection name is required")
		return
	}

	// Create schema if provided
	var schema *document.Schema
	if req.Schema != nil {
		schema = req.Schema
	} else if req.Strict {
		// Create an empty strict schema
		schema = document.NewSchema(req.Name, true)
	}

	col, err := h.collectionManager.CreateCollection(req.Name, schema)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, Response{
		Success: true,
		Data: map[string]interface{}{
			"name":      col.Name,
			"hasSchema": col.Schema != nil,
			"strict":    col.Schema != nil && col.Schema.Strict,
		},
	})
}

// GetCollectionInfo returns information about a collection
func (h *Handler) GetCollectionInfo(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	col, err := h.collectionManager.GetCollection(name)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"name":      col.Name,
		"hasSchema": col.Schema != nil,
		"strict":    col.Schema != nil && col.Schema.Strict,
		"count":     col.Count(),
	})
}

// DropCollection deletes a collection
func (h *Handler) DropCollection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.collectionManager.DropCollection(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "collection deleted",
		"name":    name,
	})
}

// InsertDocument inserts a new document into a collection
func (h *Handler) InsertDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req InsertDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Data == nil {
		writeError(w, http.StatusBadRequest, "data field is required")
		return
	}

	// Check field permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	var fields []string
	for k := range req.Data {
		fields = append(fields, k)
	}
	
	rbacCtx := rbac.RequestContext{
		TenantID:     user.TenantID,
		Collection:   collectionName,
		Action:       rbac.ActionCreate, // Insert is Create
		TargetFields: fields,
	}
	
	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied for one or more fields")
		return
	}

	var doc *document.Document
	if req.ID != "" {
		doc = document.NewDocumentWithID(req.ID, req.Data)
	} else {
		doc = document.NewDocument(req.Data)
	}

	if err := col.Insert(doc); err != nil {
		var validationErr *document.ValidationError
		if errors.As(err, &validationErr) {
			writeError(w, http.StatusBadRequest, "validation error: "+err.Error())
			return
		}
		if errors.Is(err, storage.ErrDocumentExists) {
			writeError(w, http.StatusConflict, "document with this ID already exists")
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

// GetDocument retrieves a document by ID
func (h *Handler) GetDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	doc, err := col.Get(docID)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			writeError(w, http.StatusNotFound, "document not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get document: "+err.Error())
		return
	}

	// Filter fields based on permission
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}
	allowedFields, err := h.enforcer.GetAllowedFields(user.Roles, user.TenantID, rbacCtx)
	if err == nil && allowedFields != nil {
		// If "*", return all.
		isAll := false
		for _, f := range allowedFields {
			if f == "*" { isAll = true; break }
		}
		
		if !isAll {
			// Filter doc.Data
			newData := make(map[string]interface{})
			for _, f := range allowedFields {
				if v, ok := doc.Data[f]; ok {
					newData[f] = v
				}
			}
			doc.Data = newData
		}
	}

	writeSuccess(w, doc)
}

// UpdateDocument updates an entire document
func (h *Handler) UpdateDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req UpdateDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Data == nil {
		writeError(w, http.StatusBadRequest, "data field is required")
		return
	}

	// Check field permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	var fields []string
	for k := range req.Data {
		fields = append(fields, k)
	}
	
	rbacCtx := rbac.RequestContext{
		TenantID:     user.TenantID,
		Collection:   collectionName,
		Action:       rbac.ActionUpdate,
		TargetFields: fields,
	}
	
	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied for one or more fields")
		return
	}

	// Get existing document to preserve metadata
	existingDoc, err := col.Get(docID)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			writeError(w, http.StatusNotFound, "document not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get document: "+err.Error())
		return
	}

	// Update the document data
	existingDoc.Update(req.Data)

	if err := col.Update(existingDoc); err != nil {
		var validationErr *document.ValidationError
		if errors.As(err, &validationErr) {
			writeError(w, http.StatusBadRequest, "validation error: "+err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update document: "+err.Error())
		return
	}

	writeSuccess(w, existingDoc)
}

// PatchDocument partially updates a document
func (h *Handler) PatchDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req PatchDocumentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Data == nil {
		writeError(w, http.StatusBadRequest, "data field is required")
		return
	}

	// Check field permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	var fields []string
	for k := range req.Data {
		fields = append(fields, k)
	}
	
	rbacCtx := rbac.RequestContext{
		TenantID:     user.TenantID,
		Collection:   collectionName,
		Action:       rbac.ActionUpdate, // Patch is Update
		TargetFields: fields,
	}
	
	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied for one or more fields")
		return
	}

	doc, err := col.Patch(docID, req.Data)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			writeError(w, http.StatusNotFound, "document not found")
			return
		}
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

// DeleteDocument deletes a document by ID
func (h *Handler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	docID := r.PathValue("id")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := col.Delete(docID); err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			writeError(w, http.StatusNotFound, "document not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete document: "+err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "document deleted",
		"id":      docID,
	})
}

// FindDocuments finds documents matching a filter
func (h *Handler) FindDocuments(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check if there's a filter in the query parameters
	filter := make(map[string]interface{})
	
	// Parse query parameters for simple filtering
	query := r.URL.Query()
	for key, values := range query {
		if key == "filter" {
			// Parse JSON filter from query parameter
			var jsonFilter map[string]interface{}
			if err := json.Unmarshal([]byte(values[0]), &jsonFilter); err == nil {
				filter = jsonFilter
			}
		} else if !strings.HasPrefix(key, "_") {
			// Simple key=value filter from query params
			if len(values) > 0 {
				filter[key] = values[0]
			}
		}
	}

	var documents []*document.Document
	if len(filter) > 0 {
		documents, err = col.Find(filter)
	} else {
		documents, err = col.FindAll()
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to find documents: "+err.Error())
		return
	}

	// Filter fields based on permission
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}
	allowedFields, err := h.enforcer.GetAllowedFields(user.Roles, user.TenantID, rbacCtx)
	if err == nil && allowedFields != nil {
		isAll := false
		for _, f := range allowedFields {
			if f == "*" { isAll = true; break }
		}
		
		if !isAll {
			for _, doc := range documents {
				newData := make(map[string]interface{})
				for _, f := range allowedFields {
					if v, ok := doc.Data[f]; ok {
						newData[f] = v
					}
				}
				doc.Data = newData
			}
		}
	}

	writeSuccess(w, map[string]interface{}{
		"documents": documents,
		"count":     len(documents),
	})
}

// GetSchema returns the schema for a collection
func (h *Handler) GetSchema(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.collectionManager.GetCollection(collectionName)
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

// SetSchema sets or updates the schema for a collection
func (h *Handler) SetSchema(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	_, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req struct {
		Schema *document.Schema `json:"schema"`
	}
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Use manager's UpdateSchema to ensure persistence
	if err := h.collectionManager.UpdateSchema(collectionName, req.Schema); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update schema: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":   "schema updated",
		"hasSchema": req.Schema != nil,
	})
}

// ExportCollection exports a collection to JSON
func (h *Handler) ExportCollection(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	export, err := h.collectionManager.ExportCollection(collectionName)
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

// ImportCollection imports a collection from JSON
func (h *Handler) ImportCollection(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	// Parse request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body: "+err.Error())
		return
	}
	defer r.Body.Close()

	// Check if this is a full export format or just documents
	var importData collection.ExportData
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
	if err := h.collectionManager.ImportCollection(&importData, overwrite); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to import collection: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":       "collection imported successfully",
		"name":          importData.Name,
		"documentCount": len(importData.Documents),
		"hasSchema":     importData.HasSchema,
	})
}

// Index Handlers

// CreateIndexRequest represents a request to create an index
type CreateIndexRequest struct {
	Field string          `json:"field"`
	Type  storage.IndexType `json:"type"`
}

// CreateIndex creates an index on a collection field
func (h *Handler) CreateIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req CreateIndexRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Field == "" {
		writeError(w, http.StatusBadRequest, "field is required")
		return
	}

	if req.Type == "" {
		req.Type = storage.IndexTypeBTree // Default to B-tree
	}

	if err := col.CreateIndex(req.Field, req.Type); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create index: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":     "index created successfully",
		"collection":  collectionName,
		"field":       req.Field,
		"indexType":   req.Type,
	})
}

// ListIndexes lists all indexes on a collection
func (h *Handler) ListIndexes(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	indexes := col.GetIndexes()
	indexList := make([]map[string]interface{}, 0, len(indexes))
	for field, idx := range indexes {
		indexList = append(indexList, map[string]interface{}{
			"field":     field,
			"type":      idx.Type(),
			"name":      idx.Name(),
			"entryCount": idx.Count(),
		})
	}

	writeSuccess(w, map[string]interface{}{
		"collection": collectionName,
		"indexes":    indexList,
		"count":      len(indexList),
	})
}

// DropIndex drops an index from a collection
func (h *Handler) DropIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	field := r.PathValue("field")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := col.DropIndex(field); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to drop index: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":    "index dropped successfully",
		"collection": collectionName,
		"field":      field,
	})
}

// FullTextSearchRequest represents a full-text search request.
// Supports both simple query string and JSON-based structured query (like aggregation $match).
type FullTextSearchRequest struct {
	// Simple query string (backward compatible)
	Query string `json:"q"`

	// Structured JSON query (like aggregation $match operators)
	// Supports: $text, $regex, $phrase, $fuzzy, $caseSensitive
	QueryObject map[string]interface{} `json:"query,omitempty"`

	// Pagination
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"` // Number of results to skip (for pagination)

	// Search options
	Phrase        bool `json:"phrase,omitempty"`        // If true, require terms to be adjacent (phrase search)
	Fuzzy         bool `json:"fuzzy,omitempty"`         // If true, use fuzzy matching (edit distance)
	MaxFuzzyDist  int  `json:"maxFuzzyDist,omitempty"`  // Max edit distance for fuzzy (default 2)
	CaseSensitive bool `json:"caseSensitive,omitempty"` // If true, preserve case (default false = case-insensitive)

	// Field-specific search (optional filter)
	Fields []string `json:"fields,omitempty"` // If specified, search only within these fields
}

// FullTextSearch performs a full-text search on a collection.
// Supports both simple query string (q) and JSON-based structured query (query object).
// JSON query supports operators: $text, $regex, $phrase, $fuzzy, $caseSensitive
func (h *Handler) FullTextSearch(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req FullTextSearchRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Parse QueryObject if provided (JSON-based query like aggregation $match)
	var regexPattern string
	if req.QueryObject != nil {
		// Extract $text as alternative to q
		if text, ok := req.QueryObject["$text"].(string); ok && req.Query == "" {
			req.Query = text
		}
		// Extract $regex pattern
		if regex, ok := req.QueryObject["$regex"].(string); ok {
			regexPattern = regex
		} else if regexMap, ok := req.QueryObject["$regex"].(map[string]interface{}); ok {
			// Support $regex: {"field": "pattern"} - use first pattern found
			for _, v := range regexMap {
				if s, ok := v.(string); ok {
					regexPattern = s
					break
				}
			}
		}
		// Extract $phrase
		if phrase, ok := req.QueryObject["$phrase"].(bool); ok {
			req.Phrase = phrase
		}
		// Extract $fuzzy
		if fuzzy, ok := req.QueryObject["$fuzzy"].(bool); ok {
			req.Fuzzy = fuzzy
		}
		// Extract $caseSensitive
		if cs, ok := req.QueryObject["$caseSensitive"].(bool); ok {
			req.CaseSensitive = cs
		}
	}

	// Require either q or $text from query object
	if req.Query == "" && regexPattern == "" {
		writeError(w, http.StatusBadRequest, "query 'q' or 'query.$text' or 'query.$regex' is required")
		return
	}

	if req.Limit <= 0 {
		req.Limit = 20
	}

	idx := col.GetFullTextIndex()
	if idx == nil {
		writeError(w, http.StatusBadRequest, "no full-text index exists on this collection; create one first")
		return
	}

	// Build analyzer based on case sensitivity
	var analyzer *fulltext.Analyzer
	if req.CaseSensitive {
		analyzer = fulltext.NewAnalyzer(fulltext.WithCaseSensitive())
	} else {
		analyzer = fulltext.StandardAnalyzer()
	}

	// Handle regex-only search (no text terms)
	if req.Query == "" && regexPattern != "" {
		candidates := idx.RegexSearch(regexPattern)
		if len(candidates) == 0 {
			writeSuccess(w, map[string]interface{}{
				"results": []interface{}{},
				"count":   0,
				"query":   map[string]interface{}{"$regex": regexPattern},
			})
			return
		}
		// Score: use simple term frequency as score for regex
		results := make([]fulltext.ScoredResult, 0, len(candidates))
		for docID, score := range candidates {
			results = append(results, fulltext.ScoredResult{DocID: docID, Score: score})
		}
		// Sort by score descending
		sortSearchResults(results)

		// Apply offset (pagination)
		if req.Offset > 0 && req.Offset < len(results) {
			results = results[req.Offset:]
		} else if req.Offset >= len(results) {
			results = nil
		}

		// Apply limit
		if len(results) > req.Limit {
			results = results[:req.Limit]
		}

		searchResults := make([]map[string]interface{}, 0, len(results))
		for _, res := range results {
			doc, err := col.Get(res.DocID)
			if err != nil {
				continue
			}
			searchResults = append(searchResults, map[string]interface{}{
				"document": doc,
				"score":    res.Score,
			})
		}
		writeSuccess(w, map[string]interface{}{
			"results": searchResults,
			"count":   len(searchResults),
			"query":   map[string]interface{}{"$regex": regexPattern},
		})
		return
	}

	terms := analyzer.Analyze(req.Query)
	if len(terms) == 0 {
		writeSuccess(w, map[string]interface{}{
			"results": []interface{}{},
			"count":   0,
			"query":   req.Query,
		})
		return
	}

	// Get candidate docs based on search type
	var candidates map[string]float64
	if req.Phrase {
		candidates = idx.PhraseSearch(terms)
	} else if req.Fuzzy {
		maxDist := req.MaxFuzzyDist
		if maxDist <= 0 {
			maxDist = 2
		}
		candidates = idx.FuzzySearch(terms, maxDist)
	} else {
		candidates = idx.SearchTerms(terms)
	}

	// If regex pattern provided, intersect with term search results
	if regexPattern != "" {
		regexCandidates := idx.RegexSearch(regexPattern)
		// Intersect: keep only docs in both sets
		intersected := make(map[string]float64)
		for docID, score := range candidates {
			if regexScore, ok := regexCandidates[docID]; ok {
				intersected[docID] = score + regexScore
			}
		}
		candidates = intersected
	}

	// Score with BM25
	scorer := fulltext.NewBM25Scorer()
	results := scorer.ScoreDocuments(candidates, idx, terms)

	// Apply offset (pagination)
	if req.Offset > 0 && req.Offset < len(results) {
		results = results[req.Offset:]
	} else if req.Offset >= len(results) {
		// Offset beyond results
		results = nil
	}

	// Apply limit
	if len(results) > req.Limit {
		results = results[:req.Limit]
	}

	// Fetch documents and build response
	searchResults := make([]map[string]interface{}, 0, len(results))
	for _, res := range results {
		doc, err := col.Get(res.DocID)
		if err != nil {
			continue
		}
		// Apply field filter if specified
		if len(req.Fields) > 0 && !docMatchesFields(doc, terms, req.Fields) {
			continue
		}
		searchResults = append(searchResults, map[string]interface{}{
			"document": doc,
			"score":    res.Score,
		})
	}

	writeSuccess(w, map[string]interface{}{
		"results": searchResults,
		"count":   len(searchResults),
		"query":   req.Query,
	})
}

// docMatchesFields checks if any of the query terms appear in the specified fields of the document.
func docMatchesFields(doc *document.Document, terms []string, fields []string) bool {
	if doc == nil || len(terms) == 0 || len(fields) == 0 {
		return true // no filter
	}
	for _, field := range fields {
		val, exists := doc.Data[field]
		if !exists {
			continue
		}
		var text string
		switch v := val.(type) {
		case string:
			text = v
		case []interface{}:
			for _, elem := range v {
				if s, ok := elem.(string); ok {
					text += " " + s
				}
			}
		default:
			continue
		}
		// Normalize text to lowercase for matching (case-insensitive check)
		textLower := strings.ToLower(text)
		for _, term := range terms {
			if strings.Contains(textLower, strings.ToLower(term)) {
				return true
			}
		}
	}
	return false
}

// sortSearchResults sorts search results by score descending (simple insertion sort for small arrays)
func sortSearchResults(results []fulltext.ScoredResult) {
	for i := 1; i < len(results); i++ {
		j := i
		for j > 0 && results[j].Score > results[j-1].Score {
			results[j], results[j-1] = results[j-1], results[j]
			j--
		}
	}
}

// CreateFullTextIndexRequest represents a request to create a full-text index.
type CreateFullTextIndexRequest struct {
	Fields []string `json:"fields"`
}

// CreateFullTextIndex creates a full-text index on specified fields.
func (h *Handler) CreateFullTextIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var req CreateFullTextIndexRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Fields) == 0 {
		writeError(w, http.StatusBadRequest, "fields array is required")
		return
	}

	if err := col.CreateFullTextIndex(req.Fields); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create full-text index: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":    "full-text index created successfully",
		"collection": collectionName,
		"fields":     req.Fields,
	})
}

// GetFullTextIndex returns information about the full-text index on a collection.
func (h *Handler) GetFullTextIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	idx := col.GetFullTextIndex()
	fields := col.GetFullTextFields()

	if idx == nil || len(fields) == 0 {
		writeSuccess(w, map[string]interface{}{
			"collection": collectionName,
			"exists":     false,
			"fields":     []string{},
			"termCount":  0,
			"docCount":   0,
		})
		return
	}

	writeSuccess(w, map[string]interface{}{
		"collection": collectionName,
		"exists":     true,
		"fields":     fields,
		"termCount":  len(idx.Terms()),
		"docCount":   idx.TotalDocs(),
	})
}

// DeleteFullTextIndex removes the full-text index from a collection.
func (h *Handler) DeleteFullTextIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	idx := col.GetFullTextIndex()
	if idx == nil {
		writeError(w, http.StatusNotFound, "no full-text index exists on this collection")
		return
	}

	// Clear the fulltext index
	col.ClearFullTextIndex()

	writeSuccess(w, map[string]interface{}{
		"message":    "full-text index deleted successfully",
		"collection": collectionName,
	})
}

// RebuildFullTextIndex rebuilds the full-text index from all documents in the collection.
func (h *Handler) RebuildFullTextIndex(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	fields := col.GetFullTextFields()
	if len(fields) == 0 {
		writeError(w, http.StatusBadRequest, "no full-text index fields configured; create an index first")
		return
	}

	// Rebuild from all documents
	if err := col.RebuildFullTextIndex(); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to rebuild index: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"message":    "full-text index rebuilt successfully",
		"collection": collectionName,
		"fields":     fields,
		"docCount":   col.GetFullTextIndex().TotalDocs(),
	})
}
