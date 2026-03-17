package api

import (
	"aidb/internal/auth"
	"aidb/internal/collection"
	"aidb/internal/document"
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
