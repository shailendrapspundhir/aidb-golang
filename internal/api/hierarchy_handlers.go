package api

import (
	"aidb/internal/auth"
	"aidb/internal/document"
	"aidb/internal/rbac"
	"net/http"
)

// Tenants

type CreateTenantRequest struct {
	Name string `json:"name"`
}

func (h *Handler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	col, _ := h.collectionManager.GetCollection(rbac.TenantsCollection)
	
	// Check if exists
	existing, _ := col.Find(map[string]interface{}{"name": req.Name})
	if len(existing) > 0 {
		writeError(w, http.StatusBadRequest, "tenant already exists")
		return
	}

	docData := map[string]interface{}{
		"name": req.Name,
	}
	doc := document.NewDocument(docData)
	
	if err := col.Insert(doc); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeSuccess(w, doc)
}

func (h *Handler) ListTenants(w http.ResponseWriter, r *http.Request) {
	col, _ := h.collectionManager.GetCollection(rbac.TenantsCollection)
	docs, err := col.FindAll()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeSuccess(w, docs)
}

func (h *Handler) DeleteTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	col, _ := h.collectionManager.GetCollection(rbac.TenantsCollection)
	if err := col.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeSuccess(w, map[string]string{"message": "tenant deleted"})
}

// Regions

type CreateRegionRequest struct {
	Name string `json:"name"`
}

func (h *Handler) CreateRegion(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(UserContextKey).(*auth.User)
	
	var req CreateRegionRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	col, _ := h.collectionManager.GetCollection(rbac.RegionsCollection)
	
	// Check if exists in this tenant
	existing, _ := col.Find(map[string]interface{}{"name": req.Name, "tenantId": user.TenantID})
	if len(existing) > 0 {
		writeError(w, http.StatusBadRequest, "region already exists")
		return
	}

	docData := map[string]interface{}{
		"name":     req.Name,
		"tenantId": user.TenantID,
	}
	doc := document.NewDocument(docData)
	
	if err := col.Insert(doc); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeSuccess(w, doc)
}

func (h *Handler) ListRegions(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(UserContextKey).(*auth.User)
	col, _ := h.collectionManager.GetCollection(rbac.RegionsCollection)
	
	// Filter by tenant
	docs, err := col.Find(map[string]interface{}{"tenantId": user.TenantID})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeSuccess(w, docs)
}

func (h *Handler) DeleteRegion(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	col, _ := h.collectionManager.GetCollection(rbac.RegionsCollection)
	// TODO: Verify tenant ownership
	if err := col.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeSuccess(w, map[string]string{"message": "region deleted"})
}

// Environments

type CreateEnvironmentRequest struct {
	Name     string `json:"name"`
	RegionID string `json:"regionId"`
}

func (h *Handler) CreateEnvironment(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(UserContextKey).(*auth.User)
	
	var req CreateEnvironmentRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	col, _ := h.collectionManager.GetCollection(rbac.EnvironmentsCollection)
	
	// Check if exists
	existing, _ := col.Find(map[string]interface{}{
		"name": req.Name, 
		"regionId": req.RegionID,
		"tenantId": user.TenantID,
	})
	if len(existing) > 0 {
		writeError(w, http.StatusBadRequest, "environment already exists")
		return
	}

	docData := map[string]interface{}{
		"name":     req.Name,
		"regionId": req.RegionID,
		"tenantId": user.TenantID,
	}
	doc := document.NewDocument(docData)
	
	if err := col.Insert(doc); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeSuccess(w, doc)
}

func (h *Handler) ListEnvironments(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(UserContextKey).(*auth.User)
	col, _ := h.collectionManager.GetCollection(rbac.EnvironmentsCollection)
	
	// Filter by tenant
	docs, err := col.Find(map[string]interface{}{"tenantId": user.TenantID})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeSuccess(w, docs)
}

func (h *Handler) DeleteEnvironment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	col, _ := h.collectionManager.GetCollection(rbac.EnvironmentsCollection)
	// TODO: Verify tenant ownership
	if err := col.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeSuccess(w, map[string]string{"message": "environment deleted"})
}
