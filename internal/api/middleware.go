package api

import (
	"aidb/internal/auth"
	"aidb/internal/rbac"
	"context"
	"fmt"
	"net/http"
	"strings"
)

type contextKey string

const (
	UserContextKey contextKey = "user"
)

func (h *Handler) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for public endpoints
		if r.URL.Path == "/api/v1/login" || 
		   r.URL.Path == "/api/v1/register" || 
		   r.URL.Path == "/api/v1/health" ||
		   r.URL.Path == "/" {
			next.ServeHTTP(w, r)
			return
		}

		var user *auth.User

		// Check Authorization header
		authHeader := r.Header.Get("Authorization")
		apiKeyHeader := r.Header.Get("X-API-Key")

		if authHeader != "" {
			// Bearer Token
			parts := strings.Split(authHeader, " ")
			if len(parts) == 2 && parts[0] == "Bearer" {
				claims, err := h.authService.ValidateToken(parts[1])
				if err == nil {
					// Convert claims to User struct (partial)
					user = &auth.User{
						ID:       (*claims)["sub"].(string),
						Username: (*claims)["username"].(string),
						TenantID: (*claims)["tenantId"].(string),
					}
					// Parse roles
					if roles, ok := (*claims)["roles"].([]interface{}); ok {
						for _, r := range roles {
							if rStr, ok := r.(string); ok {
								user.Roles = append(user.Roles, rStr)
							}
						}
					}
				}
			}
		} else if apiKeyHeader != "" {
			// API Key
			apiKey, err := h.authService.ValidateAPIKey(apiKeyHeader)
			if err == nil {
				// We need to fetch the full user to get roles? 
				// Or the API key has roles associated with it? 
				// For now, let's assume API key maps to a user.
				// In a real system, we'd fetch the user.
				// Here we construct a minimal user.
				user = &auth.User{
					ID:       apiKey.UserID,
					TenantID: apiKey.TenantID,
					// We might need to fetch roles from DB if not in API Key
				}
				// TODO: Fetch user roles
			}
		}

		if user == nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		// Add user to context
		ctx := context.WithValue(r.Context(), UserContextKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *Handler) RBACMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := r.Context().Value(UserContextKey).(*auth.User)
		if !ok {
			// Should be caught by AuthMiddleware, but just in case
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		// Determine Action
		var action rbac.Action
		switch r.Method {
		case http.MethodGet:
			action = rbac.ActionRead
		case http.MethodPost:
			action = rbac.ActionCreate
		case http.MethodPut, http.MethodPatch:
			action = rbac.ActionUpdate
		case http.MethodDelete:
			action = rbac.ActionDelete
		default:
			action = rbac.ActionRead
		}

		// Determine Resource
		// Keep it simple: /api/v1/collections/{name} -> collection/{name}
		pathParts := strings.Split(r.URL.Path, "/")
		
		collectionName := "*"
		regionID := "*"
		envID := "*"
		targetTenantID := user.TenantID

		if len(pathParts) >= 4 {
			switch pathParts[3] {
			case "collections":
				if len(pathParts) >= 5 {
					collectionName = pathParts[4]
				}
			case "regions":
				if len(pathParts) >= 5 {
					regionID = pathParts[4]
				}
			case "environments":
				if len(pathParts) >= 5 {
					envID = pathParts[4]
				}
			case "tenants":
				// For tenants endpoint, we are operating ON tenants.
				// If listing/creating, we might be operating on "root".
				if len(pathParts) >= 5 {
					targetTenantID = pathParts[4]
				}
			}
		}

		// Construct Context
		rbacCtx := rbac.RequestContext{
			TenantID:   targetTenantID,
			RegionID:   regionID,
			EnvID:      envID,
			Collection: collectionName,
			Action:     action,
		}

		// Check Permission
		allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
			return
		}

		if !allowed {
			fmt.Printf("Access denied: User=%s Roles=%v Tenant=%s Resource=%s Action=%s\n", user.Username, user.Roles, user.TenantID, rbacCtx.Collection, rbacCtx.Action)
			writeError(w, http.StatusForbidden, "access denied")
			return
		}

		next.ServeHTTP(w, r)
	})
}
