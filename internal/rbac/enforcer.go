package rbac

import (
	"aidb/internal/collection"
	"aidb/internal/document"
	"errors"
	"strings"
)

const (
	RolesCollection        = "_roles"
	TenantsCollection      = "_tenants"
	RegionsCollection      = "_regions"
	EnvironmentsCollection = "_environments"
)

type Enforcer struct {
	cm *collection.Manager
}

func NewEnforcer(cm *collection.Manager) *Enforcer {
	ensureSystemCollections(cm)
	return &Enforcer{cm: cm}
}

func ensureSystemCollections(cm *collection.Manager) {
	if !cm.CollectionExists(RolesCollection) {
		schema := document.NewSchema(RolesCollection, true)
		schema.AddField("name", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("tenantId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("description", &document.FieldSchema{Type: document.FieldTypeString})
		schema.AddField("policies", &document.FieldSchema{Type: document.FieldTypeArray})
		cm.CreateCollection(RolesCollection, schema)
	}

	if !cm.CollectionExists(TenantsCollection) {
		schema := document.NewSchema(TenantsCollection, true)
		schema.AddField("name", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		cm.CreateCollection(TenantsCollection, schema)
	}

	if !cm.CollectionExists(RegionsCollection) {
		schema := document.NewSchema(RegionsCollection, true)
		schema.AddField("name", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("tenantId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		cm.CreateCollection(RegionsCollection, schema)
	}

	if !cm.CollectionExists(EnvironmentsCollection) {
		schema := document.NewSchema(EnvironmentsCollection, true)
		schema.AddField("name", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("regionId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("tenantId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		cm.CreateCollection(EnvironmentsCollection, schema)
	}
}

// RequestContext defines the context for an access request
type RequestContext struct {
	TenantID     string
	RegionID     string // Optional
	EnvID        string // Optional
	Collection   string // Optional
	Action       Action
	TargetFields []string // Fields being accessed/modified
}

// Enforce checks if the user has permission for the request
func (e *Enforcer) Enforce(userRoles []string, userTenantID string, ctx RequestContext) (bool, error) {
	// 1. Super Admin Bypass (if we had a flag for it, or a specific role name)
	for _, roleID := range userRoles {
		if roleID == RoleSuperAdmin {
			return true, nil
		}
	}

	// 2. Tenant Check
	// Users can only access their own tenant, unless they are super admin (handled above)
	if ctx.TenantID != "" && ctx.TenantID != userTenantID {
		return false, nil
	}

	// 3. Fetch Roles
	roles, err := e.fetchRoles(userRoles)
	if err != nil {
		return false, err
	}

	allowed := false

	// 4. Evaluate Policies
	for _, role := range roles {
		// Ensure role belongs to the user's tenant (security check)
		if role.TenantID != userTenantID && role.Name != RoleSuperAdmin {
			continue
		}

		for _, policy := range role.Policies {
			if e.evaluatePolicy(policy, ctx) {
				if policy.Effect == EffectDeny {
					return false, nil // Explicit Deny overrides Allow
				}
				allowed = true
			}
		}
	}

	return allowed, nil
}

// GetAllowedFields returns the list of allowed fields for a request
func (e *Enforcer) GetAllowedFields(userRoles []string, userTenantID string, ctx RequestContext) ([]string, error) {
	// 1. Super Admin
	for _, roleID := range userRoles {
		if roleID == RoleSuperAdmin {
			return []string{"*"}, nil
		}
	}

	// 2. Tenant Check
	if ctx.TenantID != "" && ctx.TenantID != userTenantID {
		return nil, nil
	}

	roles, err := e.fetchRoles(userRoles)
	if err != nil {
		return nil, err
	}

	allowedFields := make(map[string]bool)
	allFieldsAllowed := false

	for _, role := range roles {
		if role.TenantID != userTenantID && role.Name != RoleSuperAdmin {
			continue
		}

		for _, policy := range role.Policies {
			if e.evaluatePolicyResourceAndAction(policy, ctx) {
				if policy.Effect == EffectDeny {
					return nil, nil // Explicit Deny
				}
				
				if len(policy.Fields) == 0 {
					allFieldsAllowed = true
				} else {
					for _, f := range policy.Fields {
						if f == "*" {
							allFieldsAllowed = true
						} else {
							allowedFields[f] = true
						}
					}
				}
			}
		}
	}

	if allFieldsAllowed {
		return []string{"*"}, nil
	}

	var fields []string
	for f := range allowedFields {
		fields = append(fields, f)
	}
	return fields, nil
}

func (e *Enforcer) evaluatePolicyResourceAndAction(policy Policy, ctx RequestContext) bool {
	// 1. Check Action
	actionMatch := false
	for _, a := range policy.Actions {
		if a == ActionAll || a == ctx.Action {
			actionMatch = true
			break
		}
	}
	if !actionMatch {
		return false
	}

	// 2. Check Resource Path
	rid := ctx.RegionID
	if rid == "" { rid = "*" }
	eid := ctx.EnvID
	if eid == "" { eid = "*" }
	cname := ctx.Collection
	if cname == "" { cname = "*" }
	
	requestPath := "tenant/" + ctx.TenantID + "/region/" + rid + "/env/" + eid + "/collection/" + cname
	
	for _, pattern := range policy.Resources {
		if match(pattern, requestPath) {
			return true
		}
	}
	return false
}

func (e *Enforcer) fetchRoles(roleIDs []string) ([]Role, error) {
	col, _ := e.cm.GetCollection(RolesCollection)
	var roles []Role

	for _, id := range roleIDs {
		// In a real DB, we'd use an "IN" query. Here we loop.
		// If ID is a name (like "super_admin"), we might search by name.
		// For now assume ID is the document ID or Name.
		
		// Try finding by ID
		doc, err := col.Get(id)
		if err == nil {
			roles = append(roles, e.docToRole(doc))
			continue
		}
		
		// Try finding by Name
		docs, err := col.Find(map[string]interface{}{"name": id})
		if err == nil && len(docs) > 0 {
			roles = append(roles, e.docToRole(docs[0]))
		}
	}
	return roles, nil
}

func (e *Enforcer) docToRole(doc *document.Document) Role {
	role := Role{
		ID:          doc.ID,
		Name:        doc.Data["name"].(string),
		TenantID:    doc.Data["tenantId"].(string),
		Description: getString(doc.Data, "description"),
		CreatedAt:   doc.CreatedAt,
	}
	
	// Parse policies
	if policiesData, ok := doc.Data["policies"].([]interface{}); ok {
		for _, pData := range policiesData {
			if pMap, ok := pData.(map[string]interface{}); ok {
				policy := Policy{
					Effect: Effect(getString(pMap, "effect")),
				}
				
				// Actions
				if actions, ok := pMap["actions"].([]interface{}); ok {
					for _, a := range actions {
						policy.Actions = append(policy.Actions, Action(a.(string)))
					}
				}
				
				// Resources
				if resources, ok := pMap["resources"].([]interface{}); ok {
					for _, r := range resources {
						policy.Resources = append(policy.Resources, r.(string))
					}
				}
				
				// Fields
				if fields, ok := pMap["fields"].([]interface{}); ok {
					for _, f := range fields {
						policy.Fields = append(policy.Fields, f.(string))
					}
				}
				
				role.Policies = append(role.Policies, policy)
			}
		}
	}
	
	return role
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (e *Enforcer) evaluatePolicy(policy Policy, ctx RequestContext) bool {
	if !e.evaluatePolicyResourceAndAction(policy, ctx) {
		return false
	}

	// 3. Check Fields (if specified in request)
	if len(ctx.TargetFields) > 0 && len(policy.Fields) > 0 {
		// If policy has specific fields, ensure all target fields are allowed
		// If policy.Fields contains "*", it allows all
		hasWildcard := false
		for _, f := range policy.Fields {
			if f == "*" {
				hasWildcard = true
				break
			}
		}
		
		if !hasWildcard {
			for _, target := range ctx.TargetFields {
				allowed := false
				for _, allowedField := range policy.Fields {
					if allowedField == target {
						allowed = true
						break
					}
				}
				if !allowed {
					return false // One of the target fields is not allowed
				}
			}
		}
	}

	return true
}

// match checks if path matches pattern (simple wildcard support)
func match(pattern, path string) bool {
	// Split by '/'
	pParts := strings.Split(pattern, "/")
	pathParts := strings.Split(path, "/")
	
	if len(pParts) != len(pathParts) {
		return false
	}
	
	for i := 0; i < len(pParts); i++ {
		if pParts[i] != "*" && pParts[i] != pathParts[i] {
			return false
		}
	}
	
	return true
}

// Helper to create a new role
func (e *Enforcer) CreateRole(name, tenantID, description string, policies []Policy) error {
	col, _ := e.cm.GetCollection(RolesCollection)
	
	// Check if exists
	existing, _ := col.Find(map[string]interface{}{"name": name, "tenantId": tenantID})
	if len(existing) > 0 {
		return errors.New("role already exists")
	}
	
	// Convert policies to map for storage
	var policiesData []map[string]interface{}
	for _, p := range policies {
		pMap := map[string]interface{}{
			"effect":    string(p.Effect),
			"actions":   p.Actions,
			"resources": p.Resources,
			"fields":    p.Fields,
		}
		policiesData = append(policiesData, pMap)
	}
	
	docData := map[string]interface{}{
		"name":        name,
		"tenantId":    tenantID,
		"description": description,
		"policies":    policiesData,
	}
	
	doc := document.NewDocument(docData)
	return col.Insert(doc)
}
