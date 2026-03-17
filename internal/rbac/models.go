package rbac

import "time"

// Hierarchy Models

type Tenant struct {
	ID        string    `json:"_id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type Region struct {
	ID        string    `json:"_id"`
	Name      string    `json:"name"` // e.g., "us-east-1"
	TenantID  string    `json:"tenantId"`
	CreatedAt time.Time `json:"createdAt"`
}

type Environment struct {
	ID        string    `json:"_id"`
	Name      string    `json:"name"` // e.g., "production", "staging"
	RegionID  string    `json:"regionId"`
	TenantID  string    `json:"tenantId"`
	CreatedAt time.Time `json:"createdAt"`
}

// Access Control Models

type Role struct {
	ID          string    `json:"_id"`
	Name        string    `json:"name"`        // e.g., "admin", "reader"
	TenantID    string    `json:"tenantId"`    // Roles are scoped to a tenant
	Description string    `json:"description"`
	Policies    []Policy  `json:"policies"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Effect string

const (
	EffectAllow Effect = "allow"
	EffectDeny  Effect = "deny"
)

type Action string

const (
	ActionRead   Action = "read"
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
	ActionManage Action = "manage" // For schema changes, etc.
	ActionAll    Action = "*"
)

type Policy struct {
	Effect    Effect   `json:"effect"`
	Actions   []Action `json:"actions"`
	// Resources is a list of resource paths/patterns
	// Format: "tenant/{tid}/region/{rid}/env/{eid}/collection/{cname}"
	// Wildcards allowed: "tenant/{tid}/region/*/env/prod/collection/users"
	Resources []string `json:"resources"`
	
	// FieldLevel restriction (optional). If empty, implies all fields.
	// If set, applies to Read/Create/Update actions.
	Fields    []string `json:"fields,omitempty"` 
}

// Default System Roles
const (
	RoleSuperAdmin = "super_admin" // Can do everything everywhere
	RoleTenantAdmin = "tenant_admin" // Can do everything in their tenant
)
