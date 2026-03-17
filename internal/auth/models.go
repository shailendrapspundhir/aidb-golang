package auth

import (
	"time"
)

type User struct {
	ID           string    `json:"_id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"passwordHash,omitempty"` // Omitted from JSON output usually
	Email        string    `json:"email"`
	Roles        []string  `json:"roles"` // IDs of roles assigned to the user
	TenantID     string    `json:"tenantId"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type APIKey struct {
	ID          string    `json:"_id"`
	Key         string    `json:"key"` // Hashed version stored
	Name        string    `json:"name"`
	UserID      string    `json:"userId"`
	TenantID    string    `json:"tenantId"`
	Roles       []string  `json:"roles"` // Optional: Specific roles for this key
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	LastUsedAt  *time.Time `json:"lastUsedAt,omitempty"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Email    string `json:"email"`
	TenantID string `json:"tenantId"`
}

type AuthResponse struct {
	Token     string `json:"token"`
	ExpiresIn int64  `json:"expiresIn"`
	User      *User  `json:"user"`
}
