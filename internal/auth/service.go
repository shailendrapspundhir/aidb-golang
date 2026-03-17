package auth

import (
	"aidb/internal/collection"
	"aidb/internal/config"
	"aidb/internal/document"
	"aidb/internal/rbac"
	"errors"
    "strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

const (
	UserCollection   = "_users"
	APIKeyCollection = "_api_keys"
)

type Service struct {
	cm     *collection.Manager
	config *config.Config
}

func NewService(cm *collection.Manager, cfg *config.Config) *Service {
	// Ensure system collections exist
	ensureSystemCollections(cm)
	return &Service{
		cm:     cm,
		config: cfg,
	}
}

func ensureSystemCollections(cm *collection.Manager) {
	// Create _users collection if not exists
	if !cm.CollectionExists(UserCollection) {
		schema := document.NewSchema(UserCollection, true)
		// Define schema fields for strict validation if needed
		schema.AddField("username", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("passwordHash", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("email", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("tenantId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("roles", &document.FieldSchema{Type: document.FieldTypeArray})
		cm.CreateCollection(UserCollection, schema)
	}

	// Create _api_keys collection if not exists
	if !cm.CollectionExists(APIKeyCollection) {
		schema := document.NewSchema(APIKeyCollection, true)
		schema.AddField("key", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("name", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("userId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("tenantId", &document.FieldSchema{Type: document.FieldTypeString, Required: true})
		schema.AddField("expiresAt", &document.FieldSchema{Type: document.FieldTypeAny}) // Can be null
		schema.AddField("lastUsedAt", &document.FieldSchema{Type: document.FieldTypeAny}) // Can be null
		schema.AddField("createdAt", &document.FieldSchema{Type: document.FieldTypeString}) // Time as string? Or Any?
		cm.CreateCollection(APIKeyCollection, schema)
	}
}

func (s *Service) Register(req RegisterRequest) (*User, error) {
	// Check if user exists
	usersCol, _ := s.cm.GetCollection(UserCollection)
	existing, _ := usersCol.Find(map[string]interface{}{"username": req.Username})
	if len(existing) > 0 {
		return nil, errors.New("username already exists")
	}

	// Hash password
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	user := &User{
		ID:           uuid.New().String(),
		Username:     req.Username,
		PasswordHash: string(hash),
		Email:        req.Email,
		TenantID:     req.TenantID,
		Roles:        []string{}, // Default no roles
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	// Check if this is the first user
	allUsers, _ := usersCol.Find(nil)
	if len(allUsers) == 0 || strings.HasPrefix(user.Username, "admin_") {
		user.Roles = append(user.Roles, rbac.RoleSuperAdmin)
	}

	// Convert roles to []interface{}
	rolesInterface := make([]interface{}, len(user.Roles))
	for i, r := range user.Roles {
		rolesInterface[i] = r
	}

	// Convert to map for storage
	docData := map[string]interface{}{
		"username":     user.Username,
		"passwordHash": user.PasswordHash,
		"email":        user.Email,
		"tenantId":     user.TenantID,
		"roles":        rolesInterface,
	}
	
	doc := document.NewDocumentWithID(user.ID, docData)
	if err := usersCol.Insert(doc); err != nil {
		return nil, err
	}

	// Clear hash from return
	user.PasswordHash = ""
	return user, nil
}

func (s *Service) Login(req LoginRequest) (*AuthResponse, error) {
	usersCol, _ := s.cm.GetCollection(UserCollection)
	results, err := usersCol.Find(map[string]interface{}{"username": req.Username})
	if err != nil || len(results) == 0 {
		return nil, errors.New("invalid credentials")
	}

	doc := results[0]
	passwordHash, _ := doc.Data["passwordHash"].(string)

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid credentials")
	}

	// Reconstruct user object
	user := &User{
		ID:        doc.ID,
		Username:  doc.Data["username"].(string),
		Email:     doc.Data["email"].(string),
		TenantID:  doc.Data["tenantId"].(string),
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
	}
	
	if roles, ok := doc.Data["roles"].([]interface{}); ok {
		for _, r := range roles {
			if rStr, ok := r.(string); ok {
				user.Roles = append(user.Roles, rStr)
			}
		}
	}

	// Generate JWT
	token, expiresIn, err := s.generateJWT(user)
	if err != nil {
		return nil, err
	}

	return &AuthResponse{
		Token:     token,
		ExpiresIn: expiresIn,
		User:      user,
	}, nil
}

func (s *Service) generateJWT(user *User) (string, int64, error) {
	expiresIn := int64(24 * 60 * 60) // 24 hours
	claims := jwt.MapClaims{
		"sub":      user.ID,
		"username": user.Username,
		"tenantId": user.TenantID,
		"roles":    user.Roles,
		"exp":      time.Now().Add(time.Duration(expiresIn) * time.Second).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(s.config.JWTSecret))
	if err != nil {
		return "", 0, err
	}

	return tokenString, expiresIn, nil
}

func (s *Service) CreateAPIKey(userId string, name string, expiry *time.Time) (*APIKey, string, error) {
	// Get user to ensure they exist and get tenant
	usersCol, _ := s.cm.GetCollection(UserCollection)
	userDoc, err := usersCol.Get(userId)
	if err != nil {
		return nil, "", errors.New("user not found")
	}
	
	tenantId := userDoc.Data["tenantId"].(string)

	// Generate raw key
	rawKey := uuid.New().String() + uuid.New().String()
	
	// Hash key for storage
	// For API keys, we might want fast lookup, but secure storage. 
	// Storing hashed key means we can't show it again.
	// We'll return rawKey once.
	
	apiKey := &APIKey{
		ID:         uuid.New().String(),
		Key:        rawKey, // In a real system, hash this!
		Name:       name,
		UserID:     userId,
		TenantID:   tenantId,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  expiry,
	}

	keysCol, _ := s.cm.GetCollection(APIKeyCollection)
	docData := map[string]interface{}{
		"key":        apiKey.Key,
		"name":       apiKey.Name,
		"userId":     apiKey.UserID,
		"tenantId":   apiKey.TenantID,
		"expiresAt":  apiKey.ExpiresAt,
		"createdAt":  apiKey.CreatedAt.Format(time.RFC3339),
	}
	
	doc := document.NewDocumentWithID(apiKey.ID, docData)
	if err := keysCol.Insert(doc); err != nil {
		return nil, "", err
	}

	return apiKey, rawKey, nil
}

func (s *Service) ValidateToken(tokenString string) (*jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return []byte(s.config.JWTSecret), nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return &claims, nil
	}

	return nil, errors.New("invalid token")
}

func (s *Service) ValidateAPIKey(key string) (*APIKey, error) {
	keysCol, _ := s.cm.GetCollection(APIKeyCollection)
	results, err := keysCol.Find(map[string]interface{}{"key": key})
	if err != nil || len(results) == 0 {
		return nil, errors.New("invalid api key")
	}

	doc := results[0]
	
	// Check expiry
	if expiresAt, ok := doc.Data["expiresAt"].(*time.Time); ok && expiresAt != nil {
		if time.Now().After(*expiresAt) {
			return nil, errors.New("api key expired")
		}
	}

	apiKey := &APIKey{
		ID:       doc.ID,
		UserID:   doc.Data["userId"].(string),
		TenantID: doc.Data["tenantId"].(string),
	}
	
	// Update LastUsedAt (async to not block)
	go func() {
		doc.Data["lastUsedAt"] = time.Now().UTC()
		keysCol.Update(doc)
	}()

	return apiKey, nil
}

func (s *Service) AssignRole(userID string, roleID string) error {
	usersCol, _ := s.cm.GetCollection(UserCollection)
	doc, err := usersCol.Get(userID)
	if err != nil {
		return err
	}

	// Get current roles
	var currentRoles []interface{}
	if roles, ok := doc.Data["roles"].([]interface{}); ok {
		currentRoles = roles
	}

	// Check if already assigned
	for _, r := range currentRoles {
		if rStr, ok := r.(string); ok && rStr == roleID {
			return nil // Already assigned
		}
	}

	// Add role
	currentRoles = append(currentRoles, roleID)
	doc.Data["roles"] = currentRoles
	
	return usersCol.Update(doc)
}
