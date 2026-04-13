package document

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Extended field types for unified schema (beyond base FieldType)
const (
	FieldTypeVector    FieldType = "vector"    // Dense vector for similarity search
	FieldTypeFullText  FieldType = "fulltext"  // Text field indexed for full-text search
	FieldTypeEmbedding FieldType = "embedding" // Auto-generated embedding from another field
)

// FieldValue represents a value that can be of any supported type
type FieldValue struct {
	// Scalar value (string, number, bool, nil)
	Scalar interface{} `json:"scalar,omitempty"`

	// Vector value (for vector/embedding fields)
	Vector []float32 `json:"vector,omitempty"`

	// Array value
	Array []interface{} `json:"array,omitempty"`

	// Object value (nested document)
	Object map[string]interface{} `json:"object,omitempty"`
}

// IsZero returns true if the field value is empty
func (fv *FieldValue) IsZero() bool {
	if fv == nil {
		return true
	}
	return fv.Scalar == nil && len(fv.Vector) == 0 && len(fv.Array) == 0 && len(fv.Object) == 0
}

// AsInterface returns the field value as a generic interface{}
func (fv *FieldValue) AsInterface() interface{} {
	if fv == nil {
		return nil
	}
	if fv.Vector != nil {
		return fv.Vector
	}
	if fv.Array != nil {
		return fv.Array
	}
	if fv.Object != nil {
		return fv.Object
	}
	return fv.Scalar
}

// UnifiedDocument represents a document that can contain any field type
type UnifiedDocument struct {
	ID        string                 `json:"_id"`
	CreatedAt time.Time              `json:"_createdAt"`
	UpdatedAt time.Time              `json:"_updatedAt"`
	Fields    map[string]*FieldValue `json:"fields"`
}

// NewUnifiedDocument creates a new unified document with auto-generated ID
func NewUnifiedDocument() *UnifiedDocument {
	now := time.Now().UTC()
	return &UnifiedDocument{
		ID:        uuid.New().String(),
		CreatedAt: now,
		UpdatedAt: now,
		Fields:    make(map[string]*FieldValue),
	}
}

// NewUnifiedDocumentWithID creates a new unified document with a specified ID
func NewUnifiedDocumentWithID(id string) *UnifiedDocument {
	now := time.Now().UTC()
	return &UnifiedDocument{
		ID:        id,
		CreatedAt: now,
		UpdatedAt: now,
		Fields:    make(map[string]*FieldValue),
	}
}

// SetScalar sets a scalar field value
func (d *UnifiedDocument) SetScalar(name string, value interface{}) {
	d.Fields[name] = &FieldValue{Scalar: value}
	d.UpdatedAt = time.Now().UTC()
}

// SetVector sets a vector field value
func (d *UnifiedDocument) SetVector(name string, vector []float32) {
	d.Fields[name] = &FieldValue{Vector: vector}
	d.UpdatedAt = time.Now().UTC()
}

// SetArray sets an array field value
func (d *UnifiedDocument) SetArray(name string, array []interface{}) {
	d.Fields[name] = &FieldValue{Array: array}
	d.UpdatedAt = time.Now().UTC()
}

// SetObject sets an object field value
func (d *UnifiedDocument) SetObject(name string, obj map[string]interface{}) {
	d.Fields[name] = &FieldValue{Object: obj}
	d.UpdatedAt = time.Now().UTC()
}

// Get gets a field value
func (d *UnifiedDocument) Get(name string) *FieldValue {
	return d.Fields[name]
}

// GetScalar gets a scalar field value
func (d *UnifiedDocument) GetScalar(name string) (interface{}, bool) {
	fv, ok := d.Fields[name]
	if !ok || fv == nil {
		return nil, false
	}
	return fv.Scalar, true
}

// GetVector gets a vector field value
func (d *UnifiedDocument) GetVector(name string) ([]float32, bool) {
	fv, ok := d.Fields[name]
	if !ok || fv == nil {
		return nil, false
	}
	return fv.Vector, len(fv.Vector) > 0
}

// GetString gets a string field value
func (d *UnifiedDocument) GetString(name string) (string, bool) {
	v, ok := d.GetScalar(name)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// GetNumber gets a number field value as float64
func (d *UnifiedDocument) GetNumber(name string) (float64, bool) {
	v, ok := d.GetScalar(name)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	default:
		return 0, false
	}
}

// GetBool gets a boolean field value
func (d *UnifiedDocument) GetBool(name string) (bool, bool) {
	v, ok := d.GetScalar(name)
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

// Delete removes a field
func (d *UnifiedDocument) Delete(name string) {
	delete(d.Fields, name)
	d.UpdatedAt = time.Now().UTC()
}

// Merge merges fields from another document
func (d *UnifiedDocument) Merge(other *UnifiedDocument) {
	for k, v := range other.Fields {
		d.Fields[k] = v
	}
	d.UpdatedAt = time.Now().UTC()
}

// ToJSON converts the document to JSON bytes
func (d *UnifiedDocument) ToJSON() ([]byte, error) {
	return json.Marshal(d)
}

// FromUnifiedJSON creates a unified document from JSON bytes
func FromUnifiedJSON(data []byte) (*UnifiedDocument, error) {
	var doc UnifiedDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// ToMap converts the document to a map for backward compatibility
func (d *UnifiedDocument) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range d.Fields {
		if v != nil {
			result[k] = v.AsInterface()
		}
	}
	return result
}

// FromMap creates field values from a map
func (d *UnifiedDocument) FromMap(data map[string]interface{}) {
	for k, v := range data {
		switch val := v.(type) {
		case []float32:
			d.SetVector(k, val)
		case []interface{}:
			// Check if it's actually a float32 array
			if isFloat32Array(val) {
				d.SetVector(k, toFloat32Array(val))
			} else {
				d.SetArray(k, val)
			}
		case map[string]interface{}:
			d.SetObject(k, val)
		default:
			d.SetScalar(k, v)
		}
	}
}

// Helper functions
func isFloat32Array(arr []interface{}) bool {
	if len(arr) == 0 {
		return false
	}
	for _, v := range arr {
		switch v.(type) {
		case float32, float64, int, int32, int64:
			continue
		default:
			return false
		}
	}
	return true
}

func toFloat32Array(arr []interface{}) []float32 {
	result := make([]float32, len(arr))
	for i, v := range arr {
		switch n := v.(type) {
		case float32:
			result[i] = n
		case float64:
			result[i] = float32(n)
		case int:
			result[i] = float32(n)
		case int32:
			result[i] = float32(n)
		case int64:
			result[i] = float32(n)
		}
	}
	return result
}

// ConvertFromLegacyDocument converts a legacy Document to UnifiedDocument
func ConvertFromLegacyDocument(doc *Document) *UnifiedDocument {
	unified := NewUnifiedDocumentWithID(doc.ID)
	unified.CreatedAt = doc.CreatedAt
	unified.UpdatedAt = doc.UpdatedAt
	unified.FromMap(doc.Data)
	return unified
}

// ConvertToLegacyDocument converts a UnifiedDocument to legacy Document
func (d *UnifiedDocument) ConvertToLegacyDocument() *Document {
	return &Document{
		ID:        d.ID,
		CreatedAt: d.CreatedAt,
		UpdatedAt: d.UpdatedAt,
		Data:      d.ToMap(),
	}
}

// UnifiedFieldSchema defines the schema for a single field in unified schema
type UnifiedFieldSchema struct {
	Type        FieldType               `json:"type"`
	Required    bool                    `json:"required,omitempty"`
	Default     interface{}             `json:"default,omitempty"`
	Description string                  `json:"description,omitempty"`
	Properties  map[string]*UnifiedFieldSchema `json:"properties,omitempty"` // For object types
	Items       *UnifiedFieldSchema     `json:"items,omitempty"`      // For array types
	MinLength   *int                    `json:"minLength,omitempty"`  // For string types
	MaxLength   *int                    `json:"maxLength,omitempty"`
	Minimum     *float64                `json:"minimum,omitempty"`    // For number types
	Maximum     *float64                `json:"maximum,omitempty"`
	Enum        []interface{}           `json:"enum,omitempty"`
	
	// Vector-specific options
	Dimensions     int    `json:"dimensions,omitempty"`     // For vector/embedding fields
	DistanceMetric string `json:"distanceMetric,omitempty"` // "cosine", "euclidean", "dot"
	
	// Full-text specific options
	Analyzer string `json:"analyzer,omitempty"` // "standard", "simple", "whitespace"
	
	// Embedding-specific options
	EmbeddingModel string `json:"embeddingModel,omitempty"` // Model to use for auto-embedding
	SourceField    string `json:"sourceField,omitempty"`    // Field to generate embedding from
	
	// Index options
	Index     bool   `json:"index,omitempty"`     // Create scalar index
	IndexType string `json:"indexType,omitempty"` // "btree", "hash"
}

// UnifiedSchema defines the structure for a unified collection
type UnifiedSchema struct {
	Name      string                        `json:"name"`
	Strict    bool                          `json:"strict"`
	Fields    map[string]*UnifiedFieldSchema `json:"fields"`
	CreatedAt time.Time                     `json:"createdAt"`
	UpdatedAt time.Time                     `json:"updatedAt"`
}

// NewUnifiedSchema creates a new unified schema
func NewUnifiedSchema(name string, strict bool) *UnifiedSchema {
	now := time.Now().UTC()
	return &UnifiedSchema{
		Name:      name,
		Strict:    strict,
		Fields:    make(map[string]*UnifiedFieldSchema),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// AddField adds a field to the schema
func (s *UnifiedSchema) AddField(name string, field *UnifiedFieldSchema) {
	s.Fields[name] = field
	s.UpdatedAt = time.Now().UTC()
}

// GetVectorFields returns all vector/embedding fields
func (s *UnifiedSchema) GetVectorFields() map[string]*UnifiedFieldSchema {
	result := make(map[string]*UnifiedFieldSchema)
	for name, field := range s.Fields {
		if field.Type == FieldTypeVector || field.Type == FieldTypeEmbedding {
			result[name] = field
		}
	}
	return result
}

// GetFullTextFields returns all fulltext fields
func (s *UnifiedSchema) GetFullTextFields() map[string]*UnifiedFieldSchema {
	result := make(map[string]*UnifiedFieldSchema)
	for name, field := range s.Fields {
		if field.Type == FieldTypeFullText {
			result[name] = field
		}
	}
	return result
}

// GetIndexedFields returns all indexed fields (scalar indexes)
func (s *UnifiedSchema) GetIndexedFields() map[string]*UnifiedFieldSchema {
	result := make(map[string]*UnifiedFieldSchema)
	for name, field := range s.Fields {
		if field.Index || field.IndexType != "" {
			result[name] = field
		}
	}
	return result
}

// Validate validates a unified document against the schema
func (s *UnifiedSchema) Validate(doc *UnifiedDocument) error {
	if s == nil || s.Fields == nil {
		return nil // No schema means schemaless, accept anything
	}

	// Check required fields
	for fieldName, fieldSchema := range s.Fields {
		if fieldSchema.Required {
			value, exists := doc.Fields[fieldName]
			if !exists || value == nil || value.IsZero() {
				return NewValidationError(fieldName, "is required")
			}
		}
	}

	// Validate field types
	for fieldName, value := range doc.Fields {
		fieldSchema, hasSchema := s.Fields[fieldName]

		// In strict mode, reject unknown fields
		if s.Strict && !hasSchema {
			return NewValidationError(fieldName, "is not defined in schema")
		}

		if hasSchema {
			if err := validateUnifiedFieldValue(fieldName, value, fieldSchema); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateUnifiedFieldValue validates a single field value against its schema
func validateUnifiedFieldValue(fieldName string, value *FieldValue, schema *UnifiedFieldSchema) error {
	if value == nil || value.IsZero() {
		if schema.Required {
			return NewValidationError(fieldName, "is required")
		}
		return nil
	}

	switch schema.Type {
	case FieldTypeString:
		if _, ok := value.Scalar.(string); !ok {
			return NewValidationError(fieldName, "must be a string")
		}
	case FieldTypeNumber:
		if !isNumber(value.Scalar) {
			return NewValidationError(fieldName, "must be a number")
		}
	case FieldTypeInteger:
		if !isInteger(value.Scalar) {
			return NewValidationError(fieldName, "must be an integer")
		}
	case FieldTypeBoolean:
		if _, ok := value.Scalar.(bool); !ok {
			return NewValidationError(fieldName, "must be a boolean")
		}
	case FieldTypeArray:
		if len(value.Array) == 0 && value.Scalar == nil {
			return NewValidationError(fieldName, "must be an array")
		}
	case FieldTypeObject:
		if len(value.Object) == 0 && value.Scalar == nil {
			return NewValidationError(fieldName, "must be an object")
		}
	case FieldTypeVector, FieldTypeEmbedding:
		if len(value.Vector) == 0 {
			return NewValidationError(fieldName, "must be a non-empty vector")
		}
		if schema.Dimensions > 0 && len(value.Vector) != schema.Dimensions {
			return NewValidationError(fieldName, "vector dimension mismatch")
		}
	case FieldTypeFullText:
		if _, ok := value.Scalar.(string); !ok {
			return NewValidationError(fieldName, "must be a string for full-text indexing")
		}
	case FieldTypeAny:
		// Accept any type
	default:
		return errors.New("unknown field type: " + string(schema.Type))
	}

	// Validate enum values
	if len(schema.Enum) > 0 {
		found := false
		for _, enumValue := range schema.Enum {
			if value.Scalar == enumValue {
				found = true
				break
			}
		}
		if !found {
			return NewValidationError(fieldName, "must be one of the enum values")
		}
	}

	return nil
}
