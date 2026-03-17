package document

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// Document represents a JSON document stored in the database
type Document struct {
	ID        string                 `json:"_id"`
	CreatedAt time.Time              `json:"_createdAt"`
	UpdatedAt time.Time              `json:"_updatedAt"`
	Data      map[string]interface{} `json:"data"`
}

// NewDocument creates a new document with auto-generated ID
func NewDocument(data map[string]interface{}) *Document {
	now := time.Now().UTC()
	return &Document{
		ID:        uuid.New().String(),
		CreatedAt: now,
		UpdatedAt: now,
		Data:      data,
	}
}

// NewDocumentWithID creates a new document with a specified ID
func NewDocumentWithID(id string, data map[string]interface{}) *Document {
	now := time.Now().UTC()
	return &Document{
		ID:        id,
		CreatedAt: now,
		UpdatedAt: now,
		Data:      data,
	}
}

// ToJSON converts the document to JSON bytes
func (d *Document) ToJSON() ([]byte, error) {
	return json.Marshal(d)
}

// FromJSON creates a document from JSON bytes
func FromJSON(data []byte) (*Document, error) {
	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// Update modifies the document data and updates the timestamp
func (d *Document) Update(data map[string]interface{}) {
	d.Data = data
	d.UpdatedAt = time.Now().UTC()
}

// MergeData merges the provided data with existing document data
func (d *Document) MergeData(data map[string]interface{}) {
	if d.Data == nil {
		d.Data = make(map[string]interface{})
	}
	for k, v := range data {
		d.Data[k] = v
	}
	d.UpdatedAt = time.Now().UTC()
}

// FieldType defines the type of a schema field
type FieldType string

const (
	FieldTypeString    FieldType = "string"
	FieldTypeNumber    FieldType = "number"
	FieldTypeInteger   FieldType = "integer"
	FieldTypeBoolean    FieldType = "boolean"
	FieldTypeArray     FieldType = "array"
	FieldTypeObject    FieldType = "object"
	FieldTypeNull      FieldType = "null"
	FieldTypeAny       FieldType = "any"
)

// FieldSchema defines the schema for a single field
type FieldSchema struct {
	Type        FieldType               `json:"type"`
	Required    bool                    `json:"required,omitempty"`
	Default     interface{}             `json:"default,omitempty"`
	Description string                  `json:"description,omitempty"`
	Properties  map[string]*FieldSchema `json:"properties,omitempty"` // For object types
	Items       *FieldSchema            `json:"items,omitempty"`      // For array types
	MinLength   *int                    `json:"minLength,omitempty"`   // For string types
	MaxLength   *int                    `json:"maxLength,omitempty"`   // For string types
	Minimum     *float64                `json:"minimum,omitempty"`     // For number types
	Maximum     *float64                `json:"maximum,omitempty"`     // For number types
	Enum        []interface{}           `json:"enum,omitempty"`        // Allowed values
}

// Schema defines the structure for a collection
type Schema struct {
	Name       string                  `json:"name"`
	Strict     bool                    `json:"strict"` // If true, reject unknown fields
	Fields     map[string]*FieldSchema `json:"fields"`
	CreatedAt  time.Time               `json:"createdAt"`
	UpdatedAt  time.Time               `json:"updatedAt"`
}

// NewSchema creates a new schema
func NewSchema(name string, strict bool) *Schema {
	now := time.Now().UTC()
	return &Schema{
		Name:      name,
		Strict:    strict,
		Fields:    make(map[string]*FieldSchema),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// AddField adds a field to the schema
func (s *Schema) AddField(name string, field *FieldSchema) {
	s.Fields[name] = field
	s.UpdatedAt = time.Now().UTC()
}

// Validate validates a document against the schema
func (s *Schema) Validate(doc *Document) error {
	if s == nil || s.Fields == nil {
		return nil // No schema means schemaless, accept anything
	}

	// Check required fields
	for fieldName, fieldSchema := range s.Fields {
		if fieldSchema.Required {
			value, exists := doc.Data[fieldName]
			if !exists || value == nil {
				return NewValidationError(fieldName, "is required")
			}
		}
	}

	// Validate field types
	for fieldName, value := range doc.Data {
		fieldSchema, hasSchema := s.Fields[fieldName]
		
		// In strict mode, reject unknown fields
		if s.Strict && !hasSchema {
			return NewValidationError(fieldName, "is not defined in schema")
		}

		if hasSchema {
			if err := validateFieldValue(fieldName, value, fieldSchema); err != nil {
				return err
			}
		}
	}

	return nil
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}

func (e *ValidationError) Error() string {
	return e.Field + " " + e.Message
}

// validateFieldValue validates a single field value against its schema
func validateFieldValue(fieldName string, value interface{}, schema *FieldSchema) error {
	if value == nil {
		if schema.Required {
			return NewValidationError(fieldName, "is required")
		}
		return nil
	}

	switch schema.Type {
	case FieldTypeString:
		if _, ok := value.(string); !ok {
			return NewValidationError(fieldName, "must be a string")
		}
	case FieldTypeNumber:
		if !isNumber(value) {
			return NewValidationError(fieldName, "must be a number")
		}
	case FieldTypeInteger:
		if !isInteger(value) {
			return NewValidationError(fieldName, "must be an integer")
		}
	case FieldTypeBoolean:
		if _, ok := value.(bool); !ok {
			return NewValidationError(fieldName, "must be a boolean")
		}
	case FieldTypeArray:
		if !isArray(value) {
			return NewValidationError(fieldName, "must be an array")
		}
	case FieldTypeObject:
		if !isObject(value) {
			return NewValidationError(fieldName, "must be an object")
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
			if value == enumValue {
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

// Helper functions for type checking
func isNumber(value interface{}) bool {
	switch value.(type) {
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func isInteger(value interface{}) bool {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float64:
		return v == float64(int64(v))
	case float32:
		return v == float32(int32(v))
	default:
		return false
	}
}

func isArray(value interface{}) bool {
	if value == nil {
		return false
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

func isObject(value interface{}) bool {
	_, ok := value.(map[string]interface{})
	return ok
}