package vector

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// DistanceMetric defines the type of distance metric for vector similarity
type DistanceMetric string

const (
	DistanceCosine   DistanceMetric = "cosine"
	DistanceEuclidean DistanceMetric = "euclidean"
	DistanceDotProduct DistanceMetric = "dot"
)

// VectorDocument represents a document with a vector embedding
type VectorDocument struct {
	ID          string                 `json:"_id"`
	Vector      []float32              `json:"vector"`
	Dimensions  int                    `json:"dimensions"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt   time.Time              `json:"_createdAt"`
	UpdatedAt   time.Time              `json:"_updatedAt"`
}

// NewVectorDocument creates a new vector document with auto-generated ID
func NewVectorDocument(vector []float32, metadata map[string]interface{}) *VectorDocument {
	now := time.Now().UTC()
	return &VectorDocument{
		ID:         uuid.New().String(),
		Vector:     vector,
		Dimensions: len(vector),
		Metadata:   metadata,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// NewVectorDocumentWithID creates a new vector document with a specified ID
func NewVectorDocumentWithID(id string, vector []float32, metadata map[string]interface{}) *VectorDocument {
	now := time.Now().UTC()
	return &VectorDocument{
		ID:         id,
		Vector:     vector,
		Dimensions: len(vector),
		Metadata:   metadata,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// Update updates the vector and metadata
func (d *VectorDocument) Update(vector []float32, metadata map[string]interface{}) {
	d.Vector = vector
	d.Dimensions = len(vector)
	d.Metadata = metadata
	d.UpdatedAt = time.Now().UTC()
}

// UpdateVector updates only the vector
func (d *VectorDocument) UpdateVector(vector []float32) {
	d.Vector = vector
	d.Dimensions = len(vector)
	d.UpdatedAt = time.Now().UTC()
}

// UpdateMetadata updates only the metadata
func (d *VectorDocument) UpdateMetadata(metadata map[string]interface{}) {
	if d.Metadata == nil {
		d.Metadata = make(map[string]interface{})
	}
	for k, v := range metadata {
		d.Metadata[k] = v
	}
	d.UpdatedAt = time.Now().UTC()
}

// ToJSON converts the vector document to JSON bytes
func (d *VectorDocument) ToJSON() ([]byte, error) {
	return json.Marshal(d)
}

// FromJSON creates a vector document from JSON bytes
func FromJSON(data []byte) (*VectorDocument, error) {
	var doc VectorDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// VectorConfig defines the configuration for a vector collection
type VectorConfig struct {
	Dimensions        int            `json:"dimensions"`
	DistanceMetric    DistanceMetric `json:"distanceMetric"`
	CreatedAt         time.Time      `json:"createdAt"`
	UpdatedAt         time.Time      `json:"updatedAt"`
	
	// HNSW index parameters
	HNSW_M              int `json:"hnswM,omitempty"`              // Max connections per layer
	HNSW_MMax           int `json:"hnswMMax,omitempty"`           // Max connections at layer 0
	HNSW_EfConstruction int `json:"hnswEfConstruction,omitempty"` // Construction candidate list size
	HNSW_EfSearch       int `json:"hnswEfSearch,omitempty"`       // Search candidate list size
}

// NewVectorConfig creates a new vector configuration
func NewVectorConfig(dimensions int, metric DistanceMetric) *VectorConfig {
	now := time.Now().UTC()
	return &VectorConfig{
		Dimensions:     dimensions,
		DistanceMetric: metric,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// NewVectorConfigWithHNSW creates a new vector configuration with HNSW parameters
func NewVectorConfigWithHNSW(dimensions int, metric DistanceMetric, m, mMax, efConstruction, efSearch int) *VectorConfig {
	now := time.Now().UTC()
	return &VectorConfig{
		Dimensions:          dimensions,
		DistanceMetric:      metric,
		CreatedAt:           now,
		UpdatedAt:           now,
		HNSW_M:              m,
		HNSW_MMax:           mMax,
		HNSW_EfConstruction: efConstruction,
		HNSW_EfSearch:       efSearch,
	}
}

// SearchResult represents a vector search result with similarity score
type SearchResult struct {
	Document *VectorDocument `json:"document"`
	Score    float32         `json:"score"`
}

// SearchRequest represents a vector search request
type SearchRequest struct {
	Vector      []float32              `json:"vector"`
	TopK        int                    `json:"topK"`
	MinScore    float32                `json:"minScore,omitempty"`
	Filter      map[string]interface{} `json:"filter,omitempty"`
	IncludeVector bool                  `json:"includeVector,omitempty"`
}

// SearchResponse represents a vector search response
type SearchResponse struct {
	Results []*SearchResult `json:"results"`
	Count   int             `json:"count"`
}

// Vector errors
var (
	ErrVectorNotFound      = errors.New("vector document not found")
	ErrVectorExists        = errors.New("vector document with this ID already exists")
	ErrInvalidVector       = errors.New("invalid vector: must be non-empty")
	ErrDimensionMismatch   = errors.New("vector dimension mismatch")
	ErrInvalidDimensions   = errors.New("dimensions must be positive")
	ErrInvalidDistanceMetric = errors.New("invalid distance metric")
	ErrDocumentExists      = errors.New("document already exists")
	ErrDocumentNotFound    = errors.New("document not found")
)
