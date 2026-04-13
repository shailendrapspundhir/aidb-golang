package collection

import (
	"aidb/internal/document"
	"aidb/internal/storage"
	"testing"
)

// TestUnifiedDocument tests unified document operations
func TestUnifiedDocument(t *testing.T) {
	doc := document.NewUnifiedDocument()
	
	// Test scalar fields
	doc.SetScalar("name", "test product")
	doc.SetScalar("price", 99.99)
	doc.SetScalar("active", true)
	
	name, ok := doc.GetString("name")
	if !ok || name != "test product" {
		t.Errorf("Expected name 'test product', got '%s'", name)
	}
	
	price, ok := doc.GetNumber("price")
	if !ok || price != 99.99 {
		t.Errorf("Expected price 99.99, got %f", price)
	}
	
	active, ok := doc.GetBool("active")
	if !ok || !active {
		t.Errorf("Expected active true, got %v", active)
	}
	
	// Test vector field
	vector := []float32{0.1, 0.2, 0.3, 0.4}
	doc.SetVector("embedding", vector)
	
	vec, ok := doc.GetVector("embedding")
	if !ok || len(vec) != 4 {
		t.Errorf("Expected vector length 4, got %d", len(vec))
	}
	
	// Test array field
	doc.SetArray("tags", []interface{}{"tag1", "tag2"})
	
	// Test object field
	doc.SetObject("metadata", map[string]interface{}{"key": "value"})
	
	// Test ToMap
	m := doc.ToMap()
	if m["name"] != "test product" {
		t.Errorf("ToMap failed for name")
	}
}

// TestUnifiedSchema tests unified schema validation
func TestUnifiedSchema(t *testing.T) {
	schema := document.NewUnifiedSchema("test", false)
	
	// Add fields
	schema.AddField("name", &document.UnifiedFieldSchema{
		Type:     document.FieldTypeString,
		Required: true,
	})
	
	schema.AddField("price", &document.UnifiedFieldSchema{
		Type:     document.FieldTypeNumber,
		Required: true,
	})
	
	schema.AddField("embedding", &document.UnifiedFieldSchema{
		Type:           document.FieldTypeVector,
		Dimensions:     4,
		DistanceMetric: "cosine",
	})
	
	schema.AddField("content", &document.UnifiedFieldSchema{
		Type:     document.FieldTypeFullText,
		Analyzer: "standard",
	})
	
	// Test valid document
	validDoc := document.NewUnifiedDocument()
	validDoc.SetScalar("name", "test")
	validDoc.SetScalar("price", 99.99)
	validDoc.SetVector("embedding", []float32{0.1, 0.2, 0.3, 0.4})
	validDoc.SetScalar("content", "test content")
	
	if err := schema.Validate(validDoc); err != nil {
		t.Errorf("Valid document failed validation: %v", err)
	}
	
	// Test invalid document (missing required)
	invalidDoc := document.NewUnifiedDocument()
	invalidDoc.SetScalar("name", "test")
	
	if err := schema.Validate(invalidDoc); err == nil {
		t.Errorf("Invalid document should have failed validation")
	}
	
	// Test vector dimension mismatch
	wrongDimDoc := document.NewUnifiedDocument()
	wrongDimDoc.SetScalar("name", "test")
	wrongDimDoc.SetScalar("price", 99.99)
	wrongDimDoc.SetVector("embedding", []float32{0.1, 0.2}) // Wrong dimensions
	
	if err := schema.Validate(wrongDimDoc); err == nil {
		t.Errorf("Wrong dimension vector should have failed validation")
	}
	
	// Test GetVectorFields
	vectorFields := schema.GetVectorFields()
	if len(vectorFields) != 1 {
		t.Errorf("Expected 1 vector field, got %d", len(vectorFields))
	}
	
	// Test GetFullTextFields
	textFields := schema.GetFullTextFields()
	if len(textFields) != 1 {
		t.Errorf("Expected 1 text field, got %d", len(textFields))
	}
}

// TestUnifiedCollection tests unified collection operations
func TestUnifiedCollection(t *testing.T) {
	// Create schema
	schema := document.NewUnifiedSchema("test", false)
	schema.AddField("name", &document.UnifiedFieldSchema{
		Type:     document.FieldTypeString,
		Required: true,
	})
	schema.AddField("embedding", &document.UnifiedFieldSchema{
		Type:           document.FieldTypeVector,
		Dimensions:     4,
		DistanceMetric: "cosine",
	})
	schema.AddField("content", &document.UnifiedFieldSchema{
		Type:     document.FieldTypeFullText,
		Analyzer: "standard",
	})
	
	// Create collection
	col, err := NewUnifiedCollection(UnifiedCollectionConfig{
		Name:    "test",
		Schema:  schema,
		Storage: storage.NewMemoryStorage(),
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	
	// Insert documents
	doc1 := document.NewUnifiedDocument()
	doc1.SetScalar("name", "Product A")
	doc1.SetScalar("content", "High quality wireless headphones")
	doc1.SetVector("embedding", []float32{0.1, 0.2, 0.3, 0.4})
	
	if err := col.Insert(doc1); err != nil {
		t.Errorf("Failed to insert doc1: %v", err)
	}
	
	doc2 := document.NewUnifiedDocument()
	doc2.SetScalar("name", "Product B")
	doc2.SetScalar("content", "Portable bluetooth speaker")
	doc2.SetVector("embedding", []float32{0.15, 0.25, 0.35, 0.45})
	
	if err := col.Insert(doc2); err != nil {
		t.Errorf("Failed to insert doc2: %v", err)
	}
	
	doc3 := document.NewUnifiedDocument()
	doc3.SetScalar("name", "Product C")
	doc3.SetScalar("content", "Running shoes for athletes")
	doc3.SetVector("embedding", []float32{0.5, 0.6, 0.7, 0.8})
	
	if err := col.Insert(doc3); err != nil {
		t.Errorf("Failed to insert doc3: %v", err)
	}
	
	// Test count
	if col.Count() != 3 {
		t.Errorf("Expected count 3, got %d", col.Count())
	}
	
	// Test get
	retrieved, err := col.Get(doc1.ID)
	if err != nil {
		t.Errorf("Failed to get document: %v", err)
	}
	if name, _ := retrieved.GetString("name"); name != "Product A" {
		t.Errorf("Expected name 'Product A', got '%s'", name)
	}
	
	// Test vector search
	results, err := col.VectorSearch("embedding", []float32{0.1, 0.2, 0.3, 0.4}, 3, 0.0)
	if err != nil {
		t.Errorf("Vector search failed: %v", err)
	}
	if len(results) == 0 {
		t.Errorf("Vector search returned no results")
	}
	
	// Test text search
	textResults, err := col.TextSearch("content", "headphones", 5)
	if err != nil {
		t.Errorf("Text search failed: %v", err)
	}
	if len(textResults) == 0 {
		t.Errorf("Text search returned no results")
	}
	
	// Test update
	doc1.SetScalar("name", "Updated Product A")
	if err := col.Update(doc1); err != nil {
		t.Errorf("Failed to update document: %v", err)
	}
	
	updated, err := col.Get(doc1.ID)
	if err != nil {
		t.Errorf("Failed to get updated document: %v", err)
	}
	if name, _ := updated.GetString("name"); name != "Updated Product A" {
		t.Errorf("Expected updated name, got '%s'", name)
	}
	
	// Test delete
	if err := col.Delete(doc3.ID); err != nil {
		t.Errorf("Failed to delete document: %v", err)
	}
	
	if col.Count() != 2 {
		t.Errorf("Expected count 2 after delete, got %d", col.Count())
	}
}

// TestUnifiedQuery tests unified query execution
func TestUnifiedQuery(t *testing.T) {
	// Create schema
	schema := document.NewUnifiedSchema("test", false)
	schema.AddField("name", &document.UnifiedFieldSchema{Type: document.FieldTypeString})
	schema.AddField("category", &document.UnifiedFieldSchema{Type: document.FieldTypeString, Index: true})
	schema.AddField("embedding", &document.UnifiedFieldSchema{
		Type:           document.FieldTypeVector,
		Dimensions:     4,
		DistanceMetric: "cosine",
	})
	schema.AddField("content", &document.UnifiedFieldSchema{
		Type:     document.FieldTypeFullText,
		Analyzer: "standard",
	})
	
	// Create collection
	col, err := NewUnifiedCollection(UnifiedCollectionConfig{
		Name:    "test",
		Schema:  schema,
		Storage: storage.NewMemoryStorage(),
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	
	// Insert test documents
	docs := []*document.UnifiedDocument{
		createTestDoc("Product A", "electronics", "wireless headphones", []float32{0.1, 0.2, 0.3, 0.4}),
		createTestDoc("Product B", "electronics", "bluetooth speaker", []float32{0.15, 0.25, 0.35, 0.45}),
		createTestDoc("Product C", "sports", "running shoes", []float32{0.5, 0.6, 0.7, 0.8}),
	}
	
	for _, doc := range docs {
		if err := col.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}
	
	// Test filter-only query
	filterResult, err := col.Query(&UnifiedQuery{
		Filter: map[string]interface{}{"category": "electronics"},
		Limit:  10,
	})
	if err != nil {
		t.Errorf("Filter query failed: %v", err)
	}
	if filterResult.Total != 2 {
		t.Errorf("Expected 2 filter results, got %d", filterResult.Total)
	}
	
	// Test vector-only query
	vectorResult, err := col.Query(&UnifiedQuery{
		Vector:      []float32{0.1, 0.2, 0.3, 0.4},
		VectorField: "embedding",
		TopK:        3,
		Limit:       10,
	})
	if err != nil {
		t.Errorf("Vector query failed: %v", err)
	}
	if vectorResult.Total == 0 {
		t.Errorf("Vector query returned no results")
	}
	
	// Test hybrid query (filter + vector)
	hybridResult, err := col.Query(&UnifiedQuery{
		Filter:      map[string]interface{}{"category": "electronics"},
		Vector:      []float32{0.1, 0.2, 0.3, 0.4},
		VectorField: "embedding",
		TopK:        5,
		Limit:       10,
	})
	if err != nil {
		t.Errorf("Hybrid query failed: %v", err)
	}
	if hybridResult.Total == 0 {
		t.Errorf("Hybrid query returned no results")
	}
}

// TestUnifiedManager tests unified manager operations
func TestUnifiedManager(t *testing.T) {
	manager := NewUnifiedManager()
	
	// Create collection
	schema := document.NewUnifiedSchema("test", false)
	schema.AddField("name", &document.UnifiedFieldSchema{Type: document.FieldTypeString})
	
	_, err := manager.CreateCollection("test", schema)
	if err != nil {
		t.Errorf("Failed to create collection: %v", err)
	}
	
	// Get collection
	retrieved, err := manager.GetCollection("test")
	if err != nil {
		t.Errorf("Failed to get collection: %v", err)
	}
	if retrieved.Name != "test" {
		t.Errorf("Expected name 'test', got '%s'", retrieved.Name)
	}
	
	// List collections
	list := manager.ListCollections()
	if len(list) != 1 {
		t.Errorf("Expected 1 collection, got %d", len(list))
	}
	
	// Check exists
	if !manager.CollectionExists("test") {
		t.Errorf("Collection should exist")
	}
	
	// Drop collection
	if err := manager.DropCollection("test"); err != nil {
		t.Errorf("Failed to drop collection: %v", err)
	}
	
	if manager.CollectionExists("test") {
		t.Errorf("Collection should not exist after drop")
	}
}

// TestVectorColumnStore tests vector column store operations
func TestVectorColumnStore(t *testing.T) {
	store, err := storage.NewVectorColumnStore(storage.VectorColumnConfig{
		Dimensions:     4,
		DistanceMetric: "cosine",
	})
	if err != nil {
		t.Fatalf("Failed to create vector store: %v", err)
	}
	
	// Insert vectors
	vectors := map[string][]float32{
		"doc1": {0.1, 0.2, 0.3, 0.4},
		"doc2": {0.15, 0.25, 0.35, 0.45},
		"doc3": {0.5, 0.6, 0.7, 0.8},
	}
	
	for id, vec := range vectors {
		if err := store.Insert(id, vec); err != nil {
			t.Errorf("Failed to insert vector %s: %v", id, err)
		}
	}
	
	// Test count
	if store.Count() != 3 {
		t.Errorf("Expected count 3, got %d", store.Count())
	}
	
	// Test get
	vec, err := store.Get("doc1")
	if err != nil {
		t.Errorf("Failed to get vector: %v", err)
	}
	if len(vec) != 4 {
		t.Errorf("Expected vector length 4, got %d", len(vec))
	}
	
	// Test search
	results, err := store.Search([]float32{0.1, 0.2, 0.3, 0.4}, 3, 0.0)
	if err != nil {
		t.Errorf("Search failed: %v", err)
	}
	if len(results) == 0 {
		t.Errorf("Search returned no results")
	}
	
	// Test update
	if err := store.Update("doc1", []float32{0.2, 0.3, 0.4, 0.5}); err != nil {
		t.Errorf("Failed to update vector: %v", err)
	}
	
	// Test delete
	if err := store.Delete("doc3"); err != nil {
		t.Errorf("Failed to delete vector: %v", err)
	}
	
	if store.Count() != 2 {
		t.Errorf("Expected count 2 after delete, got %d", store.Count())
	}
}

// TestFullTextColumnStore tests full-text column store operations
func TestFullTextColumnStore(t *testing.T) {
	store := storage.NewFullTextColumnStore(storage.FullTextColumnConfig{
		Analyzer: "standard",
	})
	
	// Insert texts
	texts := map[string]string{
		"doc1": "High quality wireless headphones with noise cancellation",
		"doc2": "Portable bluetooth speaker with deep bass",
		"doc3": "Running shoes for athletes",
	}
	
	for id, text := range texts {
		if err := store.Insert(id, text); err != nil {
			t.Errorf("Failed to insert text %s: %v", id, err)
		}
	}
	
	// Test count
	if store.Count() != 3 {
		t.Errorf("Expected count 3, got %d", store.Count())
	}
	
	// Test search
	results, err := store.Search("headphones", 5)
	if err != nil {
		t.Errorf("Search failed: %v", err)
	}
	if len(results) == 0 {
		t.Errorf("Search returned no results")
	}
	
	// Test delete
	if err := store.Delete("doc3"); err != nil {
		t.Errorf("Failed to delete text: %v", err)
	}
	
	if store.Count() != 2 {
		t.Errorf("Expected count 2 after delete, got %d", store.Count())
	}
}

// Helper function
func createTestDoc(name, category, content string, embedding []float32) *document.UnifiedDocument {
	doc := document.NewUnifiedDocument()
	doc.SetScalar("name", name)
	doc.SetScalar("category", category)
	doc.SetScalar("content", content)
	doc.SetVector("embedding", embedding)
	return doc
}
