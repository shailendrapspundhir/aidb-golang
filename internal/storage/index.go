package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// IndexType defines the type of index
type IndexType string

const (
	IndexTypeBTree IndexType = "btree"  // B-tree for range queries
	IndexTypeHash  IndexType = "hash"   // Hash for exact match
	IndexTypeFull  IndexType = "full"   // Full-text index
)

// IndexStats holds statistics about an index
type IndexStats struct {
	Name        string    `json:"name"`
	Field       string    `json:"field"`
	Type        IndexType `json:"type"`
	EntryCount  int       `json:"entryCount"`
	Cardinality int       `json:"cardinality"` // unique keys
	// For BTree
	Height       int `json:"height,omitempty"`
	NodeCount    int `json:"nodeCount,omitempty"`
	// For Hash
	BucketCount  int `json:"bucketCount,omitempty"`
}

// Index defines the interface for indexes
type Index interface {
	// Insert adds a key-documentID pair to the index
	Insert(key interface{}, docID string) error
	
	// Delete removes a key-documentID pair from the index
	Delete(key interface{}, docID string) error
	
	// Find retrieves document IDs matching the key
	Find(key interface{}) ([]string, error)
	
	// FindRange retrieves document IDs within a range (for B-tree)
	FindRange(start, end interface{}) ([]string, error)
	
	// FindPrefix retrieves document IDs with keys matching a prefix (for strings)
	FindPrefix(prefix string) ([]string, error)
	
	// Count returns the number of entries in the index
	Count() int
	
	// Clear removes all entries from the index
	Clear()
	
	// Name returns the index name
	Name() string
	
	// Field returns the indexed field
	Field() string
	
	// Type returns the index type
	Type() IndexType
	
	// Stats returns index statistics
	Stats() IndexStats
}

// BTreeIndex is a B-tree based index for range queries
type BTreeIndex struct {
	name   string
	field  string
	mu     sync.RWMutex
	root   *btreeNode
	count  int
	order  int // B-tree order (max children per node)
}

// btreeNode represents a node in the B-tree
type btreeNode struct {
	keys     []interface{}
	values   [][]string // doc IDs for each key
	children []*btreeNode
	isLeaf   bool
}

// NewBTreeIndex creates a new B-tree index
func NewBTreeIndex(name, field string, order int) *BTreeIndex {
	if order < 3 {
		order = 3 // Minimum order for B-tree
	}
	return &BTreeIndex{
		name:  name,
		field: field,
		order: order,
		root: &btreeNode{
			keys:     make([]interface{}, 0),
			values:   make([][]string, 0),
			children: make([]*btreeNode, 0),
			isLeaf:   true,
		},
	}
}

// Insert adds a key-documentID pair to the B-tree
func (idx *BTreeIndex) Insert(key interface{}, docID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Convert key to comparable
	compKey, err := toComparable(key)
	if err != nil {
		return err
	}

	// Check if key exists
	if existingValues := idx.findInNode(idx.root, compKey); existingValues != nil {
		for _, id := range existingValues {
			if id == docID {
				return nil // Already exists
			}
		}
		// Add docID to existing key
		idx.addValueToKey(idx.root, compKey, docID)
		idx.count++
		return nil
	}

	// Insert new key
	idx.insertKey(idx.root, compKey, docID)
	idx.count++

	// Check if root needs to be split
	if len(idx.root.keys) >= idx.order {
		idx.splitRoot()
	}

	return nil
}

// Delete removes a key-documentID pair from the B-tree
func (idx *BTreeIndex) Delete(key interface{}, docID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	compKey, err := toComparable(key)
	if err != nil {
		return err
	}

	if idx.deleteFromNode(idx.root, compKey, docID) {
		idx.count--
	}

	return nil
}

// Find retrieves document IDs matching the key
func (idx *BTreeIndex) Find(key interface{}) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	compKey, err := toComparable(key)
	if err != nil {
		return nil, err
	}

	values := idx.findInNode(idx.root, compKey)
	if values == nil {
		return []string{}, nil
	}

	// Return a copy
	result := make([]string, len(values))
	copy(result, values)
	return result, nil
}

// FindRange retrieves document IDs within a range
func (idx *BTreeIndex) FindRange(start, end interface{}) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	startKey, err := toComparable(start)
	if err != nil {
		return nil, err
	}
	endKey, err := toComparable(end)
	if err != nil {
		return nil, err
	}

	var results []string
	idx.rangeSearch(idx.root, startKey, endKey, &results)
	return results, nil
}

// FindPrefix retrieves document IDs with keys matching a prefix
func (idx *BTreeIndex) FindPrefix(prefix string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []string
	idx.prefixSearch(idx.root, prefix, &results)
	return results, nil
}

// Count returns the number of entries
func (idx *BTreeIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.count
}

// Clear removes all entries
func (idx *BTreeIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.root = &btreeNode{
		keys:     make([]interface{}, 0),
		values:   make([][]string, 0),
		children: make([]*btreeNode, 0),
		isLeaf:   true,
	}
	idx.count = 0
}

// Name returns the index name
func (idx *BTreeIndex) Name() string { return idx.name }

// Field returns the indexed field
func (idx *BTreeIndex) Field() string { return idx.field }

// Type returns the index type
func (idx *BTreeIndex) Type() IndexType { return IndexTypeBTree }

// Stats returns index statistics for BTreeIndex
func (idx *BTreeIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Count unique keys (cardinality) and estimate height
	cardinality := 0
	height := 0
	nodeCount := 0

	var countNodes func(n *btreeNode, level int)
	countNodes = func(n *btreeNode, level int) {
		if n == nil {
			return
		}
		nodeCount++
		if level > height {
			height = level
		}
		cardinality += len(n.keys)
		for _, child := range n.children {
			countNodes(child, level+1)
		}
	}
	countNodes(idx.root, 1)

	return IndexStats{
		Name:        idx.name,
		Field:       idx.field,
		Type:        IndexTypeBTree,
		EntryCount:  idx.count,
		Cardinality: cardinality,
		Height:      height,
		NodeCount:   nodeCount,
	}
}

// Internal methods

func (idx *BTreeIndex) findInNode(node *btreeNode, key interface{}) []string {
	if node == nil {
		return nil
	}

	i := sort.Search(len(node.keys), func(i int) bool {
		return compareKeys(node.keys[i], key) >= 0
	})

	if i < len(node.keys) && compareKeys(node.keys[i], key) == 0 {
		return node.values[i]
	}

	if node.isLeaf {
		return nil
	}

	if i < len(node.children) {
		return idx.findInNode(node.children[i], key)
	}

	return nil
}

func (idx *BTreeIndex) addValueToKey(node *btreeNode, key interface{}, docID string) bool {
	i := sort.Search(len(node.keys), func(i int) bool {
		return compareKeys(node.keys[i], key) >= 0
	})

	if i < len(node.keys) && compareKeys(node.keys[i], key) == 0 {
		node.values[i] = append(node.values[i], docID)
		return true
	}

	if !node.isLeaf && i < len(node.children) {
		return idx.addValueToKey(node.children[i], key, docID)
	}

	return false
}

func (idx *BTreeIndex) insertKey(node *btreeNode, key interface{}, docID string) {
	i := sort.Search(len(node.keys), func(i int) bool {
		return compareKeys(node.keys[i], key) >= 0
	})

	if node.isLeaf {
		// Insert into leaf
		node.keys = append(node.keys[:i], append([]interface{}{key}, node.keys[i:]...)...)
		node.values = append(node.values[:i], append([][]string{{docID}}, node.values[i:]...)...)
		return
	}

	// Insert into child
	if i < len(node.children) {
		child := node.children[i]
		idx.insertKey(child, key, docID)

		// Split child if needed
		if len(child.keys) >= idx.order {
			idx.splitChild(node, i)
		}
	}
}

func (idx *BTreeIndex) splitChild(parent *btreeNode, index int) {
	child := parent.children[index]
	mid := len(child.keys) / 2

	// Create new node
	newNode := &btreeNode{
		keys:     make([]interface{}, len(child.keys)-mid-1),
		values:   make([][]string, len(child.keys)-mid-1),
		isLeaf:   child.isLeaf,
	}
	copy(newNode.keys, child.keys[mid+1:])
	copy(newNode.values, child.values[mid+1:])

	// Promote middle key to parent
	promotedKey := child.keys[mid]
	promotedValues := child.values[mid]

	// Trim original child
	child.keys = child.keys[:mid]
	child.values = child.values[:mid]

	// Update parent
	parent.keys = append(parent.keys[:index], append([]interface{}{promotedKey}, parent.keys[index:]...)...)
	parent.values = append(parent.values[:index], append([][]string{promotedValues}, parent.values[index:]...)...)

	if !child.isLeaf {
		newNode.children = make([]*btreeNode, len(child.children)-mid-1)
		copy(newNode.children, child.children[mid+1:])
		child.children = child.children[:mid+1]
	}

	parent.children = append(parent.children[:index+1], append([]*btreeNode{newNode}, parent.children[index+1:]...)...)
}

func (idx *BTreeIndex) splitRoot() {
	newRoot := &btreeNode{
		keys:     make([]interface{}, 0),
		values:   make([][]string, 0),
		children: []*btreeNode{idx.root},
		isLeaf:   false,
	}
	idx.splitChild(newRoot, 0)
	idx.root = newRoot
}

func (idx *BTreeIndex) deleteFromNode(node *btreeNode, key interface{}, docID string) bool {
	i := sort.Search(len(node.keys), func(i int) bool {
		return compareKeys(node.keys[i], key) >= 0
	})

	if i < len(node.keys) && compareKeys(node.keys[i], key) == 0 {
		// Found key, remove docID
		values := node.values[i]
		for j, id := range values {
			if id == docID {
				node.values[i] = append(values[:j], values[j+1:]...)
				if len(node.values[i]) == 0 {
					// Remove key entirely
					node.keys = append(node.keys[:i], node.keys[i+1:]...)
					node.values = append(node.values[:i], node.values[i+1:]...)
				}
				return true
			}
		}
		return false
	}

	if !node.isLeaf && i < len(node.children) {
		return idx.deleteFromNode(node.children[i], key, docID)
	}

	return false
}

func (idx *BTreeIndex) rangeSearch(node *btreeNode, start, end interface{}, results *[]string) {
	if node == nil {
		return
	}

	for i, key := range node.keys {
		// If key is within range, add values
		if compareKeys(key, start) >= 0 && compareKeys(key, end) <= 0 {
			*results = append(*results, node.values[i]...)
		}

		// Search children if not leaf
		if !node.isLeaf && i < len(node.children) {
			if compareKeys(key, start) > 0 {
				idx.rangeSearch(node.children[i], start, end, results)
			}
		}
	}

	// Search last child
	if !node.isLeaf && len(node.children) > len(node.keys) {
		idx.rangeSearch(node.children[len(node.keys)], start, end, results)
	}
}

func (idx *BTreeIndex) prefixSearch(node *btreeNode, prefix string, results *[]string) {
	if node == nil {
		return
	}

	for i, key := range node.keys {
		if keyStr, ok := key.(string); ok {
			if len(keyStr) >= len(prefix) && keyStr[:len(prefix)] == prefix {
				*results = append(*results, node.values[i]...)
			}
		}
	}

	if !node.isLeaf {
		for _, child := range node.children {
			idx.prefixSearch(child, prefix, results)
		}
	}
}

// HashIndex is a simple hash-based index for exact match queries
type HashIndex struct {
	name  string
	field string
	mu    sync.RWMutex
	data  map[string]map[string]struct{} // key -> set of doc IDs
	count int
}

// NewHashIndex creates a new hash index
func NewHashIndex(name, field string) *HashIndex {
	return &HashIndex{
		name:  name,
		field: field,
		data:  make(map[string]map[string]struct{}),
	}
}

// Insert adds a key-documentID pair
func (idx *HashIndex) Insert(key interface{}, docID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	keyStr, err := toKeyString(key)
	if err != nil {
		return err
	}

	if idx.data[keyStr] == nil {
		idx.data[keyStr] = make(map[string]struct{})
	}
	
	if _, exists := idx.data[keyStr][docID]; !exists {
		idx.data[keyStr][docID] = struct{}{}
		idx.count++
	}

	return nil
}

// Delete removes a key-documentID pair
func (idx *HashIndex) Delete(key interface{}, docID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	keyStr, err := toKeyString(key)
	if err != nil {
		return err
	}

	if ids, exists := idx.data[keyStr]; exists {
		if _, found := ids[docID]; found {
			delete(ids, docID)
			idx.count--
			if len(ids) == 0 {
				delete(idx.data, keyStr)
			}
		}
	}

	return nil
}

// Find retrieves document IDs matching the key
func (idx *HashIndex) Find(key interface{}) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keyStr, err := toKeyString(key)
	if err != nil {
		return nil, err
	}

	ids := idx.data[keyStr]
	if ids == nil {
		return []string{}, nil
	}

	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	return result, nil
}

// FindRange is not supported for hash index
func (idx *HashIndex) FindRange(start, end interface{}) ([]string, error) {
	return nil, fmt.Errorf("range queries not supported for hash index")
}

// FindPrefix is not supported for hash index
func (idx *HashIndex) FindPrefix(prefix string) ([]string, error) {
	return nil, fmt.Errorf("prefix queries not supported for hash index")
}

// Count returns the number of entries
func (idx *HashIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.count
}

// Clear removes all entries
func (idx *HashIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.data = make(map[string]map[string]struct{})
	idx.count = 0
}

// Name returns the index name
func (idx *HashIndex) Name() string { return idx.name }

// Field returns the indexed field
func (idx *HashIndex) Field() string { return idx.field }

// Type returns the index type
func (idx *HashIndex) Type() IndexType { return IndexTypeHash }

// Stats returns index statistics for HashIndex
func (idx *HashIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	cardinality := len(idx.data)
	return IndexStats{
		Name:        idx.name,
		Field:       idx.field,
		Type:        IndexTypeHash,
		EntryCount:  idx.count,
		Cardinality: cardinality,
		BucketCount: cardinality,
	}
}

// Helper functions

func toComparable(key interface{}) (interface{}, error) {
	switch v := key.(type) {
	case string, int, int64, float64, bool:
		return v, nil
	case int32:
		return int64(v), nil
	case float32:
		return float64(v), nil
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, nil
		}
		return v.String(), nil
	default:
		return nil, fmt.Errorf("unsupported key type: %T", key)
	}
}

func toKeyString(key interface{}) (string, error) {
	switch v := key.(type) {
	case string:
		return v, nil
	case int, int64, int32, float64, float32, bool:
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("unsupported key type: %T", key)
	}
}

func compareKeys(a, b interface{}) int {
	switch aVal := a.(type) {
	case string:
		bVal, ok := b.(string)
		if !ok {
			return -1
		}
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case int64:
		bVal, ok := b.(int64)
		if !ok {
			return -1
		}
		return int(aVal - bVal)
	case float64:
		bVal, ok := b.(float64)
		if !ok {
			return -1
		}
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	default:
		return 0
	}
}
