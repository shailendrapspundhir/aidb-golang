package storage

import (
	"container/heap"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// HNSWIndex is a Hierarchical Navigable Small World graph index for vector similarity search
type HNSWIndex struct {
	mu sync.RWMutex

	// Configuration
	M              int     // Max number of connections per element per layer
	MMax           int     // Max number of connections per element at layer 0
	EfConstruction int     // Size of dynamic candidate list during construction
	Ml             float64 // Level generation factor

	// Graph structure
	nodes     map[string]*HNSWNode // node ID -> node
	entryPoint string               // Entry point node ID
	maxLevel  int                   // Current maximum level in the graph

	// Distance function
	distanceFunc func(a, b []float64) float64

	// Random source
	rng *rand.Rand
}

// HNSWNode represents a node in the HNSW graph
type HNSWNode struct {
	ID         string
	Vector     []float64
	Metadata   map[string]interface{}
	Level      int
	Neighbors  [][]string // Neighbors[level] = list of neighbor IDs
}

// HNSWConfig contains configuration for HNSW index
type HNSWConfig struct {
	M              int     // Max connections per layer (default: 16)
	MMax           int     // Max connections at layer 0 (default: 2*M)
	EfConstruction int     // Construction candidate list size (default: 200)
	Ml             float64 // Level factor (default: 1/ln(M))
	DistanceMetric string  // "cosine", "euclidean", or "dot"
}

// NewHNSWIndex creates a new HNSW index
func NewHNSWIndex(config HNSWConfig) *HNSWIndex {
	// Set defaults
	if config.M <= 0 {
		config.M = 16
	}
	if config.MMax <= 0 {
		config.MMax = 2 * config.M
	}
	if config.EfConstruction <= 0 {
		config.EfConstruction = 200
	}
	if config.Ml <= 0 {
		config.Ml = 1.0 / math.Log(float64(config.M))
	}

	// Set distance function
	var distFunc func(a, b []float64) float64
	switch config.DistanceMetric {
	case "euclidean":
		distFunc = euclideanDistance
	case "dot":
		distFunc = dotProductDistance
	default: // cosine
		distFunc = cosineDistance
	}

	return &HNSWIndex{
		M:              config.M,
		MMax:           config.MMax,
		EfConstruction: config.EfConstruction,
		Ml:             config.Ml,
		nodes:          make(map[string]*HNSWNode),
		distanceFunc:   distFunc,
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Insert adds a vector to the HNSW index
func (h *HNSWIndex) Insert(id string, vector []float64, metadata map[string]interface{}) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if already exists
	if _, exists := h.nodes[id]; exists {
		return nil // Already exists, skip
	}

	// Generate random level for this node
	level := h.randomLevel()

	// Create new node
	node := &HNSWNode{
		ID:        id,
		Vector:    vector,
		Metadata:  metadata,
		Level:     level,
		Neighbors: make([][]string, level+1),
	}

	for i := 0; i <= level; i++ {
		node.Neighbors[i] = make([]string, 0)
	}

	// If this is the first node
	if h.entryPoint == "" {
		h.nodes[id] = node
		h.entryPoint = id
		h.maxLevel = level
		return nil
	}

	// Find entry point at the highest level
	currentNode := h.entryPoint
	currentDist := h.distanceFunc(vector, h.nodes[currentNode].Vector)

	// Traverse from top level to the level of the new node
	for lc := h.maxLevel; lc > level; lc-- {
		changed := true
		for changed {
			changed = false
			for _, neighborID := range h.nodes[currentNode].Neighbors[lc] {
				neighbor := h.nodes[neighborID]
				if neighbor == nil {
					continue
				}
				dist := h.distanceFunc(vector, neighbor.Vector)
				if dist < currentDist {
					currentNode = neighborID
					currentDist = dist
					changed = true
				}
			}
		}
	}

	// Insert at each level from level down to 0
	for lc := min(level, h.maxLevel); lc >= 0; lc-- {
		candidates := h.searchLayer(vector, currentNode, h.EfConstruction, lc)
		neighbors := h.selectNeighbors(candidates, h.getM(lc))

		// Add bidirectional connections
		node.Neighbors[lc] = neighbors
		for _, neighborID := range neighbors {
			neighbor := h.nodes[neighborID]
			if neighbor != nil && lc < len(neighbor.Neighbors) {
				neighbor.Neighbors[lc] = append(neighbor.Neighbors[lc], id)
				// Prune if too many connections
				if len(neighbor.Neighbors[lc]) > h.getM(lc) {
					neighbor.Neighbors[lc] = h.pruneNeighbors(neighbor.ID, neighbor.Neighbors[lc], lc)
				}
			}
		}

		// Update current node for next layer
		if len(candidates) > 0 {
			currentNode = candidates[0].id
		}
	}

	// Add node to index
	h.nodes[id] = node

	// Update entry point if new node has higher level
	if level > h.maxLevel {
		h.entryPoint = id
		h.maxLevel = level
	}

	return nil
}

// Delete removes a vector from the HNSW index
func (h *HNSWIndex) Delete(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, exists := h.nodes[id]
	if !exists {
		return nil
	}

	// Remove connections from neighbors
	for lc := 0; lc <= node.Level; lc++ {
		for _, neighborID := range node.Neighbors[lc] {
			neighbor := h.nodes[neighborID]
			if neighbor != nil && lc < len(neighbor.Neighbors) {
				// Remove this node from neighbor's connections
				newNeighbors := make([]string, 0)
				for _, nid := range neighbor.Neighbors[lc] {
					if nid != id {
						newNeighbors = append(newNeighbors, nid)
					}
				}
				neighbor.Neighbors[lc] = newNeighbors
			}
		}
	}

	delete(h.nodes, id)

	// Update entry point if needed
	if h.entryPoint == id {
		if len(h.nodes) > 0 {
			// Find new entry point (highest level node)
			for nid, n := range h.nodes {
				if h.entryPoint == id || n.Level > h.nodes[h.entryPoint].Level {
					h.entryPoint = nid
				}
			}
			h.maxLevel = h.nodes[h.entryPoint].Level
		} else {
			h.entryPoint = ""
			h.maxLevel = 0
		}
	}

	return nil
}

// Search finds the k nearest neighbors
func (h *HNSWIndex) Search(query []float64, k int, ef int) []SearchResult {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.entryPoint == "" || len(h.nodes) == 0 {
		return []SearchResult{}
	}

	if ef <= 0 {
		ef = max(k, 50)
	}

	// Start from entry point
	currentNode := h.entryPoint
	currentDist := h.distanceFunc(query, h.nodes[currentNode].Vector)

	// Traverse from top level to level 1
	for lc := h.maxLevel; lc > 0; lc-- {
		changed := true
		for changed {
			changed = false
			for _, neighborID := range h.nodes[currentNode].Neighbors[lc] {
				neighbor := h.nodes[neighborID]
				if neighbor == nil {
					continue
				}
				dist := h.distanceFunc(query, neighbor.Vector)
				if dist < currentDist {
					currentNode = neighborID
					currentDist = dist
					changed = true
				}
			}
		}
	}

	// Search at layer 0 with ef candidates
	candidates := h.searchLayer(query, currentNode, ef, 0)

	// Return top k results
	results := make([]SearchResult, 0, k)
	for i := 0; i < min(k, len(candidates)); i++ {
		c := candidates[i]
		node := h.nodes[c.id]
		if node != nil {
			results = append(results, SearchResult{
				ID:       c.id,
				Distance: c.dist,
				Vector:   node.Vector,
				Metadata: node.Metadata,
			})
		}
	}

	return results
}

// SearchWithFilter finds k nearest neighbors with metadata filter
func (h *HNSWIndex) SearchWithFilter(query []float64, k int, ef int, filter map[string]interface{}) []SearchResult {
	// Get more candidates to account for filtering
	efSearch := max(ef, k*10)
	candidates := h.Search(query, efSearch, efSearch)

	results := make([]SearchResult, 0, k)
	for _, c := range candidates {
		if matchesMetadataFilter(c.Metadata, filter) {
			results = append(results, c)
			if len(results) >= k {
				break
			}
		}
	}

	return results
}

// SearchResult represents a search result
type SearchResult struct {
	ID       string
	Distance float64
	Vector   []float64
	Metadata map[string]interface{}
}

// Get retrieves a node by ID
func (h *HNSWIndex) Get(id string) (*HNSWNode, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	node, exists := h.nodes[id]
	return node, exists
}

// Count returns the number of vectors in the index
func (h *HNSWIndex) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// Clear removes all vectors from the index
func (h *HNSWIndex) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nodes = make(map[string]*HNSWNode)
	h.entryPoint = ""
	h.maxLevel = 0
}

// Stats returns index statistics
func (h *HNSWIndex) Stats() HNSWStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	totalConnections := 0
	for _, node := range h.nodes {
		for _, neighbors := range node.Neighbors {
			totalConnections += len(neighbors)
		}
	}

	return HNSWStats{
		NodeCount:        len(h.nodes),
		MaxLevel:         h.maxLevel,
		M:                h.M,
		EfConstruction:   h.EfConstruction,
		TotalConnections: totalConnections,
		AvgConnections:   float64(totalConnections) / float64(max(len(h.nodes), 1)),
	}
}

// HNSWStats contains HNSW index statistics
type HNSWStats struct {
	NodeCount        int
	MaxLevel         int
	M                int
	EfConstruction   int
	TotalConnections int
	AvgConnections   float64
}

// Internal methods

type candidate struct {
	id   string
	dist float64
}

type candidateHeap []candidate

func (h candidateHeap) Len() int           { return len(h) }
func (h candidateHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h candidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *candidateHeap) Push(x interface{}) {
	*h = append(*h, x.(candidate))
}

func (h *candidateHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *HNSWIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < 1.0/math.Exp(h.Ml) && level < 50 {
		level++
	}
	return level
}

func (h *HNSWIndex) getM(level int) int {
	if level == 0 {
		return h.MMax
	}
	return h.M
}

func (h *HNSWIndex) searchLayer(query []float64, entryPoint string, ef int, level int) []candidate {
	visited := make(map[string]bool)
	candidates := &candidateHeap{}
	results := &candidateHeap{}

	heap.Init(candidates)
	heap.Init(results)

	entryDist := h.distanceFunc(query, h.nodes[entryPoint].Vector)
	heap.Push(candidates, candidate{id: entryPoint, dist: entryDist})
	heap.Push(results, candidate{id: entryPoint, dist: entryDist})
	visited[entryPoint] = true

	for candidates.Len() > 0 {
		// Get nearest unprocessed candidate
		c := heap.Pop(candidates).(candidate)
		
		// Get furthest result
		furthestResult := (*results)[0]

		if c.dist > furthestResult.dist && results.Len() >= ef {
			break
		}

		// Explore neighbors
		node := h.nodes[c.id]
		if node == nil || level >= len(node.Neighbors) {
			continue
		}

		for _, neighborID := range node.Neighbors[level] {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			neighbor := h.nodes[neighborID]
			if neighbor == nil {
				continue
			}

			dist := h.distanceFunc(query, neighbor.Vector)

			if results.Len() < ef || dist < furthestResult.dist {
				heap.Push(candidates, candidate{id: neighborID, dist: dist})
				heap.Push(results, candidate{id: neighborID, dist: dist})

				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	// Convert results heap to sorted slice
	resultSlice := make([]candidate, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		resultSlice[i] = heap.Pop(results).(candidate)
	}

	return resultSlice
}

func (h *HNSWIndex) selectNeighbors(candidates []candidate, m int) []string {
	if len(candidates) <= m {
		neighbors := make([]string, len(candidates))
		for i, c := range candidates {
			neighbors[i] = c.id
		}
		return neighbors
	}

	// Sort by distance and take top m
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	neighbors := make([]string, m)
	for i := 0; i < m; i++ {
		neighbors[i] = candidates[i].id
	}
	return neighbors
}

func (h *HNSWIndex) pruneNeighbors(nodeID string, neighbors []string, level int) []string {
	if len(neighbors) <= h.getM(level) {
		return neighbors
	}

	node := h.nodes[nodeID]
	if node == nil {
		return neighbors
	}

	// Calculate distances to all neighbors
	candidates := make([]candidate, len(neighbors))
	for i, neighborID := range neighbors {
		neighbor := h.nodes[neighborID]
		if neighbor != nil {
			candidates[i] = candidate{
				id:   neighborID,
				dist: h.distanceFunc(node.Vector, neighbor.Vector),
			}
		}
	}

	// Sort and take top M
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	result := make([]string, h.getM(level))
	for i := 0; i < h.getM(level) && i < len(candidates); i++ {
		result[i] = candidates[i].id
	}
	return result
}

// Distance functions

func cosineDistance(a, b []float64) float64 {
	dot := 0.0
	normA := 0.0
	normB := 0.0

	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	return 1.0 - dot/(math.Sqrt(normA)*math.Sqrt(normB))
}

func euclideanDistance(a, b []float64) float64 {
	sum := 0.0
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

func dotProductDistance(a, b []float64) float64 {
	// For dot product, higher is better, so we negate for distance
	dot := 0.0
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}

// Helper functions

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func matchesMetadataFilter(metadata map[string]interface{}, filter map[string]interface{}) bool {
	if filter == nil {
		return true
	}
	for key, value := range filter {
		metaValue, exists := metadata[key]
		if !exists || metaValue != value {
			return false
		}
	}
	return true
}
