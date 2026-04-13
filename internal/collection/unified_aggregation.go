package collection

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// VectorMatchStage performs vector similarity search as a pipeline stage
type VectorMatchStage struct {
	Field    string    `json:"field"`
	Vector   []float32 `json:"vector"`
	TopK     int       `json:"topK,omitempty"`
	MinScore float32   `json:"minScore,omitempty"`
}

// HybridSearchStage combines vector and text search
type HybridSearchStage struct {
	Vector  *VectorSearchSpec `json:"vector,omitempty"`
	Text    *TextSearchSpec   `json:"text,omitempty"`
	Scoring *ScoringConfig    `json:"scoring,omitempty"`
}

// VectorSearchSpec specifies vector search parameters
type VectorSearchSpec struct {
	Field  string    `json:"field"`
	Vector []float32 `json:"vector"`
	TopK   int       `json:"topK,omitempty"`
}

// TextSearchSpec specifies text search parameters
type TextSearchSpec struct {
	Field string `json:"field"`
	Query string `json:"query"`
	Limit int    `json:"limit,omitempty"`
}

// VectorGroupAccumulators contains vector-specific accumulators for $group
type VectorGroupAccumulators struct {
	// Average vector similarity score within group
	AvgVectorScore map[string]interface{} `json:"$avgVectorScore,omitempty"`
	// Maximum vector similarity score in group
	MaxVectorScore map[string]interface{} `json:"$maxVectorScore,omitempty"`
	// Minimum vector similarity score in group
	MinVectorScore map[string]interface{} `json:"$minVectorScore,omitempty"`
	// Compute centroid vector for group
	Centroid map[string]interface{} `json:"$centroid,omitempty"`
	// Top N documents by vector score
	TopN map[string]TopNAccumulator `json:"$topN,omitempty"`
}

// TopNAccumulator specifies top N accumulator options
type TopNAccumulator struct {
	By string `json:"by"` // Field to sort by (e.g., "$vectorScore")
	N  int    `json:"n"`  // Number of items to return
}

// VectorClusterStage clusters documents by vector similarity
type VectorClusterStage struct {
	Field         string `json:"field"`
	Algorithm     string `json:"algorithm"`     // "kmeans" or "dbscan"
	K             int    `json:"k,omitempty"`   // For kmeans
	Epsilon       float64 `json:"epsilon,omitempty"` // For dbscan
	MaxIterations int    `json:"maxIterations,omitempty"`
}

// SemanticGroupStage groups semantically similar documents
type SemanticGroupStage struct {
	VectorField        string  `json:"vectorField"`
	SimilarityThreshold float32 `json:"similarityThreshold"`
}

// UnifiedPipelineStage extends PipelineStage with vector-specific stages
type UnifiedPipelineStage struct {
	PipelineStage
	
	// Vector-specific stages
	VectorMatch    *VectorMatchStage    `json:"$vectorMatch,omitempty"`
	HybridSearch   *HybridSearchStage   `json:"$hybridSearch,omitempty"`
	VectorCluster  *VectorClusterStage  `json:"$vectorCluster,omitempty"`
	SemanticGroup  *SemanticGroupStage  `json:"$semanticGroup,omitempty"`
	
	// Vector-specific accumulators for $group
	AvgVectorScore map[string]interface{} `json:"$avgVectorScore,omitempty"`
	MaxVectorScore map[string]interface{} `json:"$maxVectorScore,omitempty"`
	MinVectorScore map[string]interface{} `json:"$minVectorScore,omitempty"`
	Centroid       map[string]interface{} `json:"$centroid,omitempty"`
	TopN           map[string]TopNAccumulator `json:"$topN,omitempty"`
}

// UnifiedGroupStage extends GroupStage with vector accumulators
type UnifiedGroupStage struct {
	GroupStage
	
	// Vector-specific accumulators
	AvgVectorScore map[string]interface{} `json:"$avgVectorScore,omitempty"`
	MaxVectorScore map[string]interface{} `json:"$maxVectorScore,omitempty"`
	MinVectorScore map[string]interface{} `json:"$minVectorScore,omitempty"`
	Centroid       map[string]interface{} `json:"$centroid,omitempty"`
	TopN           map[string]TopNAccumulator `json:"$topN,omitempty"`
}

// UnifiedAggregationPipeline represents an aggregation pipeline for unified collections
type UnifiedAggregationPipeline struct {
	stages []UnifiedPipelineStage
}

// NewUnifiedAggregationPipeline creates a new unified aggregation pipeline
func NewUnifiedAggregationPipeline(stages ...UnifiedPipelineStage) *UnifiedAggregationPipeline {
	return &UnifiedAggregationPipeline{stages: stages}
}

// AddStage adds a stage to the pipeline
func (p *UnifiedAggregationPipeline) AddStage(stage UnifiedPipelineStage) {
	p.stages = append(p.stages, stage)
}

// ExecuteAggregation runs the aggregation pipeline on a unified collection
func (c *UnifiedCollection) ExecuteAggregation(pipeline *UnifiedAggregationPipeline) ([]map[string]interface{}, error) {
	// Track vector scores for each document (populated by vector search stages)
	docScores := make(map[string]float32)
	docVectors := make(map[string][]float32)
	
	// Get all documents
	docs, err := c.FindAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get documents: %w", err)
	}
	
	// Convert to maps for pipeline processing
	results := make([]map[string]interface{}, 0, len(docs))
	for _, doc := range docs {
		docMap := doc.ToMap()
		results = append(results, docMap)
		
		// Store vectors for later use
		for fieldName, vecStore := range c.vectorStores {
			if vec, err := vecStore.Get(doc.ID); err == nil {
				// Store with field name prefix to distinguish multiple vector fields
				docVectors[doc.ID+"_"+fieldName] = vec
				// Also store without prefix for default field
				if _, exists := docVectors[doc.ID]; !exists {
					docVectors[doc.ID] = vec
				}
			}
		}
	}
	
	// Execute each stage
	for _, stage := range pipeline.stages {
		results, docScores, err = c.executeUnifiedStage(results, stage, docScores, docVectors)
		if err != nil {
			return nil, err
		}
	}
	
	return results, nil
}

// executeUnifiedStage executes a single pipeline stage
func (c *UnifiedCollection) executeUnifiedStage(
	docs []map[string]interface{},
	stage UnifiedPipelineStage,
	docScores map[string]float32,
	docVectors map[string][]float32,
) ([]map[string]interface{}, map[string]float32, error) {
	switch {
	case stage.VectorMatch != nil:
		return c.executeVectorMatch(docs, stage.VectorMatch, docScores, docVectors)
	case stage.HybridSearch != nil:
		return c.executeHybridSearch(docs, stage.HybridSearch, docScores, docVectors)
	case stage.VectorCluster != nil:
		return c.executeVectorCluster(docs, stage.VectorCluster, docVectors)
	case stage.SemanticGroup != nil:
		return c.executeSemanticGroup(docs, stage.SemanticGroup, docVectors)
	case stage.Match != nil:
		result, err := c.executeMatch(docs, stage.Match)
		return result, docScores, err
	case stage.Group != nil:
		return c.executeUnifiedGroup(docs, &stage, docScores, docVectors)
	case stage.Sort != nil:
		result, err := c.executeSort(docs, stage.Sort, docScores)
		return result, docScores, err
	case stage.Project != nil:
		result, err := c.executeProject(docs, stage.Project)
		return result, docScores, err
	case stage.Limit != nil:
		result, err := c.executeLimit(docs, *stage.Limit)
		return result, docScores, err
	case stage.Skip != nil:
		result, err := c.executeSkip(docs, *stage.Skip)
		return result, docScores, err
	case stage.AddFields != nil:
		result, err := c.executeAddFields(docs, stage.AddFields, docScores)
		return result, docScores, err
	case stage.Count != "":
		return []map[string]interface{}{{stage.Count: len(docs)}}, docScores, nil
	default:
		return docs, docScores, nil
	}
}

// executeVectorMatch performs vector similarity search
func (c *UnifiedCollection) executeVectorMatch(
	docs []map[string]interface{},
	stage *VectorMatchStage,
	docScores map[string]float32,
	docVectors map[string][]float32,
) ([]map[string]interface{}, map[string]float32, error) {
	// Get vector store for the field
	vecStore, exists := c.vectorStores[stage.Field]
	if !exists {
		return nil, docScores, fmt.Errorf("vector field %s not found", stage.Field)
	}
	
	// Search for similar vectors
	topK := stage.TopK
	if topK <= 0 {
		topK = 10
	}
	
	results, err := vecStore.Search(stage.Vector, topK, stage.MinScore)
	if err != nil {
		return nil, docScores, fmt.Errorf("vector search failed: %w", err)
	}
	
	// Create lookup map for docs by ID
	docMap := make(map[string]map[string]interface{})
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			docMap[id] = doc
		}
	}
	
	// Build result set with scores
	var matchedDocs []map[string]interface{}
	newScores := make(map[string]float32)
	for _, result := range results {
		if doc, exists := docMap[result.ID]; exists {
			// Add vectorScore to document
			docCopy := copyMap(doc)
			docCopy["vectorScore"] = result.Score
			matchedDocs = append(matchedDocs, docCopy)
			newScores[result.ID] = result.Score
		}
	}
	
	return matchedDocs, newScores, nil
}

// executeHybridSearch performs combined vector and text search
func (c *UnifiedCollection) executeHybridSearch(
	docs []map[string]interface{},
	stage *HybridSearchStage,
	docScores map[string]float32,
	docVectors map[string][]float32,
) ([]map[string]interface{}, map[string]float32, error) {
	// Collect candidate IDs and scores from each search type
	candidates := make(map[string]map[string]float64) // searchType -> docID -> score
	
	// Vector search
	if stage.Vector != nil && stage.Vector.Field != "" {
		vecStore, exists := c.vectorStores[stage.Vector.Field]
		if exists {
			topK := stage.Vector.TopK
			if topK <= 0 {
				topK = 100
			}
			results, err := vecStore.Search(stage.Vector.Vector, topK, 0)
			if err == nil {
				candidates["vector"] = make(map[string]float64)
				for _, r := range results {
					candidates["vector"][r.ID] = float64(r.Score)
				}
			}
		}
	}
	
	// Text search
	if stage.Text != nil && stage.Text.Field != "" {
		textStore, exists := c.textStores[stage.Text.Field]
		if exists {
			limit := stage.Text.Limit
			if limit <= 0 {
				limit = 100
			}
			results, err := textStore.Search(stage.Text.Query, limit)
			if err == nil {
				candidates["text"] = make(map[string]float64)
				for _, r := range results {
					candidates["text"][r.ID] = float64(r.Score)
				}
			}
		}
	}
	
	// Merge candidates using scoring method
	merged := mergeCandidatesWithScoring(candidates, stage.Scoring)
	
	// Create lookup map for docs by ID
	docMap := make(map[string]map[string]interface{})
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			docMap[id] = doc
		}
	}
	
	// Build result set with scores
	var matchedDocs []map[string]interface{}
	newScores := make(map[string]float32)
	for id, score := range merged {
		if doc, exists := docMap[id]; exists {
			docCopy := copyMap(doc)
			docCopy["hybridScore"] = float32(score)
			matchedDocs = append(matchedDocs, docCopy)
			newScores[id] = float32(score)
		}
	}
	
	// Sort by hybrid score descending
	sort.Slice(matchedDocs, func(i, j int) bool {
		si, _ := matchedDocs[i]["hybridScore"].(float32)
		sj, _ := matchedDocs[j]["hybridScore"].(float32)
		return si > sj
	})
	
	return matchedDocs, newScores, nil
}

// mergeCandidatesWithScoring merges candidates from multiple search types
func mergeCandidatesWithScoring(candidates map[string]map[string]float64, scoring *ScoringConfig) map[string]float64 {
	merged := make(map[string]float64)
	
	// Collect all unique doc IDs
	allIDs := make(map[string]bool)
	for _, typeResults := range candidates {
		for id := range typeResults {
			allIDs[id] = true
		}
	}
	
	// Default to weighted sum
	method := "weighted"
	if scoring != nil && scoring.Method != "" {
		method = scoring.Method
	}
	
	switch method {
	case "rrf":
		// Reciprocal Rank Fusion
		k := 60.0 // RRF constant
		for id := range allIDs {
			var rrfScore float64
			for _, typeResults := range candidates {
				if score, exists := typeResults[id]; exists {
					// Use score as rank proxy (higher score = lower rank)
					rank := 1.0 / (score + 0.01) // Avoid division by zero
					rrfScore += 1.0 / (k + rank)
				}
			}
			merged[id] = rrfScore
		}
	default: // weighted
		weights := map[string]float32{"vector": 0.5, "text": 0.5}
		if scoring != nil && len(scoring.Weights) > 0 {
			weights = scoring.Weights
		}
		
		for id := range allIDs {
			var totalScore float64
			for searchType, typeResults := range candidates {
				if score, exists := typeResults[id]; exists {
					weight := float64(weights[searchType])
					if weight == 0 {
						weight = 0.5
					}
					totalScore += score * weight
				}
			}
			merged[id] = totalScore
		}
	}
	
	return merged
}

// executeVectorCluster clusters documents by vector similarity
func (c *UnifiedCollection) executeVectorCluster(
	docs []map[string]interface{},
	stage *VectorClusterStage,
	docVectors map[string][]float32,
) ([]map[string]interface{}, map[string]float32, error) {
	// Get vectors for all documents
	vectors := make([][]float32, 0, len(docs))
	docIDs := make([]string, 0, len(docs))
	
	for _, doc := range docs {
		id, _ := doc["_id"].(string)
		if vec, exists := docVectors[id]; exists && len(vec) > 0 {
			vectors = append(vectors, vec)
			docIDs = append(docIDs, id)
		}
	}
	
	if len(vectors) == 0 {
		return []map[string]interface{}{}, nil, nil
	}
	
	// Perform clustering based on algorithm
	var assignments []int
	var centroids [][]float32
	
	switch stage.Algorithm {
	case "kmeans":
		k := stage.K
		if k <= 0 {
			k = 3
		}
		assignments, centroids = kMeansClustering(vectors, k, stage.MaxIterations)
	default:
		assignments, centroids = kMeansClustering(vectors, 3, 100)
	}
	
	// Group documents by cluster
	clusters := make(map[int][]map[string]interface{})
	for i, clusterID := range assignments {
		if i < len(docIDs) {
			docMap := copyMap(docs[i])
			docMap["clusterId"] = clusterID
			clusters[clusterID] = append(clusters[clusterID], docMap)
		}
	}
	
	// Build result with cluster info
	var results []map[string]interface{}
	for clusterID, clusterDocs := range clusters {
		result := map[string]interface{}{
			"clusterId":      clusterID,
			"centroid":       centroids[clusterID],
			"documentCount":  len(clusterDocs),
			"documents":      clusterDocs,
		}
		results = append(results, result)
	}
	
	// Sort by cluster ID
	sort.Slice(results, func(i, j int) bool {
		ci, _ := results[i]["clusterId"].(int)
		cj, _ := results[j]["clusterId"].(int)
		return ci < cj
	})
	
	return results, nil, nil
}

// kMeansClustering performs k-means clustering on vectors
func kMeansClustering(vectors [][]float32, k int, maxIterations int) ([]int, [][]float32) {
	n := len(vectors)
	if n == 0 || k <= 0 {
		return nil, nil
	}
	
	// Initialize centroids randomly from data points
	centroids := make([][]float32, k)
	for i := 0; i < k; i++ {
		centroids[i] = make([]float32, len(vectors[0]))
		copy(centroids[i], vectors[i%len(vectors)])
	}
	
	assignments := make([]int, n)
	
	for iter := 0; iter < maxIterations; iter++ {
		// Assign points to nearest centroid
		changed := false
		for i, vec := range vectors {
			minDist := math.MaxFloat64
			minCluster := 0
			for j, centroid := range centroids {
				dist := euclideanDistance(vec, centroid)
				if dist < minDist {
					minDist = dist
					minCluster = j
				}
			}
			if assignments[i] != minCluster {
				assignments[i] = minCluster
				changed = true
			}
		}
		
		if !changed {
			break
		}
		
		// Update centroids
		counts := make([]int, k)
		newCentroids := make([][]float32, k)
		for j := 0; j < k; j++ {
			newCentroids[j] = make([]float32, len(vectors[0]))
		}
		
		for i, vec := range vectors {
			cluster := assignments[i]
			counts[cluster]++
			for d := range vec {
				newCentroids[cluster][d] += vec[d]
			}
		}
		
		for j := 0; j < k; j++ {
			if counts[j] > 0 {
				for d := range newCentroids[j] {
					newCentroids[j][d] /= float32(counts[j])
				}
				centroids[j] = newCentroids[j]
			}
		}
	}
	
	return assignments, centroids
}

// euclideanDistance computes Euclidean distance between two vectors
func euclideanDistance(a, b []float32) float64 {
	var sum float64
	for i := range a {
		if i < len(b) {
			diff := float64(a[i] - b[i])
			sum += diff * diff
		}
	}
	return math.Sqrt(sum)
}

// executeSemanticGroup groups semantically similar documents
func (c *UnifiedCollection) executeSemanticGroup(
	docs []map[string]interface{},
	stage *SemanticGroupStage,
	docVectors map[string][]float32,
) ([]map[string]interface{}, map[string]float32, error) {
	// Build similarity graph and find connected components
	n := len(docs)
	visited := make([]bool, n)
	groups := make([][]int, 0)
	
	// Compute pairwise similarities
	for i := 0; i < n; i++ {
		if visited[i] {
			continue
		}
		
		group := []int{i}
		visited[i] = true
		
		id1, _ := docs[i]["_id"].(string)
		vec1, exists1 := docVectors[id1]
		if !exists1 {
			continue
		}
		
		// Find all similar documents
		for j := i + 1; j < n; j++ {
			if visited[j] {
				continue
			}
			
			id2, _ := docs[j]["_id"].(string)
			vec2, exists2 := docVectors[id2]
			if !exists2 {
				continue
			}
			
			// Compute cosine similarity
			sim := cosineSimilarity(vec1, vec2)
			if sim >= stage.SimilarityThreshold {
				group = append(group, j)
				visited[j] = true
			}
		}
		
		groups = append(groups, group)
	}
	
	// Build result
	var results []map[string]interface{}
	for groupID, group := range groups {
		groupDocs := make([]map[string]interface{}, len(group))
		for i, idx := range group {
			groupDocs[i] = copyMap(docs[idx])
		}
		
		result := map[string]interface{}{
			"semanticGroup": groupID,
			"documentCount": len(groupDocs),
			"documents":     groupDocs,
		}
		results = append(results, result)
	}
	
	return results, nil, nil
}

// cosineSimilarity computes cosine similarity between two vectors
func cosineSimilarity(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		if i < len(b) {
			dot += a[i] * b[i]
			normA += a[i] * a[i]
			normB += b[i] * b[i]
		}
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// executeUnifiedGroup handles grouping with vector accumulators
func (c *UnifiedCollection) executeUnifiedGroup(
	docs []map[string]interface{},
	stage *UnifiedPipelineStage,
	docScores map[string]float32,
	docVectors map[string][]float32,
) ([]map[string]interface{}, map[string]float32, error) {
	// Use the embedded GroupStage for standard accumulators
	group := stage.Group
	if group == nil {
		group = &GroupStage{}
	}
	
	groups := make(map[interface{}][]map[string]interface{})
	
	// Group documents by _id
	for _, doc := range docs {
		key := resolveGroupKey(doc, group.ID)
		groups[key] = append(groups[key], doc)
	}
	
	// Apply accumulators to each group
	var results []map[string]interface{}
	newScores := make(map[string]float32)
	
	for key, groupDocs := range groups {
		result := make(map[string]interface{})
		result["_id"] = key
		
		// Standard accumulators - use unified value resolution
		for field, expr := range group.Sum {
			result[field] = accumulateUnifiedSum(groupDocs, expr)
		}
		for field, expr := range group.Avg {
			result[field] = accumulateUnifiedAvg(groupDocs, expr)
		}
		for field, expr := range group.Min {
			result[field] = accumulateMin(groupDocs, expr)
		}
		for field, expr := range group.Max {
			result[field] = accumulateMax(groupDocs, expr)
		}
		for field := range group.Count {
			result[field] = len(groupDocs)
		}
		for field, expr := range group.First {
			result[field] = accumulateUnifiedFirst(groupDocs, expr)
		}
		for field, expr := range group.Last {
			result[field] = accumulateLast(groupDocs, expr)
		}
		for field, expr := range group.Push {
			result[field] = accumulateUnifiedPush(groupDocs, expr)
		}
		for field, expr := range group.AddToSet {
			result[field] = accumulateAddToSet(groupDocs, expr)
		}
		
		// Vector-specific accumulators
		for field := range stage.AvgVectorScore {
			result[field] = accumulateAvgVectorScore(groupDocs, docScores)
		}
		for field := range stage.MaxVectorScore {
			result[field] = accumulateMaxVectorScore(groupDocs, docScores)
		}
		for field := range stage.MinVectorScore {
			result[field] = accumulateMinVectorScore(groupDocs, docScores)
		}
		for field := range stage.Centroid {
			result[field] = accumulateCentroid(groupDocs, docVectors)
		}
		for field, topNConfig := range stage.TopN {
			result[field] = accumulateTopN(groupDocs, topNConfig, docScores)
		}
		
		results = append(results, result)
	}
	
	return results, newScores, nil
}

// Vector accumulator functions

func accumulateAvgVectorScore(docs []map[string]interface{}, docScores map[string]float32) float32 {
	var sum float32
	var count int
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			if score, exists := docScores[id]; exists {
				sum += score
				count++
			}
		}
		// Also check for vectorScore field added by $vectorMatch
		if score, ok := doc["vectorScore"].(float32); ok {
			sum += score
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float32(count)
}

func accumulateMaxVectorScore(docs []map[string]interface{}, docScores map[string]float32) float32 {
	var max float32
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			if score, exists := docScores[id]; exists && score > max {
				max = score
			}
		}
		if score, ok := doc["vectorScore"].(float32); ok && score > max {
			max = score
		}
	}
	return max
}

func accumulateMinVectorScore(docs []map[string]interface{}, docScores map[string]float32) float32 {
	min := float32(math.MaxFloat32)
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			if score, exists := docScores[id]; exists && score < min {
				min = score
			}
		}
		if score, ok := doc["vectorScore"].(float32); ok && score < min {
			min = score
		}
	}
	if min == float32(math.MaxFloat32) {
		return 0
	}
	return min
}

func accumulateCentroid(docs []map[string]interface{}, docVectors map[string][]float32) []float32 {
	if len(docs) == 0 {
		return nil
	}
	
	// Get dimensions from first vector
	var dims int
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			if vec, exists := docVectors[id]; exists && len(vec) > 0 {
				dims = len(vec)
				break
			}
		}
	}
	
	if dims == 0 {
		return nil
	}
	
	// Sum all vectors
	centroid := make([]float32, dims)
	var count int
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			if vec, exists := docVectors[id]; exists && len(vec) == dims {
				for i := range vec {
					centroid[i] += vec[i]
				}
				count++
			}
		}
	}
	
	// Average
	if count > 0 {
		for i := range centroid {
			centroid[i] /= float32(count)
		}
	}
	
	return centroid
}

func accumulateTopN(docs []map[string]interface{}, config TopNAccumulator, docScores map[string]float32) []map[string]interface{} {
	// Sort documents by the specified field
	sort.Slice(docs, func(i, j int) bool {
		var si, sj float32
		
		// Check for vectorScore or hybridScore
		if config.By == "$vectorScore" || config.By == "vectorScore" {
			if score, ok := docs[i]["vectorScore"].(float32); ok {
				si = score
			}
			if score, ok := docs[j]["vectorScore"].(float32); ok {
				sj = score
			}
		} else if config.By == "$hybridScore" || config.By == "hybridScore" {
			if score, ok := docs[i]["hybridScore"].(float32); ok {
				si = score
			}
			if score, ok := docs[j]["hybridScore"].(float32); ok {
				sj = score
			}
		}
		
		return si > sj // Descending order
	})
	
	// Return top N
	n := config.N
	if n <= 0 {
		n = 5
	}
	if n > len(docs) {
		n = len(docs)
	}
	
	return docs[:n]
}

// Helper methods for unified collection

// resolveUnifiedValue resolves a value from a unified document
func resolveUnifiedValue(doc map[string]interface{}, expr interface{}) interface{} {
	switch v := expr.(type) {
	case string:
		if strings.HasPrefix(v, "$") {
			fieldPath := v[1:] // Remove $ prefix
			return getUnifiedFieldValue(doc, fieldPath)
		}
		return v
	case map[string]interface{}:
		// Handle expressions
		return evaluateExpression(doc, v)
	default:
		return v
	}
}

// getUnifiedFieldValue gets a field value from a unified document structure
func getUnifiedFieldValue(doc map[string]interface{}, fieldPath string) interface{} {
	// Try direct field access first
	if val, ok := doc[fieldPath]; ok {
		return val
	}
	
	// Try fields.fieldName.scalar pattern for unified documents
	if fields, ok := doc["fields"].(map[string]interface{}); ok {
		if field, ok := fields[fieldPath].(map[string]interface{}); ok {
			// Return scalar value if present
			if scalar, ok := field["scalar"]; ok {
				return scalar
			}
			// Return vector value if present
			if vector, ok := field["vector"]; ok {
				return vector
			}
			// Return the field itself
			return field
		}
		// Direct access within fields
		if val, ok := fields[fieldPath]; ok {
			return val
		}
	}
	
	// Try nested field access
	parts := strings.Split(fieldPath, ".")
	var current interface{} = doc
	for _, part := range parts {
		switch c := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = c[part]
			if !ok {
				return nil
			}
		default:
			return nil
		}
	}
	return current
}

func (c *UnifiedCollection) executeMatch(docs []map[string]interface{}, match *MatchStage) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	for _, doc := range docs {
		if matchesUnifiedConditions(doc, match) {
			results = append(results, doc)
		}
	}
	return results, nil
}

// matchesUnifiedConditions checks if a unified document matches conditions
func matchesUnifiedConditions(doc map[string]interface{}, match *MatchStage) bool {
	// Check simple field matches (these come from raw JSON that wasn't parsed into struct fields)
	// The MatchStage struct has json:"-" on Fields, so we need to handle this separately
	// For now, we'll check if the match has any conditions that weren't parsed
	
	// Check comparison operators
	for field, value := range match.Eq {
		if getUnifiedFieldValue(doc, field) != value {
			return false
		}
	}
	
	for field, value := range match.Ne {
		if getUnifiedFieldValue(doc, field) == value {
			return false
		}
	}
	
	for field, value := range match.Gt {
		if !compareUnifiedGreater(doc, field, value) {
			return false
		}
	}
	
	for field, value := range match.Gte {
		if !compareUnifiedGreaterOrEqual(doc, field, value) {
			return false
		}
	}
	
	for field, value := range match.Lt {
		if !compareUnifiedLess(doc, field, value) {
			return false
		}
	}
	
	for field, value := range match.Lte {
		if !compareUnifiedLessOrEqual(doc, field, value) {
			return false
		}
	}
	
	// Check $and conditions
	if len(match.And) > 0 {
		for _, cond := range match.And {
			if !matchUnifiedSimpleConditions(doc, cond) {
				return false
			}
		}
	}
	
	// Check $or conditions
	if len(match.Or) > 0 {
		matched := false
		for _, cond := range match.Or {
			if matchUnifiedSimpleConditions(doc, cond) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	
	return true
}

// matchUnifiedSimpleConditions matches simple field: value conditions
func matchUnifiedSimpleConditions(doc map[string]interface{}, conds map[string]interface{}) bool {
	for field, value := range conds {
		if !matchUnifiedField(doc, field, value) {
			return false
		}
	}
	return true
}

// matchUnifiedField matches a field in a unified document
func matchUnifiedField(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getUnifiedFieldValue(doc, field)
	
	// Handle comparison operators in value
	if m, ok := value.(map[string]interface{}); ok {
		for op, v := range m {
			switch op {
			case "$eq":
				if docValue != v {
					return false
				}
			case "$ne":
				if docValue == v {
					return false
				}
			case "$gt":
				if !compareValues(docValue, v, ">") {
					return false
				}
			case "$gte":
				if !compareValues(docValue, v, ">=") {
					return false
				}
			case "$lt":
				if !compareValues(docValue, v, "<") {
					return false
				}
			case "$lte":
				if !compareValues(docValue, v, "<=") {
					return false
				}
			}
		}
		return true
	}
	
	return docValue == value
}

// compareUnifiedGreater checks if field value is greater than value
func compareUnifiedGreater(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getUnifiedFieldValue(doc, field)
	return compareValues(docValue, value, ">")
}

// compareUnifiedGreaterOrEqual checks if field value is greater than or equal to value
func compareUnifiedGreaterOrEqual(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getUnifiedFieldValue(doc, field)
	return compareValues(docValue, value, ">=")
}

// compareUnifiedLess checks if field value is less than value
func compareUnifiedLess(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getUnifiedFieldValue(doc, field)
	return compareValues(docValue, value, "<")
}

// compareUnifiedLessOrEqual checks if field value is less than or equal to value
func compareUnifiedLessOrEqual(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getUnifiedFieldValue(doc, field)
	return compareValues(docValue, value, "<=")
}

// Unified accumulator functions

func accumulateUnifiedSum(docs []map[string]interface{}, expr interface{}) interface{} {
	var sum float64
	for _, doc := range docs {
		if val := resolveUnifiedValue(doc, expr); val != nil {
			if num, ok := toFloat64(val); ok {
				sum += num
			}
		}
	}
	return sum
}

func accumulateUnifiedAvg(docs []map[string]interface{}, expr interface{}) interface{} {
	var sum float64
	var count int
	for _, doc := range docs {
		if val := resolveUnifiedValue(doc, expr); val != nil {
			if num, ok := toFloat64(val); ok {
				sum += num
				count++
			}
		}
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

func accumulateUnifiedFirst(docs []map[string]interface{}, expr interface{}) interface{} {
	if len(docs) == 0 {
		return nil
	}
	return resolveUnifiedValue(docs[0], expr)
}

func accumulateUnifiedPush(docs []map[string]interface{}, expr interface{}) interface{} {
	var results []interface{}
	for _, doc := range docs {
		results = append(results, resolveUnifiedValue(doc, expr))
	}
	return results
}

func (c *UnifiedCollection) executeSort(docs []map[string]interface{}, sortStage *SortStage, docScores map[string]float32) ([]map[string]interface{}, error) {
	sortSpec := make([]sortField, 0)
	for field, order := range sortStage.Fields {
		sortSpec = append(sortSpec, sortField{field: field, order: order})
	}
	
	sort.Slice(docs, func(i, j int) bool {
		for _, sf := range sortSpec {
			var vi, vj interface{}
			
			// Handle vectorScore and hybridScore specially
			if sf.field == "vectorScore" || sf.field == "$vectorScore" {
				vi = docs[i]["vectorScore"]
				vj = docs[j]["vectorScore"]
			} else if sf.field == "hybridScore" || sf.field == "$hybridScore" {
				vi = docs[i]["hybridScore"]
				vj = docs[j]["hybridScore"]
			} else {
				vi = getFieldValue(docs[i], sf.field)
				vj = getFieldValue(docs[j], sf.field)
			}
			
			cmp := compareValuesForSort(vi, vj)
			if cmp != 0 {
				if sf.order == 1 {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false
	})
	
	return docs, nil
}

func (c *UnifiedCollection) executeProject(docs []map[string]interface{}, project *ProjectStage) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	
	for _, doc := range docs {
		result := make(map[string]interface{})
		
		// Handle Fields map
		for field, spec := range project.Fields {
			if b, ok := spec.(bool); ok {
				if b {
					if val, ok := getNestedField(doc, field); ok {
						result[field] = val
					}
				}
			} else if m, ok := spec.(map[string]interface{}); ok {
				result[field] = evaluateExpression(doc, m)
			} else {
				result[field] = spec
			}
		}
		
		results = append(results, result)
	}
	
	return results, nil
}

func (c *UnifiedCollection) executeLimit(docs []map[string]interface{}, limit int) ([]map[string]interface{}, error) {
	if limit >= len(docs) {
		return docs, nil
	}
	return docs[:limit], nil
}

func (c *UnifiedCollection) executeSkip(docs []map[string]interface{}, skip int) ([]map[string]interface{}, error) {
	if skip >= len(docs) {
		return []map[string]interface{}{}, nil
	}
	return docs[skip:], nil
}

func (c *UnifiedCollection) executeAddFields(docs []map[string]interface{}, fields map[string]interface{}, docScores map[string]float32) ([]map[string]interface{}, error) {
	for _, doc := range docs {
		for field, expr := range fields {
			// Handle special expressions
			if str, ok := expr.(string); ok {
				if str == "$$vectorScore" {
					if id, ok := doc["_id"].(string); ok {
						doc[field] = docScores[id]
					}
					continue
				}
			}
			doc[field] = resolveValue(doc, expr)
		}
	}
	return docs, nil
}
