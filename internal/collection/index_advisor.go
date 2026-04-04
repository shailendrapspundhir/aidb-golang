package collection

import (
	"fmt"
	"sort"
	"sync"
)

// IndexRecommendation represents a suggested index with scoring details
type IndexRecommendation struct {
	Type       string    `json:"type"`       // "single" or "composite"
	Fields     []string  `json:"fields"`     // field(s) to index
	Score      float64   `json:"score"`      // net benefit score (higher = better)
	Benefit    float64   `json:"benefit"`    // estimated benefit (time saved)
	Cost       float64   `json:"cost"`       // estimated cost (maintenance)
	Confidence float64   `json:"confidence"` // 0-1 how confident we are
	Reason     string    `json:"reason"`     // human-readable explanation
	IndexType  string    `json:"indexType"`  // "btree" or "hash" (btree preferred for ranges)
}

// IndexAdvisor analyzes query statistics and recommends indexes based on scoring
type IndexAdvisor struct {
	mu     sync.RWMutex
	config AutoIndexConfig
}

// AutoIndexConfig holds configuration for auto-indexing
type AutoIndexConfig struct {
	Enabled               bool
	MinQueryCount         int64   // minimum queries on a field to consider
	MinFullScanRatio      float64 // if full scans > this ratio, worth indexing
	TopNFields            int     // max single-field indexes to auto-create
	TopNPairs             int     // max composite indexes to auto-create
	ConfidenceThreshold   float64 // minimum confidence to recommend
	MaxIndexesPerColl     int     // safety cap on total indexes per collection
	EstimatedLatencySaved float64 // ms saved per query by using index (default 50ms)
	WritePenaltyMs        float64 // ms cost per write to maintain index (default 0.5ms)
	DefaultSelectivity    float64 // for fields without cardinality data (default 0.1)
}

// DefaultAutoIndexConfig returns sensible defaults
func DefaultAutoIndexConfig() AutoIndexConfig {
	return AutoIndexConfig{
		Enabled:               true,
		MinQueryCount:         100,
		MinFullScanRatio:      0.5,
		TopNFields:            5,
		TopNPairs:             3,
		ConfidenceThreshold:   0.6,
		MaxIndexesPerColl:     10,
		EstimatedLatencySaved: 50.0,
		WritePenaltyMs:        0.5,
		DefaultSelectivity:    0.1,
	}
}

// NewIndexAdvisor creates a new advisor with given config
func NewIndexAdvisor(config AutoIndexConfig) *IndexAdvisor {
	return &IndexAdvisor{config: config}
}

// Analyze computes index recommendations for a collection based on its query stats
func (a *IndexAdvisor) Analyze(col *Collection) []IndexRecommendation {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if col.queryStats == nil {
		return nil
	}

	summary := col.queryStats.GetSummary()
	recommendations := []IndexRecommendation{}

	// Get existing indexes to avoid duplicates
	existingIndexes := col.GetIndexes()
	existingIndexFields := make(map[string]bool)
	for field := range existingIndexes {
		existingIndexFields[field] = true
	}

	// ----- Single-field recommendations -----
	for field, queryCount := range summary.QueriesByField {
		if queryCount < a.config.MinQueryCount {
			continue
		}
		if existingIndexFields[field] {
			continue // already indexed
		}

		fullScanCount := summary.FullScansByField[field]
		fullScanRatio := 0.0
		if queryCount > 0 {
			fullScanRatio = float64(fullScanCount) / float64(queryCount)
		}
		if fullScanRatio < a.config.MinFullScanRatio {
			continue
		}

		// Estimate selectivity
		selectivity := a.config.DefaultSelectivity
		if idx, hasIdx := existingIndexes[field]; hasIdx {
			stats := idx.Stats()
			if stats.Cardinality > 0 {
				totalDocs := col.Count()
				if totalDocs > 0 {
					selectivity = 1.0 / float64(stats.Cardinality)
				}
			}
		}

		// Benefit: queries × fullScanRatio × selectivity × latencySaved
		benefit := float64(queryCount) * fullScanRatio * selectivity * a.config.EstimatedLatencySaved

		// Cost: writes × writePenalty
		writeCount := summary.WritesByField[field]
		cost := float64(writeCount) * a.config.WritePenaltyMs

		netScore := benefit - cost
		if netScore <= 0 {
			continue // not worth it
		}

		confidence := fullScanRatio
		if confidence > 1.0 {
			confidence = 1.0
		}

		recommendations = append(recommendations, IndexRecommendation{
			Type:       "single",
			Fields:     []string{field},
			Score:      netScore,
			Benefit:    benefit,
			Cost:       cost,
			Confidence: confidence,
			Reason:     buildSingleFieldReason(field, queryCount, fullScanRatio, selectivity),
			IndexType:  "btree", // default to btree for range support
		})
	}

	// ----- Composite (pair) recommendations -----
	// TODO: Implement composite index scoring when CreateCompositeIndex is available
	// for pair, pairCount := range col.queryStats.fieldPairCount { ... }

	// Sort by score descending
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Score > recommendations[j].Score
	})

	// Take top N
	if len(recommendations) > a.config.TopNFields {
		recommendations = recommendations[:a.config.TopNFields]
	}

	// Filter by confidence threshold
	filtered := []IndexRecommendation{}
	for _, r := range recommendations {
		if r.Confidence >= a.config.ConfidenceThreshold {
			filtered = append(filtered, r)
		}
	}

	return filtered
}

func buildSingleFieldReason(field string, queryCount int64, fullScanRatio, selectivity float64) string {
	pct := int(fullScanRatio * 100)
	return fmt.Sprintf("Field '%s' queried %d times; %d%% were full scans; selectivity ~%.3f",
		field, queryCount, pct, selectivity)
}
