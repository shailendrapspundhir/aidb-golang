package collection

import (
	"sync"
	"time"
)

// QueryStats holds statistics about a single query execution
type QueryStats struct {
	CollectionName   string                 `json:"collectionName"`
	Filter           map[string]interface{} `json:"filter"`
	IndexUsed        string                 `json:"indexUsed,omitempty"`
	IndexType        string                 `json:"indexType,omitempty"`
	ScanType         string                 `json:"scanType"` // "index", "full"
	DocumentsScanned int                    `json:"documentsScanned"`
	DocumentsMatched int                    `json:"documentsMatched"`
	DurationMs       int64                  `json:"durationMs"`
	Timestamp        time.Time              `json:"timestamp"`
}

// QueryStatsSummary holds aggregated query statistics
type QueryStatsSummary struct {
	TotalQueries        int64            `json:"totalQueries"`
	TotalDurationMs     int64            `json:"totalDurationMs"`
	AvgDurationMs       float64          `json:"avgDurationMs"`
	IndexHitCount       int64            `json:"indexHitCount"`
	FullScanCount       int64            `json:"fullScanCount"`
	IndexHitRate        float64          `json:"indexHitRate"`
	QueriesByField      map[string]int64 `json:"queriesByField,omitempty"`
	QueriesByCollection map[string]int64 `json:"queriesByCollection,omitempty"`
	// Auto-indexing related
	FullScansByField map[string]int64 `json:"fullScansByField,omitempty"`
	WritesByField    map[string]int64 `json:"writesByField,omitempty"`
}

// QueryStatisticsCollector collects and aggregates query statistics
type QueryStatisticsCollector struct {
	mu sync.RWMutex

	// Per-collection query history (limited size)
	recentQueries map[string][]QueryStats // collection -> recent queries

	// Aggregated stats
	totalQueries    int64
	totalDurationMs int64
	indexHitCount   int64
	fullScanCount   int64

	// Per-field and per-collection counters
	queriesByField      map[string]int64
	queriesByCollection map[string]int64

	// Auto-indexing: track full scans per field (queries that used no index)
	fullScansByField map[string]int64

	// Auto-indexing: track writes per field (for cost estimation)
	writesByField map[string]int64

	// Auto-indexing: field pair co-occurrence (key: "fieldA|fieldB" sorted)
	fieldPairCount map[string]int64

	// Max history per collection
	maxHistory int
}

// NewQueryStatisticsCollector creates a new collector
func NewQueryStatisticsCollector() *QueryStatisticsCollector {
	return &QueryStatisticsCollector{
		recentQueries:       make(map[string][]QueryStats),
		queriesByField:      make(map[string]int64),
		queriesByCollection: make(map[string]int64),
		fullScansByField:    make(map[string]int64),
		writesByField:       make(map[string]int64),
		fieldPairCount:      make(map[string]int64),
		maxHistory:          100, // keep last 100 queries per collection
	}
}

// RecordQuery records a query execution
func (qsc *QueryStatisticsCollector) RecordQuery(stats QueryStats) {
	qsc.mu.Lock()
	defer qsc.mu.Unlock()

	// Update totals
	qsc.totalQueries++
	qsc.totalDurationMs += stats.DurationMs

	if stats.ScanType == "index" {
		qsc.indexHitCount++
	} else {
		qsc.fullScanCount++
	}

	// Per-collection
	qsc.queriesByCollection[stats.CollectionName]++

	// Per-field: count ALL fields in the filter (not just first)
	fields := make([]string, 0, len(stats.Filter))
	for field := range stats.Filter {
		qsc.queriesByField[field]++
		fields = append(fields, field)

		// Track full scans per field (queries that didn't use an index)
		if stats.ScanType != "index" {
			qsc.fullScansByField[field]++
		}
	}

	// Track field pair co-occurrence (for composite index recommendations)
	if len(fields) >= 2 {
		// Sort fields for consistent key
		for i := 0; i < len(fields); i++ {
			for j := i + 1; j < len(fields); j++ {
				pair := fields[i] + "|" + fields[j]
				qsc.fieldPairCount[pair]++
			}
		}
	}

	// Append to recent queries
	history := qsc.recentQueries[stats.CollectionName]
	history = append(history, stats)
	if len(history) > qsc.maxHistory {
		history = history[len(history)-qsc.maxHistory:]
	}
	qsc.recentQueries[stats.CollectionName] = history
}

// RecordWrite records a write operation on a field (for cost estimation)
func (qsc *QueryStatisticsCollector) RecordWrite(field string) {
	qsc.mu.Lock()
	defer qsc.mu.Unlock()
	qsc.writesByField[field]++
}

// GetSummary returns aggregated statistics
func (qsc *QueryStatisticsCollector) GetSummary() QueryStatsSummary {
	qsc.mu.RLock()
	defer qsc.mu.RUnlock()

	var avg float64
	if qsc.totalQueries > 0 {
		avg = float64(qsc.totalDurationMs) / float64(qsc.totalQueries)
	}

	var hitRate float64
	if qsc.totalQueries > 0 {
		hitRate = float64(qsc.indexHitCount) / float64(qsc.totalQueries)
	}

	// Copy maps to avoid exposing internal state
	byField := make(map[string]int64)
	for k, v := range qsc.queriesByField {
		byField[k] = v
	}
	byColl := make(map[string]int64)
	for k, v := range qsc.queriesByCollection {
		byColl[k] = v
	}
	fullScans := make(map[string]int64)
	for k, v := range qsc.fullScansByField {
		fullScans[k] = v
	}
	writes := make(map[string]int64)
	for k, v := range qsc.writesByField {
		writes[k] = v
	}

	return QueryStatsSummary{
		TotalQueries:        qsc.totalQueries,
		TotalDurationMs:     qsc.totalDurationMs,
		AvgDurationMs:       avg,
		IndexHitCount:       qsc.indexHitCount,
		FullScanCount:       qsc.fullScanCount,
		IndexHitRate:        hitRate,
		QueriesByField:      byField,
		QueriesByCollection: byColl,
		FullScansByField:    fullScans,
		WritesByField:       writes,
	}
}

// GetRecentQueries returns recent queries for a collection
func (qsc *QueryStatisticsCollector) GetRecentQueries(collection string, limit int) []QueryStats {
	qsc.mu.RLock()
	defer qsc.mu.RUnlock()

	history := qsc.recentQueries[collection]
	if limit > 0 && limit < len(history) {
		history = history[len(history)-limit:]
	}
	// Return a copy
	result := make([]QueryStats, len(history))
	copy(result, history)
	return result
}

// Clear resets all statistics
func (qsc *QueryStatisticsCollector) Clear() {
	qsc.mu.Lock()
	defer qsc.mu.Unlock()

	qsc.recentQueries = make(map[string][]QueryStats)
	qsc.totalQueries = 0
	qsc.totalDurationMs = 0
	qsc.indexHitCount = 0
	qsc.fullScanCount = 0
	qsc.queriesByField = make(map[string]int64)
	qsc.queriesByCollection = make(map[string]int64)
	qsc.fullScansByField = make(map[string]int64)
	qsc.writesByField = make(map[string]int64)
	qsc.fieldPairCount = make(map[string]int64)
}

// ExplainPlan represents an execution plan for a query
type ExplainPlan struct {
	CollectionName string                 `json:"collectionName"`
	Filter         map[string]interface{} `json:"filter"`
	Strategy       string                 `json:"strategy"` // "index_scan", "full_scan"
	IndexUsed      string                 `json:"indexUsed,omitempty"`
	IndexType      string                 `json:"indexType,omitempty"`
	ScanType       string                 `json:"scanType,omitempty"` // "exact", "range", "prefix"
	EstimatedCost  int64                  `json:"estimatedCost"`
	EstimatedRows  int64                  `json:"estimatedRows"`
	Notes          []string               `json:"notes,omitempty"`
}
