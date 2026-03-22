package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aidb/internal/document"
)

// TempStorageManager manages temporary storage for spill-to-disk operations.
// It creates temporary files for intermediate aggregation results and
// automatically cleans them up based on TTL or explicit release.
type TempStorageManager struct {
	mu sync.RWMutex

	// Configuration
	config TempStorageConfig

	// State
	tempFiles    map[string]*TempFile
	totalBytes   int64
	fileCounter  int64
	cleanupTimer *time.Ticker

	// Shutdown
	stopCh chan struct{}
}

// TempStorageConfig holds configuration for temp storage
type TempStorageConfig struct {
	// Directory for temp files (default /tmp/aidb_spill)
	SpillDir string

	// Prefix for temp file names
	FilePrefix string

	// TTL for temp files (default 1 hour)
	DefaultTTL time.Duration

	// Max total bytes before cleanup is forced (0 = unlimited)
	MaxTotalBytes int64

	// Max number of temp files (0 = unlimited)
	MaxFiles int

	// Cleanup interval (default 5 minutes)
	CleanupInterval time.Duration
}

// DefaultTempStorageConfig returns sensible defaults
func DefaultTempStorageConfig() TempStorageConfig {
	return TempStorageConfig{
		SpillDir:        "/tmp/aidb_spill",
		FilePrefix:      "aidb_temp_",
		DefaultTTL:      1 * time.Hour,
		MaxTotalBytes:   10 * 1024 * 1024 * 1024, // 10GB
		MaxFiles:        1000,
		CleanupInterval: 5 * time.Minute,
	}
}

// TempFile represents a temporary file for spill storage
type TempFile struct {
	ID           string
	Path         string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	Size         int64
	File         *os.File
	WriteOffset  int64
	ReadOffset   int64
	IsSealed     bool // No more writes allowed
	IsCompressed bool
	RefCount     int32
	Metadata     map[string]interface{}
}

// TempFileStats holds statistics about a temp file
type TempFileStats struct {
	ID           string
	Path         string
	Size         int64
	CreatedAt    time.Time
	ExpiresAt    time.Time
	Age          time.Duration
	IsSealed     bool
	IsCompressed bool
	RefCount     int32
}

// TempStorageStats holds overall temp storage statistics
type TempStorageStats struct {
	TotalFiles    int
	TotalBytes    int64
	OldestFile    time.Time
	NewestFile    time.Time
	ExpiredFiles  int
	ActiveWriters int
	ActiveReaders int
}

// NewTempStorageManager creates a new temp storage manager
func NewTempStorageManager(config TempStorageConfig) (*TempStorageManager, error) {
	// Apply defaults
	if config.SpillDir == "" {
		config.SpillDir = "/tmp/aidb_spill"
	}
	if config.FilePrefix == "" {
		config.FilePrefix = "aidb_temp_"
	}
	if config.DefaultTTL <= 0 {
		config.DefaultTTL = 1 * time.Hour
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 5 * time.Minute
	}

	// Ensure spill directory exists
	if err := os.MkdirAll(config.SpillDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create spill directory: %w", err)
	}

	tsm := &TempStorageManager{
		config:     config,
		tempFiles:  make(map[string]*TempFile),
		stopCh:     make(chan struct{}),
	}

	// Start cleanup goroutine
	tsm.cleanupTimer = time.NewTicker(config.CleanupInterval)
	go tsm.cleanupLoop()

	return tsm, nil
}

// CreateTempFile creates a new temporary file for writing
func (tsm *TempStorageManager) CreateTempFile(prefix string, ttl time.Duration) (*TempFile, error) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	// Check limits
	if tsm.config.MaxFiles > 0 && len(tsm.tempFiles) >= tsm.config.MaxFiles {
		// Force cleanup of oldest expired files
		tsm.cleanupExpiredLocked()
		if len(tsm.tempFiles) >= tsm.config.MaxFiles {
			return nil, fmt.Errorf("max temp files limit reached (%d)", tsm.config.MaxFiles)
		}
	}

	if ttl <= 0 {
		ttl = tsm.config.DefaultTTL
	}

	// Generate unique ID
	id := fmt.Sprintf("%s%d_%d", tsm.config.FilePrefix+prefix, time.Now().UnixNano(), atomic.AddInt64(&tsm.fileCounter, 1))
	path := filepath.Join(tsm.config.SpillDir, id+".jsonl")

	// Create file
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	tf := &TempFile{
		ID:        id,
		Path:      path,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(ttl),
		File:      f,
		Metadata:  make(map[string]interface{}),
	}

	tsm.tempFiles[id] = tf
	return tf, nil
}

// GetTempFile retrieves an existing temp file by ID
func (tsm *TempStorageManager) GetTempFile(id string) (*TempFile, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	tf, exists := tsm.tempFiles[id]
	if !exists {
		return nil, fmt.Errorf("temp file not found: %s", id)
	}
	return tf, nil
}

// SealTempFile marks a temp file as complete (no more writes)
func (tsm *TempStorageManager) SealTempFile(id string) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	tf, exists := tsm.tempFiles[id]
	if !exists {
		return fmt.Errorf("temp file not found: %s", id)
	}

	if tf.IsSealed {
		return nil // Already sealed
	}

	// Sync and prepare for reading
	if tf.File != nil {
		if err := tf.File.Sync(); err != nil {
			return fmt.Errorf("failed to sync temp file: %w", err)
		}
		// Close and reopen for reading
		tf.File.Close()
		f, err := os.Open(tf.Path)
		if err != nil {
			return fmt.Errorf("failed to reopen temp file: %w", err)
		}
		tf.File = f
	}

	tf.IsSealed = true
	return nil
}

// ReleaseTempFile releases a temp file (decrements ref count, may delete)
func (tsm *TempStorageManager) ReleaseTempFile(id string) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	tf, exists := tsm.tempFiles[id]
	if !exists {
		return nil // Already gone
	}

	atomic.AddInt32(&tf.RefCount, -1)
	if tf.RefCount <= 0 && tf.IsSealed {
		tsm.deleteTempFileLocked(tf)
	}

	return nil
}

// DeleteTempFile immediately deletes a temp file
func (tsm *TempStorageManager) DeleteTempFile(id string) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	tf, exists := tsm.tempFiles[id]
	if !exists {
		return nil
	}

	return tsm.deleteTempFileLocked(tf)
}

// ListTempFiles returns stats for all temp files
func (tsm *TempStorageManager) ListTempFiles() []TempFileStats {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	stats := make([]TempFileStats, 0, len(tsm.tempFiles))
	for _, tf := range tsm.tempFiles {
		stats = append(stats, TempFileStats{
			ID:           tf.ID,
			Path:         tf.Path,
			Size:         tf.Size,
			CreatedAt:    tf.CreatedAt,
			ExpiresAt:    tf.ExpiresAt,
			Age:          time.Since(tf.CreatedAt),
			IsSealed:     tf.IsSealed,
			IsCompressed: tf.IsCompressed,
			RefCount:     tf.RefCount,
		})
	}
	return stats
}

// GetStats returns overall temp storage statistics
func (tsm *TempStorageManager) GetStats() TempStorageStats {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	var oldest, newest time.Time
	var expired, writers, readers int

	for _, tf := range tsm.tempFiles {
		if oldest.IsZero() || tf.CreatedAt.Before(oldest) {
			oldest = tf.CreatedAt
		}
		if newest.IsZero() || tf.CreatedAt.After(newest) {
			newest = tf.CreatedAt
		}
		if time.Now().After(tf.ExpiresAt) {
			expired++
		}
		if !tf.IsSealed {
			writers++
		} else {
			readers += int(tf.RefCount)
		}
	}

	return TempStorageStats{
		TotalFiles:    len(tsm.tempFiles),
		TotalBytes:    tsm.totalBytes,
		OldestFile:    oldest,
		NewestFile:    newest,
		ExpiredFiles:  expired,
		ActiveWriters: writers,
		ActiveReaders: readers,
	}
}

// CleanupExpired removes all expired temp files
func (tsm *TempStorageManager) CleanupExpired() int {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	return tsm.cleanupExpiredLocked()
}

// Close shuts down the temp storage manager and deletes all files
func (tsm *TempStorageManager) Close() error {
	close(tsm.stopCh)
	if tsm.cleanupTimer != nil {
		tsm.cleanupTimer.Stop()
	}

	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	// Delete all temp files
	for _, tf := range tsm.tempFiles {
		tsm.deleteTempFileLocked(tf)
	}

	return nil
}

// Internal methods

func (tsm *TempStorageManager) deleteTempFileLocked(tf *TempFile) error {
	if tf.File != nil {
		tf.File.Close()
		tf.File = nil
	}

	if err := os.Remove(tf.Path); err != nil && !os.IsNotExist(err) {
		return err
	}

	tsm.totalBytes -= tf.Size
	delete(tsm.tempFiles, tf.ID)
	return nil
}

func (tsm *TempStorageManager) cleanupExpiredLocked() int {
	now := time.Now()
	deleted := 0

	for _, tf := range tsm.tempFiles {
		if now.After(tf.ExpiresAt) && tf.RefCount <= 0 {
			tsm.deleteTempFileLocked(tf)
			deleted++
		}
	}

	return deleted
}

func (tsm *TempStorageManager) cleanupLoop() {
	for {
		select {
		case <-tsm.stopCh:
			return
		case <-tsm.cleanupTimer.C:
			tsm.CleanupExpired()
		}
	}
}

// TempFile methods for writing

// WriteDocument writes a single document to the temp file
func (tf *TempFile) WriteDocument(doc map[string]interface{}) error {
	if tf.IsSealed {
		return fmt.Errorf("temp file is sealed, no more writes allowed")
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	data = append(data, '\n')
	n, err := tf.File.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write document: %w", err)
	}

	tf.Size += int64(n)
	tf.WriteOffset += int64(n)
	return nil
}

// WriteDocuments writes multiple documents to the temp file
func (tf *TempFile) WriteDocuments(docs []map[string]interface{}) error {
	for _, doc := range docs {
		if err := tf.WriteDocument(doc); err != nil {
			return err
		}
	}
	return nil
}

// ReadDocument reads a single document from the temp file
func (tf *TempFile) ReadDocument() (map[string]interface{}, error) {
	if !tf.IsSealed {
		return nil, fmt.Errorf("temp file must be sealed before reading")
	}

	decoder := json.NewDecoder(tf.File)
	var doc map[string]interface{}
	if err := decoder.Decode(&doc); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to decode document: %w", err)
	}

	return doc, nil
}

// ReadAllDocuments reads all documents from the temp file
func (tf *TempFile) ReadAllDocuments() ([]map[string]interface{}, error) {
	if !tf.IsSealed {
		return nil, fmt.Errorf("temp file must be sealed before reading")
	}

	// Seek to beginning
	if _, err := tf.File.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	var docs []map[string]interface{}
	decoder := json.NewDecoder(tf.File)

	for {
		var doc map[string]interface{}
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode document: %w", err)
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

// StreamDocuments returns a channel for streaming documents
func (tf *TempFile) StreamDocuments() <-chan map[string]interface{} {
	ch := make(chan map[string]interface{}, 100)

	go func() {
		defer close(ch)

		// Seek to beginning
		if _, err := tf.File.Seek(0, 0); err != nil {
			return
		}

		decoder := json.NewDecoder(tf.File)
		for {
			var doc map[string]interface{}
			if err := decoder.Decode(&doc); err != nil {
				if err == io.EOF {
					return
				}
				return
			}
			ch <- doc
		}
	}()

	return ch
}

// AddRef increments the reference count
func (tf *TempFile) AddRef() {
	atomic.AddInt32(&tf.RefCount, 1)
}

// SetMetadata stores metadata for the temp file
func (tf *TempFile) SetMetadata(key string, value interface{}) {
	tf.Metadata[key] = value
}

// GetMetadata retrieves metadata from the temp file
func (tf *TempFile) GetMetadata(key string) (interface{}, bool) {
	v, ok := tf.Metadata[key]
	return v, ok
}

// ExternalMergeSort implements disk-based merge sort for large datasets
type ExternalMergeSort struct {
	tempManager *TempStorageManager
	memControl  *MemoryController
	config      ExternalMergeSortConfig
}

// ExternalMergeSortConfig holds configuration for external merge sort
type ExternalMergeSortConfig struct {
	// Maximum memory to use for in-memory sorting (default 100MB)
	MaxMemoryBytes int64

	// Number of runs to merge at once (default 10)
	MergeFanout int

	// Size of read buffer for each run during merge (default 1MB)
	ReadBufferSize int
}

// DefaultExternalMergeSortConfig returns sensible defaults
func DefaultExternalMergeSortConfig() ExternalMergeSortConfig {
	return ExternalMergeSortConfig{
		MaxMemoryBytes: 100 * 1024 * 1024, // 100MB
		MergeFanout:    10,
		ReadBufferSize: 1024 * 1024, // 1MB
	}
}

// NewExternalMergeSort creates a new external merge sorter
func NewExternalMergeSort(tempManager *TempStorageManager, memControl *MemoryController, config ExternalMergeSortConfig) *ExternalMergeSort {
	if config.MaxMemoryBytes <= 0 {
		config.MaxMemoryBytes = 100 * 1024 * 1024
	}
	if config.MergeFanout <= 0 {
		config.MergeFanout = 10
	}
	if config.ReadBufferSize <= 0 {
		config.ReadBufferSize = 1024 * 1024
	}

	return &ExternalMergeSort{
		tempManager: tempManager,
		memControl:  memControl,
		config:      config,
	}
}

// Sort performs external merge sort on documents
// compareFunc returns true if a should come before b
func (ems *ExternalMergeSort) Sort(documents []map[string]interface{}, compareFunc func(a, b map[string]interface{}) bool) ([]map[string]interface{}, error) {
	// If fits in memory, sort directly
	estimatedSize := estimateDocumentsSize(documents)
	if estimatedSize <= ems.config.MaxMemoryBytes {
		sort.Slice(documents, func(i, j int) bool {
			return compareFunc(documents[i], documents[j])
		})
		return documents, nil
	}

	// Need external merge sort
	return ems.externalSort(documents, compareFunc)
}

func (ems *ExternalMergeSort) externalSort(documents []map[string]interface{}, compareFunc func(a, b map[string]interface{}) bool) ([]map[string]interface{}, error) {
	// Phase 1: Create sorted runs
	runs, err := ems.createSortedRuns(documents, compareFunc)
	if err != nil {
		return nil, err
	}

	// Phase 2: Merge runs
	return ems.mergeRuns(runs, compareFunc)
}

func (ems *ExternalMergeSort) createSortedRuns(documents []map[string]interface{}, compareFunc func(a, b map[string]interface{}) bool) ([]*TempFile, error) {
	var runs []*TempFile
	chunk := make([]map[string]interface{}, 0)
	chunkSize := int64(0)

	for _, doc := range documents {
		docSize := estimateDocumentSize(doc)
		if chunkSize+docSize > ems.config.MaxMemoryBytes && len(chunk) > 0 {
			// Sort and write chunk
			run, err := ems.writeSortedRun(chunk, compareFunc)
			if err != nil {
				return nil, err
			}
			runs = append(runs, run)
			chunk = make([]map[string]interface{}, 0)
			chunkSize = 0
		}
		chunk = append(chunk, doc)
		chunkSize += docSize
	}

	// Write remaining
	if len(chunk) > 0 {
		run, err := ems.writeSortedRun(chunk, compareFunc)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}

	return runs, nil
}

func (ems *ExternalMergeSort) writeSortedRun(chunk []map[string]interface{}, compareFunc func(a, b map[string]interface{}) bool) (*TempFile, error) {
	// Sort in memory
	sort.Slice(chunk, func(i, j int) bool {
		return compareFunc(chunk[i], chunk[j])
	})

	// Write to temp file
	run, err := ems.tempManager.CreateTempFile("sort_run_", 30*time.Minute)
	if err != nil {
		return nil, err
	}

	if err := run.WriteDocuments(chunk); err != nil {
		run.File.Close()
		return nil, err
	}

	if err := ems.tempManager.SealTempFile(run.ID); err != nil {
		return nil, err
	}

	return run, nil
}

func (ems *ExternalMergeSort) mergeRuns(runs []*TempFile, compareFunc func(a, b map[string]interface{}) bool) ([]map[string]interface{}, error) {
	// If few enough runs, merge in one pass
	if len(runs) <= ems.config.MergeFanout {
		return ems.mergeSingleLevel(runs, compareFunc)
	}

	// Multi-level merge needed
	for len(runs) > 1 {
		var newRuns []*TempFile
		for i := 0; i < len(runs); i += ems.config.MergeFanout {
			end := i + ems.config.MergeFanout
			if end > len(runs) {
				end = len(runs)
			}

			merged, err := ems.mergeSingleLevel(runs[i:end], compareFunc)
			if err != nil {
				return nil, err
			}

			// Write merged result to new run
			newRun, err := ems.tempManager.CreateTempFile("sort_merged_", 30*time.Minute)
			if err != nil {
				return nil, err
			}

			if err := newRun.WriteDocuments(merged); err != nil {
				return nil, err
			}

			if err := ems.tempManager.SealTempFile(newRun.ID); err != nil {
				return nil, err
			}

			newRuns = append(newRuns, newRun)

			// Clean up old runs
			for j := i; j < end; j++ {
				ems.tempManager.DeleteTempFile(runs[j].ID)
			}
		}
		runs = newRuns
	}

	// Read final result
	if len(runs) == 0 {
		return nil, nil
	}
	return runs[0].ReadAllDocuments()
}

func (ems *ExternalMergeSort) mergeSingleLevel(runs []*TempFile, compareFunc func(a, b map[string]interface{}) bool) ([]map[string]interface{}, error) {
	if len(runs) == 0 {
		return nil, nil
	}
	if len(runs) == 1 {
		return runs[0].ReadAllDocuments()
	}

	// K-way merge using a heap-like approach
	type runReader struct {
		run     *TempFile
		current map[string]interface{}
		err     error
	}

	readers := make([]*runReader, 0, len(runs))
	for _, run := range runs {
		run.AddRef()
		r := &runReader{run: run}
		r.current, r.err = r.run.ReadDocument()
		readers = append(readers, r)
	}

	var result []map[string]interface{}

	for {
		// Find minimum among all readers
		var minIdx = -1
		for i, r := range readers {
			if r.err == io.EOF {
				continue
			}
			if r.err != nil {
				return nil, r.err
			}
			if minIdx < 0 || compareFunc(r.current, readers[minIdx].current) {
				minIdx = i
			}
		}

		if minIdx < 0 {
			break // All readers exhausted
		}

		result = append(result, readers[minIdx].current)
		readers[minIdx].current, readers[minIdx].err = readers[minIdx].run.ReadDocument()
	}

	// Release readers
	for _, r := range readers {
		ems.tempManager.ReleaseTempFile(r.run.ID)
	}

	return result, nil
}

// HashPartitioner implements hash partitioning for large $group operations
type HashPartitioner struct {
	tempManager *TempStorageManager
	memControl  *MemoryController
	config      HashPartitionerConfig
}

// HashPartitionerConfig holds configuration for hash partitioning
type HashPartitionerConfig struct {
	// Number of partitions (default 16)
	NumPartitions int

	// Maximum memory per partition before spilling (default 50MB)
	MaxMemoryPerPartition int64
}

// DefaultHashPartitionerConfig returns sensible defaults
func DefaultHashPartitionerConfig() HashPartitionerConfig {
	return HashPartitionerConfig{
		NumPartitions:         16,
		MaxMemoryPerPartition: 50 * 1024 * 1024, // 50MB
	}
}

// NewHashPartitioner creates a new hash partitioner
func NewHashPartitioner(tempManager *TempStorageManager, memControl *MemoryController, config HashPartitionerConfig) *HashPartitioner {
	if config.NumPartitions <= 0 {
		config.NumPartitions = 16
	}
	if config.MaxMemoryPerPartition <= 0 {
		config.MaxMemoryPerPartition = 50 * 1024 * 1024
	}

	return &HashPartitioner{
		tempManager: tempManager,
		memControl:  memControl,
		config:      config,
	}
}

// Partition partitions documents by hash key into separate temp files
// hashFunc returns a string key for partitioning
func (hp *HashPartitioner) Partition(documents []map[string]interface{}, hashFunc func(doc map[string]interface{}) string) ([]*TempFile, error) {
	// Create partition buffers
	partitions := make([][]map[string]interface{}, hp.config.NumPartitions)
	partitionSizes := make([]int64, hp.config.NumPartitions)
	partitionFiles := make([]*TempFile, hp.config.NumPartitions)

	// Distribute documents
	for _, doc := range documents {
		key := hashFunc(doc)
		partIdx := hp.hashKey(key) % hp.config.NumPartitions

		docSize := estimateDocumentSize(doc)
		if partitionSizes[partIdx]+docSize > hp.config.MaxMemoryPerPartition && len(partitions[partIdx]) > 0 {
			// Spill partition to disk
			if err := hp.spillPartition(partIdx, partitions, partitionFiles); err != nil {
				return nil, err
			}
			partitions[partIdx] = nil
			partitionSizes[partIdx] = 0
		}

		partitions[partIdx] = append(partitions[partIdx], doc)
		partitionSizes[partIdx] += docSize
	}

	// Finalize all partitions
	for i := range partitions {
		if len(partitions[i]) > 0 {
			if err := hp.spillPartition(i, partitions, partitionFiles); err != nil {
				return nil, err
			}
		}
		if partitionFiles[i] != nil {
			hp.tempManager.SealTempFile(partitionFiles[i].ID)
		}
	}

	// Filter out nil partitions
	result := make([]*TempFile, 0)
	for _, f := range partitionFiles {
		if f != nil {
			result = append(result, f)
		}
	}

	return result, nil
}

func (hp *HashPartitioner) spillPartition(idx int, partitions [][]map[string]interface{}, files []*TempFile) error {
	if len(partitions[idx]) == 0 {
		return nil
	}

	// Create or append to partition file
	if files[idx] == nil {
		f, err := hp.tempManager.CreateTempFile(fmt.Sprintf("partition_%d_", idx), 30*time.Minute)
		if err != nil {
			return err
		}
		files[idx] = f
	}

	return files[idx].WriteDocuments(partitions[idx])
}

func (hp *HashPartitioner) hashKey(key string) int {
	// Simple FNV-1a hash
	h := uint32(2166136261)
	for _, c := range key {
		h ^= uint32(c)
		h *= 16777619
	}
	return int(h)
}

// Helper functions

func estimateDocumentsSize(docs []map[string]interface{}) int64 {
	var total int64
	for _, doc := range docs {
		total += estimateDocumentSize(doc)
	}
	return total
}

func estimateDocumentSize(doc map[string]interface{}) int64 {
	// Rough estimate: ~100 bytes base + size of values
	size := int64(100)
	for k, v := range doc {
		size += int64(len(k))
		size += estimateValueSize(v)
	}
	return size
}

func estimateValueSize(v interface{}) int64 {
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case map[string]interface{}:
		return estimateDocumentSize(val)
	case []interface{}:
		var s int64
		for _, item := range val {
			s += estimateValueSize(item)
		}
		return s
	default:
		return 16 // Rough estimate for numbers, bools, etc.
	}
}

// TempCollection is a higher-level abstraction for temporary collections
type TempCollection struct {
	ID          string
	Name        string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	storage     Storage
	tempManager *TempStorageManager
	docCount    int64
}

// CreateTempCollection creates a temporary collection with a unique name
func (tsm *TempStorageManager) CreateTempCollection(namePrefix string, ttl time.Duration) (*TempCollection, error) {
	if ttl <= 0 {
		ttl = tsm.config.DefaultTTL
	}

	id := fmt.Sprintf("__temp_%s_%d", namePrefix, time.Now().UnixNano())
	memStorage := NewMemoryStorage()

	return &TempCollection{
		ID:          id,
		Name:        id,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(ttl),
		storage:     memStorage,
		tempManager: tsm,
	}, nil
}

// Insert adds a document to the temp collection
func (tc *TempCollection) Insert(doc *document.Document) error {
	err := tc.storage.Insert(doc)
	if err == nil {
		atomic.AddInt64(&tc.docCount, 1)
	}
	return err
}

// InsertMap adds a map as a document
func (tc *TempCollection) InsertMap(data map[string]interface{}) error {
	doc := &document.Document{
		ID:   fmt.Sprintf("%d", time.Now().UnixNano()),
		Data: data,
	}
	return tc.Insert(doc)
}

// FindAll returns all documents
func (tc *TempCollection) FindAll() ([]*document.Document, error) {
	return tc.storage.FindAll()
}

// Count returns the document count
func (tc *TempCollection) Count() int64 {
	return atomic.LoadInt64(&tc.docCount)
}

// Cursor returns a cursor for streaming
func (tc *TempCollection) Cursor() (Cursor, error) {
	return tc.storage.Cursor()
}

// Delete removes the temp collection
func (tc *TempCollection) Delete() error {
	if tc.tempManager != nil {
		return tc.tempManager.DeleteTempFile(tc.ID)
	}
	return nil
}

// IsExpired checks if the temp collection has expired
func (tc *TempCollection) IsExpired() bool {
	return time.Now().After(tc.ExpiresAt)
}

// Ensure SpillDir exists on temp storage config
func (c *TempStorageConfig) validate() error {
	if c.SpillDir == "" {
		c.SpillDir = "/tmp/aidb_spill"
	}
	return os.MkdirAll(c.SpillDir, 0755)
}

// Clean up temp files by pattern
func (tsm *TempStorageManager) CleanupByPattern(pattern string) int {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	deleted := 0
	for id, tf := range tsm.tempFiles {
		if strings.Contains(id, pattern) {
			tsm.deleteTempFileLocked(tf)
			deleted++
		}
	}
	return deleted
}