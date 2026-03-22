package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aidb/internal/document"
)

// AdaptiveBatchCursor provides intelligent batch fetching with automatic
// batch size adjustment. It starts with a default batch size, increases on
// success, and decreases on failure (timeout, memory pressure, etc.).
type AdaptiveBatchCursor struct {
	storage    Storage
	memControl *MemoryController
	ctx        context.Context
	cancel     context.CancelFunc

	// Configuration
	config AdaptiveCursorConfig

	// State
	mu              sync.Mutex
	currentBatch    []*document.Document
	batchIndex      int
	offset          int
	batchSize       int
	consecSuccesses int
	consecFailures  int
	exhausted       bool
	err             error

	// Statistics
	stats CursorStats
}

// AdaptiveCursorConfig holds configuration for adaptive cursor
type AdaptiveCursorConfig struct {
	// Initial batch size (default 1000)
	InitialBatchSize int

	// Minimum batch size (default 1, cursor mode)
	MinBatchSize int

	// Maximum batch size (default 50000)
	MaxBatchSize int

	// Increase batch by this factor on success (default 1.5)
	GrowthFactor float64

	// Decrease batch by this factor on failure (default 0.5)
	ShrinkFactor float64

	// Number of consecutive successes before increasing (default 3)
	SuccessesBeforeGrow int

	// Number of consecutive failures before giving up (default 5)
	MaxConsecutiveFailures int

	// Timeout for each batch fetch (default 30s)
	BatchTimeout time.Duration

	// Memory check interval (default: check every batch)
	MemoryCheckInterval int
}

// DefaultAdaptiveCursorConfig returns sensible defaults
func DefaultAdaptiveCursorConfig() AdaptiveCursorConfig {
	return AdaptiveCursorConfig{
		InitialBatchSize:       1000,
		MinBatchSize:           1,
		MaxBatchSize:           50000,
		GrowthFactor:           1.5,
		ShrinkFactor:           0.5,
		SuccessesBeforeGrow:    3,
		MaxConsecutiveFailures: 5,
		BatchTimeout:           30 * time.Second,
		MemoryCheckInterval:    1,
	}
}

// CursorStats holds statistics about cursor operation
type CursorStats struct {
	TotalDocuments   int64
	TotalBatches     int64
	CurrentBatchSize int
	AvgBatchSize     float64
	MaxBatchSizeUsed int
	MinBatchSizeUsed int
	ShrinkEvents     int64
	GrowEvents       int64
	TimeoutEvents    int64
	MemoryEvents     int64
}

// ErrBatchTimeout is returned when a batch fetch times out
var ErrBatchTimeout = errors.New("batch fetch timeout")

// ErrMemoryPressure is returned when memory pressure prevents fetching
var ErrMemoryPressure = errors.New("memory pressure too high")

// ErrCursorExhausted is returned when no more documents are available
var ErrCursorExhausted = errors.New("cursor exhausted")

// NewAdaptiveBatchCursor creates a new adaptive batch cursor
func NewAdaptiveBatchCursor(storage Storage, memControl *MemoryController, config AdaptiveCursorConfig) *AdaptiveBatchCursor {
	// Apply defaults for missing values
	if config.InitialBatchSize <= 0 {
		config.InitialBatchSize = 1000
	}
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = 1
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 50000
	}
	if config.GrowthFactor <= 1 {
		config.GrowthFactor = 1.5
	}
	if config.ShrinkFactor <= 0 || config.ShrinkFactor >= 1 {
		config.ShrinkFactor = 0.5
	}
	if config.SuccessesBeforeGrow <= 0 {
		config.SuccessesBeforeGrow = 3
	}
	if config.MaxConsecutiveFailures <= 0 {
		config.MaxConsecutiveFailures = 5
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 30 * time.Second
	}

	// Ensure initial is within bounds
	if config.InitialBatchSize > config.MaxBatchSize {
		config.InitialBatchSize = config.MaxBatchSize
	}
	if config.InitialBatchSize < config.MinBatchSize {
		config.InitialBatchSize = config.MinBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &AdaptiveBatchCursor{
		storage:    storage,
		memControl: memControl,
		config:     config,
		batchSize:  config.InitialBatchSize,
		ctx:        ctx,
		cancel:     cancel,
		stats: CursorStats{
			CurrentBatchSize: config.InitialBatchSize,
			MaxBatchSizeUsed: config.InitialBatchSize,
			MinBatchSizeUsed: config.InitialBatchSize,
		},
	}
}

// Next advances to the next document. Returns false when done or on error.
func (c *AdaptiveBatchCursor) Next() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exhausted or errored
	if c.exhausted || c.err != nil {
		return false
	}

	// Check context cancellation
	select {
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
		return false
	default:
	}

	// If we have documents in current batch, return next
	if c.batchIndex < len(c.currentBatch) {
		return true
	}

	// Need to fetch next batch
	if err := c.fetchNextBatch(); err != nil {
		if errors.Is(err, ErrCursorExhausted) {
			c.exhausted = true
			return false
		}
		c.err = err
		return false
	}

	// Check if we got any documents
	if len(c.currentBatch) == 0 {
		c.exhausted = true
		return false
	}

	c.batchIndex = 0
	return true
}

// Current returns the current document
func (c *AdaptiveBatchCursor) Current() *document.Document {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.batchIndex < len(c.currentBatch) && c.currentBatch != nil {
		return c.currentBatch[c.batchIndex]
	}
	return nil
}

// Advance moves to the next document and returns it (convenience method)
func (c *AdaptiveBatchCursor) Advance() *document.Document {
	if c.Next() {
		doc := c.Current()
		c.batchIndex++
		return doc
	}
	return nil
}

// Err returns any error encountered
func (c *AdaptiveBatchCursor) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close releases resources
func (c *AdaptiveBatchCursor) Close() error {
	c.cancel()
	c.mu.Lock()
	c.currentBatch = nil
	c.mu.Unlock()
	return nil
}

// GetStats returns cursor statistics
func (c *AdaptiveBatchCursor) GetStats() CursorStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.CurrentBatchSize = c.batchSize
	return c.stats
}

// Cancel cancels the cursor operation
func (c *AdaptiveBatchCursor) Cancel() {
	c.cancel()
}

// fetchNextBatch fetches the next batch with adaptive sizing
func (c *AdaptiveBatchCursor) fetchNextBatch() error {
	for {
		// Check context
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		// Check memory pressure if controller is available
		if c.memControl != nil && c.stats.TotalBatches%int64(maxInt(1, c.config.MemoryCheckInterval)) == 0 {
			if c.memControl.ShouldSpill() {
				atomic.AddInt64(&c.stats.MemoryEvents, 1)
				// Reduce batch size due to memory pressure
				c.shrinkBatchSize()
				c.consecFailures++
				if c.consecFailures >= c.config.MaxConsecutiveFailures {
					return ErrMemoryPressure
				}
				continue
			}
		}

		// Try to fetch batch with timeout
		batch, err := c.fetchWithTimeout()

		if err != nil {
			c.consecFailures++
			c.consecSuccesses = 0

			// Check if we should retry with smaller batch
			if c.shouldRetry(err) {
				c.shrinkBatchSize()

				if c.consecFailures >= c.config.MaxConsecutiveFailures {
					return fmt.Errorf("max consecutive failures reached: %w", err)
				}
				continue
			}

			return err
		}

		// Success - reset failure counter and maybe grow
		c.consecFailures = 0
		c.consecSuccesses++
		c.currentBatch = batch
		c.offset += len(batch)
		c.batchIndex = 0
		atomic.AddInt64(&c.stats.TotalBatches, 1)
		atomic.AddInt64(&c.stats.TotalDocuments, int64(len(batch)))

		// Update batch size stats
		if c.batchSize > c.stats.MaxBatchSizeUsed {
			c.stats.MaxBatchSizeUsed = c.batchSize
		}
		if c.batchSize < c.stats.MinBatchSizeUsed || c.stats.MinBatchSizeUsed == 0 {
			c.stats.MinBatchSizeUsed = c.batchSize
		}

		// Update average batch size
		totalBatches := atomic.LoadInt64(&c.stats.TotalBatches)
		c.stats.AvgBatchSize = float64(atomic.LoadInt64(&c.stats.TotalDocuments)) / float64(totalBatches)

		// Grow batch size if consistently successful
		if c.consecSuccesses >= c.config.SuccessesBeforeGrow {
			c.growBatchSize()
			c.consecSuccesses = 0
		}

		// If batch is smaller than requested, we're at the end
		if len(batch) < c.batchSize {
			return nil
		}

		return nil
	}
}

// fetchWithTimeout fetches a batch with timeout
func (c *AdaptiveBatchCursor) fetchWithTimeout() ([]*document.Document, error) {
	// Create timeout context
	ctx, cancel := context.WithTimeout(c.ctx, c.config.BatchTimeout)
	defer cancel()

	// Channel for result
	type result struct {
		docs []*document.Document
		err  error
	}
	resultCh := make(chan result, 1)

	// Fetch in goroutine
	go func() {
		docs, err := c.storage.FindAll() // For now, use FindAll; will optimize later
		select {
		case resultCh <- result{docs: docs, err: err}:
		case <-ctx.Done():
		}
	}()

	// Wait for result or timeout
	select {
	case res := <-resultCh:
		if res.err != nil {
			return nil, res.err
		}
		// Apply offset and limit
		start := c.offset
		if start >= len(res.docs) {
			return []*document.Document{}, nil
		}
		end := start + c.batchSize
		if end > len(res.docs) {
			end = len(res.docs)
		}
		return res.docs[start:end], nil
	case <-ctx.Done():
		atomic.AddInt64(&c.stats.TimeoutEvents, 1)
		return nil, ErrBatchTimeout
	}
}

// shouldRetry determines if we should retry with a smaller batch
func (c *AdaptiveBatchCursor) shouldRetry(err error) bool {
	// Retry on timeout or memory pressure
	if errors.Is(err, ErrBatchTimeout) || errors.Is(err, ErrMemoryPressure) {
		return c.batchSize > c.config.MinBatchSize
	}
	return false
}

// shrinkBatchSize reduces the batch size
func (c *AdaptiveBatchCursor) shrinkBatchSize() {
	newSize := int(float64(c.batchSize) * c.config.ShrinkFactor)
	if newSize < c.config.MinBatchSize {
		newSize = c.config.MinBatchSize
	}
	if newSize != c.batchSize {
		c.batchSize = newSize
		atomic.AddInt64(&c.stats.ShrinkEvents, 1)
	}
}

// growBatchSize increases the batch size
func (c *AdaptiveBatchCursor) growBatchSize() {
	newSize := int(float64(c.batchSize) * c.config.GrowthFactor)
	if newSize > c.config.MaxBatchSize {
		newSize = c.config.MaxBatchSize
	}
	if newSize != c.batchSize {
		c.batchSize = newSize
		atomic.AddInt64(&c.stats.GrowEvents, 1)
	}
}

// BatchCursorAdapter wraps AdaptiveBatchCursor to implement the Cursor interface
type BatchCursorAdapter struct {
	adaptive *AdaptiveBatchCursor
	current  *document.Document
}

// NewBatchCursorAdapter creates a Cursor-compatible adapter
func NewBatchCursorAdapter(storage Storage, memControl *MemoryController, config AdaptiveCursorConfig) Cursor {
	return &BatchCursorAdapter{
		adaptive: NewAdaptiveBatchCursor(storage, memControl, config),
	}
}

func (a *BatchCursorAdapter) Next() bool {
	if a.adaptive.Next() {
		a.current = a.adaptive.Current()
		a.adaptive.batchIndex++ // Advance internal pointer
		return true
	}
	a.current = nil
	return false
}

func (a *BatchCursorAdapter) Current() *document.Document {
	return a.current
}

func (a *BatchCursorAdapter) Err() error {
	return a.adaptive.Err()
}

func (a *BatchCursorAdapter) Close() error {
	return a.adaptive.Close()
}

// Helper function for maxInt (Go 1.21 has max built-in, but for compatibility)
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}