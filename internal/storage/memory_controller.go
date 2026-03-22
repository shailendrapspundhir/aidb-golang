package storage

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// MemoryController provides centralized memory management for the database.
// It prevents OOM by tracking memory usage, providing reservation system,
// and coordinating spill-to-disk when memory pressure is high.
type MemoryController struct {
	mu sync.RWMutex

	// Configuration
	config MemoryControllerConfig

	// State
	usedBytes       int64 // Total memory currently in use
	reservedBytes   int64 // Memory reserved but not yet used
	peakUsedBytes   int64 // Peak memory usage for monitoring
	spillCount      int64 // Number of times spill was triggered
	reservationCount int64 // Total reservations made
	rejectionCount  int64 // Reservations rejected due to memory pressure

	// Callbacks for memory pressure events
	onPressureHigh     []func()
	onPressureCritical []func()

	// Shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// MemoryControllerConfig holds configuration for the memory controller
type MemoryControllerConfig struct {
	// Total memory limit in bytes (0 = auto-detect from system)
	TotalMemoryBytes int64

	// Safety margin - keep this much memory free (default 10%)
	SafetyMarginPercent int

	// Threshold for "high" memory pressure (default 70%)
	HighPressurePercent int

	// Threshold for "critical" memory pressure (default 90%)
	CriticalPressurePercent int

	// Minimum reservation size (smaller requests are batched)
	MinReservationBytes int64

	// Enable forced GC when under memory pressure
	EnableForcedGC bool

	// GC trigger threshold (percent of total, default 85%)
	GCTriggerPercent int

	// Spill directory for temporary files
	SpillDir string
}

// DefaultMemoryControllerConfig returns sensible defaults
func DefaultMemoryControllerConfig() MemoryControllerConfig {
	totalMem := getSystemMemoryBytes()
	return MemoryControllerConfig{
		TotalMemoryBytes:        totalMem,
		SafetyMarginPercent:     10,
		HighPressurePercent:     70,
		CriticalPressurePercent: 90,
		MinReservationBytes:     1024 * 1024, // 1MB minimum
		EnableForcedGC:          true,
		GCTriggerPercent:        85,
		SpillDir:                "/tmp/aidb_spill",
	}
}

// MemoryReservation represents a reserved memory block
type MemoryReservation struct {
	controller *MemoryController
	bytes      int64
	used       int64
	released   bool
	id         int64
}

// MemoryStats holds current memory statistics
type MemoryStats struct {
	TotalBytes         int64
	UsedBytes          int64
	ReservedBytes      int64
	AvailableBytes     int64
	PeakUsedBytes      int64
	UtilizationPercent float64
	PressureLevel      MemoryPressureLevel
	SpillCount         int64
	ReservationCount   int64
	RejectionCount     int64
}

// MemoryPressureLevel indicates current memory pressure
type MemoryPressureLevel int

const (
	PressureNormal MemoryPressureLevel = iota
	PressureHigh
	PressureCritical
)

func (l MemoryPressureLevel) String() string {
	switch l {
	case PressureNormal:
		return "normal"
	case PressureHigh:
		return "high"
	case PressureCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// NewMemoryController creates a new memory controller
func NewMemoryController(config MemoryControllerConfig) *MemoryController {
	// Auto-detect total memory if not specified
	if config.TotalMemoryBytes <= 0 {
		config.TotalMemoryBytes = getSystemMemoryBytes()
	}

	// Set defaults for missing values
	if config.SafetyMarginPercent <= 0 {
		config.SafetyMarginPercent = 10
	}
	if config.HighPressurePercent <= 0 {
		config.HighPressurePercent = 70
	}
	if config.CriticalPressurePercent <= 0 {
		config.CriticalPressurePercent = 90
	}
	if config.MinReservationBytes <= 0 {
		config.MinReservationBytes = 1024 * 1024 // 1MB
	}
	if config.GCTriggerPercent <= 0 {
		config.GCTriggerPercent = 85
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryController{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Reserve attempts to reserve memory bytes. Returns a reservation or an error
// if memory pressure is too high. The reservation must be Released when done.
func (mc *MemoryController) Reserve(bytes int64) (*MemoryReservation, error) {
	if bytes <= 0 {
		return &MemoryReservation{controller: mc, bytes: 0, id: atomic.AddInt64(&mc.reservationCount, 1)}, nil
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Calculate effective limit (total - safety margin)
	safetyBytes := mc.config.TotalMemoryBytes * int64(mc.config.SafetyMarginPercent) / 100
	effectiveLimit := mc.config.TotalMemoryBytes - safetyBytes

	// Check if reservation would exceed limit
	projectedUsage := mc.usedBytes + mc.reservedBytes + bytes
	pressureLevel := mc.getPressureLevelLocked(projectedUsage)

	// Reject if critical pressure (unless request is tiny)
	if pressureLevel == PressureCritical && bytes > mc.config.MinReservationBytes {
		atomic.AddInt64(&mc.rejectionCount, 1)
		return nil, fmt.Errorf("memory reservation rejected: critical pressure (used=%d, requested=%d, limit=%d)",
			mc.usedBytes+mc.reservedBytes, bytes, effectiveLimit)
	}

	// Trigger GC if needed and enabled
	if mc.config.EnableForcedGC {
		utilization := float64(projectedUsage) / float64(mc.config.TotalMemoryBytes) * 100
		if utilization >= float64(mc.config.GCTriggerPercent) {
			runtime.GC()
		}
	}

	// Update state
	mc.reservedBytes += bytes
	atomic.AddInt64(&mc.reservationCount, 1)

	// Update peak tracking
	currentTotal := mc.usedBytes + mc.reservedBytes
	if currentTotal > mc.peakUsedBytes {
		mc.peakUsedBytes = currentTotal
	}

	// Notify pressure callbacks
	mc.notifyPressureLocked(pressureLevel)

	return &MemoryReservation{
		controller: mc,
		bytes:      bytes,
		id:         atomic.LoadInt64(&mc.reservationCount),
	}, nil
}

// TryReserve attempts to reserve memory but returns nil instead of error on failure
func (mc *MemoryController) TryReserve(bytes int64) *MemoryReservation {
	res, err := mc.Reserve(bytes)
	if err != nil {
		return nil
	}
	return res
}

// Use marks bytes as actually used (not just reserved)
func (mc *MemoryController) Use(bytes int64) {
	if bytes <= 0 {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Move from reserved to used
	if mc.reservedBytes >= bytes {
		mc.reservedBytes -= bytes
		mc.usedBytes += bytes
	} else {
		// More used than reserved - just add to used
		mc.usedBytes += bytes
	}

	// Update peak
	currentTotal := mc.usedBytes + mc.reservedBytes
	if currentTotal > mc.peakUsedBytes {
		mc.peakUsedBytes = currentTotal
	}
}

// Release releases a memory reservation
func (mc *MemoryController) Release(reservation *MemoryReservation) {
	if reservation == nil || reservation.released {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Subtract from both reserved and used
	if mc.reservedBytes >= reservation.bytes {
		mc.reservedBytes -= reservation.bytes
	}
	if mc.usedBytes >= reservation.bytes {
		mc.usedBytes -= reservation.bytes
	}

	reservation.released = true
}

// ReleaseBytes releases a specific amount of memory
func (mc *MemoryController) ReleaseBytes(bytes int64) {
	if bytes <= 0 {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.usedBytes >= bytes {
		mc.usedBytes -= bytes
	} else {
		mc.usedBytes = 0
	}
}

// GetStats returns current memory statistics
func (mc *MemoryController) GetStats() MemoryStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	totalUsed := mc.usedBytes + mc.reservedBytes
	safetyBytes := mc.config.TotalMemoryBytes * int64(mc.config.SafetyMarginPercent) / 100
	available := mc.config.TotalMemoryBytes - safetyBytes - totalUsed
	if available < 0 {
		available = 0
	}

	utilization := float64(totalUsed) / float64(mc.config.TotalMemoryBytes) * 100

	return MemoryStats{
		TotalBytes:         mc.config.TotalMemoryBytes,
		UsedBytes:          mc.usedBytes,
		ReservedBytes:      mc.reservedBytes,
		AvailableBytes:     available,
		PeakUsedBytes:      mc.peakUsedBytes,
		UtilizationPercent: utilization,
		PressureLevel:      mc.getPressureLevelLocked(totalUsed),
		SpillCount:         atomic.LoadInt64(&mc.spillCount),
		ReservationCount:   atomic.LoadInt64(&mc.reservationCount),
		RejectionCount:     atomic.LoadInt64(&mc.rejectionCount),
	}
}

// GetPressureLevel returns the current memory pressure level
func (mc *MemoryController) GetPressureLevel() MemoryPressureLevel {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.getPressureLevelLocked(mc.usedBytes + mc.reservedBytes)
}

// ShouldSpill returns true if the controller recommends spilling to disk
func (mc *MemoryController) ShouldSpill() bool {
	return mc.GetPressureLevel() >= PressureHigh
}

// OnPressureHigh registers a callback for high pressure events
func (mc *MemoryController) OnPressureHigh(callback func()) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.onPressureHigh = append(mc.onPressureHigh, callback)
}

// OnPressureCritical registers a callback for critical pressure events
func (mc *MemoryController) OnPressureCritical(callback func()) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.onPressureCritical = append(mc.onPressureCritical, callback)
}

// RecordSpill records that a spill to disk occurred
func (mc *MemoryController) RecordSpill() {
	atomic.AddInt64(&mc.spillCount, 1)
}

// Close shuts down the memory controller
func (mc *MemoryController) Close() error {
	mc.cancel()
	return nil
}

// Internal methods

func (mc *MemoryController) getPressureLevelLocked(usedBytes int64) MemoryPressureLevel {
	utilization := float64(usedBytes) / float64(mc.config.TotalMemoryBytes) * 100

	if utilization >= float64(mc.config.CriticalPressurePercent) {
		return PressureCritical
	}
	if utilization >= float64(mc.config.HighPressurePercent) {
		return PressureHigh
	}
	return PressureNormal
}

func (mc *MemoryController) notifyPressureLocked(level MemoryPressureLevel) {
	// Notify callbacks without holding lock
	var callbacks []func()
	switch level {
	case PressureCritical:
		callbacks = append(callbacks, mc.onPressureCritical...)
		fallthrough
	case PressureHigh:
		callbacks = append(callbacks, mc.onPressureHigh...)
	}

	// Call callbacks in goroutines to avoid deadlock
	for _, cb := range callbacks {
		go cb()
	}
}

// getSystemMemoryBytes attempts to detect total system memory
func getSystemMemoryBytes() int64 {
	// Default to 4GB if we can't detect
	defaultMem := int64(4 * 1024 * 1024 * 1024)

	// Use runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// If Sys seems reasonable, use it; otherwise default
	if m.Sys > 0 && m.Sys < 1<<40 { // Less than 1TB
		return int64(m.Sys)
	}

	return defaultMem
}

// MemoryReservation methods

// Use marks bytes as used within this reservation
func (r *MemoryReservation) Use(bytes int64) {
	if r == nil || r.released || bytes <= 0 {
		return
	}
	if bytes > r.bytes-r.used {
		bytes = r.bytes - r.used
	}
	r.used += bytes
	r.controller.Use(bytes)
}

// Release releases the reservation
func (r *MemoryReservation) Release() {
	if r != nil && r.controller != nil {
		r.controller.Release(r)
	}
}

// Bytes returns the total reserved bytes
func (r *MemoryReservation) Bytes() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

// Used returns the bytes actually used
func (r *MemoryReservation) Used() int64 {
	if r == nil {
		return 0
	}
	return r.used
}

// Remaining returns the bytes remaining in the reservation
func (r *MemoryReservation) Remaining() int64 {
	if r == nil {
		return 0
	}
	return r.bytes - r.used
}