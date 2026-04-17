package pitr

import (
	"aidb/internal/wal"
	"fmt"
	"log"
	"sync"
	"time"
)

// CheckpointManager handles periodic and on-demand checkpoints for PITR and efficient recovery.
type CheckpointManager struct {
	wal         wal.WAL
	interval    time.Duration
	stopCh      chan struct{}
	mu          sync.Mutex
	lastLSN     uint64
	running     bool
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(w wal.WAL, interval time.Duration) *CheckpointManager {
	return &CheckpointManager{
		wal:      w,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the background checkpoint goroutine.
func (cm *CheckpointManager) Start() {
	cm.mu.Lock()
	if cm.running {
		cm.mu.Unlock()
		return
	}
	cm.running = true
	cm.mu.Unlock()

	go cm.run()
	log.Printf("[CheckpointManager] Started with interval %v", cm.interval)
}

// Stop stops the checkpoint manager.
func (cm *CheckpointManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.running {
		return
	}

	close(cm.stopCh)
	cm.running = false
	log.Println("[CheckpointManager] Stopped")
}

// CreateCheckpoint forces an immediate checkpoint.
func (cm *CheckpointManager) CreateCheckpoint() (uint64, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Write checkpoint record
	entry := wal.CreateLogEntry("", wal.LogEntryCheckpoint, "", nil, nil, nil)
	lsn, err := cm.wal.Append(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to write checkpoint: %w", err)
	}

	if err := cm.wal.Sync(); err != nil {
		return lsn, fmt.Errorf("failed to sync checkpoint: %w", err)
	}

	cm.lastLSN = lsn

	// Truncate old WAL segments (keep last 2 for safety)
	if err := cm.wal.Truncate(lsn - 1000); err != nil { // conservative truncation
		log.Printf("[Checkpoint] Truncate warning: %v", err)
	}

	log.Printf("[Checkpoint] Created at LSN %d", lsn)
	return lsn, nil
}

func (cm *CheckpointManager) run() {
	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := cm.CreateCheckpoint(); err != nil {
				log.Printf("[Checkpoint] Error: %v", err)
			}
		case <-cm.stopCh:
			return
		}
	}
}

// GetLastCheckpointLSN returns the LSN of the last successful checkpoint.
func (cm *CheckpointManager) GetLastCheckpointLSN() uint64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.lastLSN
}
