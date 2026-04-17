package pitr

import (
	"aidb/internal/wal"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALArchiver continuously archives completed WAL segments for PITR.
type WALArchiver struct {
	wal         wal.WAL
	archiveDir  string
	interval    time.Duration
	stopCh      chan struct{}
	mu          sync.Mutex
	running     bool
	archived    map[string]bool // track already archived segments
}

// NewWALArchiver creates a new WAL archiver.
func NewWALArchiver(w wal.WAL, archiveDir string, interval time.Duration) (*WALArchiver, error) {
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create archive dir: %w", err)
	}

	return &WALArchiver{
		wal:        w,
		archiveDir: archiveDir,
		interval:   interval,
		stopCh:     make(chan struct{}),
		archived:   make(map[string]bool),
	}, nil
}

// Start begins background archiving.
func (a *WALArchiver) Start() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.mu.Unlock()

	go a.run()
	log.Printf("[WALArchiver] Started, archiving to %s", a.archiveDir)
}

// Stop stops the archiver.
func (a *WALArchiver) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.running {
		return
	}
	close(a.stopCh)
	a.running = false
	log.Println("[WALArchiver] Stopped")
}

func (a *WALArchiver) run() {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.archiveNewSegments()
		case <-a.stopCh:
			return
		}
	}
}

func (a *WALArchiver) archiveNewSegments() {
	segments := a.wal.GetSegments()
	for _, seg := range segments {
		a.mu.Lock()
		if a.archived[seg] {
			a.mu.Unlock()
			continue
		}
		a.mu.Unlock()

		if err := a.archiveSegment(seg); err != nil {
			log.Printf("[WALArchiver] Failed to archive %s: %v", seg, err)
			continue
		}

		a.mu.Lock()
		a.archived[seg] = true
		a.mu.Unlock()
	}
}

func (a *WALArchiver) archiveSegment(srcPath string) error {
	// Only archive segments that are not the current active one (simple heuristic)
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}

	// Skip very recent files (still being written)
	if time.Since(info.ModTime()) < 2*time.Second {
		return nil
	}

	dstPath := filepath.Join(a.archiveDir, filepath.Base(srcPath))

	// Copy file
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}

	if err := dst.Sync(); err != nil {
		return err
	}

	log.Printf("[WALArchiver] Archived segment: %s", filepath.Base(srcPath))
	return nil
}

// IsArchived returns whether a segment has been archived.
func (a *WALArchiver) IsArchived(path string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.archived[path]
}
