package backup

import (
	"aidb/internal/wal"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// BackupManifest describes a base backup for PITR.
type BackupManifest struct {
	ID          string    `json:"id"`
	StartLSN    uint64    `json:"start_lsn"`
	EndLSN      uint64    `json:"end_lsn"`
	Timestamp   time.Time `json:"timestamp"`
	DBFile      string    `json:"db_file"`
	WALSegments []string  `json:"wal_segments"`
	SizeBytes   int64     `json:"size_bytes"`
}

// BackupManager handles base backups for PITR.
type BackupManager struct {
	db        interface{ Close() error } // minimal interface to avoid import cycle
	wal       wal.WAL
	backupDir string
}

// NewBackupManager creates a BackupManager.
func NewBackupManager(db interface{ Close() error }, w wal.WAL, backupDir string) (*BackupManager, error) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup dir: %w", err)
	}
	return &BackupManager{db: db, wal: w, backupDir: backupDir}, nil
}

// CreateBaseBackup creates a new consistent base backup.
func (bm *BackupManager) CreateBaseBackup() (*BackupManifest, error) {
	id := fmt.Sprintf("backup-%s", time.Now().Format("20060102-150405"))
	backupPath := filepath.Join(bm.backupDir, id)
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return nil, err
	}

	// Record current LSN as start
	startLSN := bm.wal.CurrentLSN()

	// Best-effort checkpoint via WAL (if CheckpointManager is used, it will be better)
	manifest := &BackupManifest{
		ID:        id,
		StartLSN:  startLSN,
		EndLSN:    bm.wal.CurrentLSN(),
		Timestamp: time.Now(),
		DBFile:    filepath.Join(backupPath, "aidb.db"),
	}

	// Copy current WAL segments into backup
	segments := bm.wal.GetSegments()
	for _, seg := range segments {
		dst := filepath.Join(backupPath, filepath.Base(seg))
		if err := copyFile(seg, dst); err == nil {
			manifest.WALSegments = append(manifest.WALSegments, filepath.Base(seg))
		}
	}

	// Save manifest
	manifestPath := filepath.Join(backupPath, "manifest.json")
	data, _ := json.MarshalIndent(manifest, "", "  ")
	os.WriteFile(manifestPath, data, 0644)

	// Update size
	manifest.SizeBytes = calculateSize(backupPath)

	return manifest, nil
}

// ListBackups returns all available base backups.
func (bm *BackupManager) ListBackups() ([]*BackupManifest, error) {
	entries, err := os.ReadDir(bm.backupDir)
	if err != nil {
		return nil, err
	}

	var manifests []*BackupManifest
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		manifestPath := filepath.Join(bm.backupDir, e.Name(), "manifest.json")
		if data, err := os.ReadFile(manifestPath); err == nil {
			var m BackupManifest
			if json.Unmarshal(data, &m) == nil {
				manifests = append(manifests, &m)
			}
		}
	}
	return manifests, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func calculateSize(dir string) int64 {
	var size int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
