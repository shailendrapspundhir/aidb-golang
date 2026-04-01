package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FileWAL implements WAL using append-only segment files
type FileWAL struct {
	mu           sync.RWMutex
	config       *Config
	currentFile  *os.File
	writer       *bufio.Writer
	currentLSN   uint64
	firstLSN     uint64
	currentSize  int64
	segmentNum   int
	segments     []string // List of segment file paths
}

// segmentFilePrefix is the prefix for WAL segment files
const segmentFilePrefix = "wal-"
const segmentFileSuffix = ".log"

// NewFileWAL creates a new file-based WAL
func NewFileWAL(config *Config) (*FileWAL, error) {
	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &FileWAL{
		config:   config,
		segments: make([]string, 0),
	}

	// Discover existing segments
	if err := wal.discoverSegments(); err != nil {
		return nil, fmt.Errorf("failed to discover segments: %w", err)
	}

	// Open or create current segment
	if err := wal.openCurrentSegment(); err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}

	return wal, nil
}

// discoverSegments finds existing WAL segment files
func (w *FileWAL) discoverSegments() error {
	entries, err := os.ReadDir(w.config.Directory)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, segmentFilePrefix) && strings.HasSuffix(name, segmentFileSuffix) {
			path := filepath.Join(w.config.Directory, name)
			w.segments = append(w.segments, path)
		}
	}

	// Sort segments by number
	sort.Strings(w.segments)

	// Find highest LSN from existing segments
	if len(w.segments) > 0 {
		lastSegment := w.segments[len(w.segments)-1]
		lsn, err := w.getMaxLSNFromSegment(lastSegment)
		if err != nil {
			return err
		}
		w.currentLSN = lsn
		w.segmentNum = len(w.segments)
	}

	return nil
}

// getMaxLSNFromSegment reads a segment and finds the maximum LSN
func (w *FileWAL) getMaxLSNFromSegment(path string) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var maxLSN uint64
	reader := bufio.NewReader(file)

	for {
		// Read length prefix
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return maxLSN, nil // Partial entry at end is ok
		}

		entryLen := binary.BigEndian.Uint32(lenBuf)

		// Read entry data
		entryBuf := make([]byte, entryLen)
		if _, err := io.ReadFull(reader, entryBuf); err != nil {
			// Partial/corrupted entry
			break
		}

		// Decode just the header to get LSN
		if len(entryBuf) >= 9 {
			lsn := binary.BigEndian.Uint64(entryBuf[1:9])
			if lsn > maxLSN {
				maxLSN = lsn
			}
		}
	}

	return maxLSN, nil
}

// openCurrentSegment opens or creates the current segment file
func (w *FileWAL) openCurrentSegment() error {
	// Close existing file if any
	if w.currentFile != nil {
		w.writer.Flush()
		w.currentFile.Sync()
		w.currentFile.Close()
	}

	// Determine segment filename
	var path string
	if w.segmentNum == 0 && len(w.segments) == 0 {
		// First segment
		path = w.segmentPath(0)
		w.firstLSN = 1
	} else if len(w.segments) > 0 {
		// Reuse last segment if not full
		lastPath := w.segments[len(w.segments)-1]
		info, err := os.Stat(lastPath)
		if err == nil && info.Size() < w.config.SegmentSize {
			path = lastPath
			// Remove from segments list, will be re-added
			w.segments = w.segments[:len(w.segments)-1]
		} else {
			w.segmentNum++
			path = w.segmentPath(w.segmentNum)
		}
	} else {
		w.segmentNum++
		path = w.segmentPath(w.segmentNum)
	}

	// Open file (append mode)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.currentFile = file
	w.writer = bufio.NewWriter(file)
	w.segments = append(w.segments, path)

	// Get current size
	info, err := file.Stat()
	if err == nil {
		w.currentSize = info.Size()
	}

	return nil
}

// segmentPath returns the path for a segment number
func (w *FileWAL) segmentPath(num int) string {
	return filepath.Join(w.config.Directory, fmt.Sprintf("%s%06d%s", segmentFilePrefix, num, segmentFileSuffix))
}

// parseSegmentNumber extracts segment number from filename
func parseSegmentNumber(name string) int {
	name = strings.TrimPrefix(name, segmentFilePrefix)
	name = strings.TrimSuffix(name, segmentFileSuffix)
	num, _ := strconv.Atoi(name)
	return num
}

// Append writes a log entry and returns the assigned LSN
func (w *FileWAL) Append(entry *LogEntry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Assign LSN
	lsn := atomic.AddUint64(&w.currentLSN, 1)
	entry.LSN = lsn
	entry.Timestamp = time.Now().UnixNano()

	// Encode entry
	data := entry.Encode()

	// Check if we need to rotate
	if w.currentSize+int64(len(data)) > w.config.SegmentSize {
		if err := w.rotateSegment(); err != nil {
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	// Write entry
	if _, err := w.writer.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	w.currentSize += int64(len(data))

	// Sync based on policy
	if w.config.SyncPolicy == SyncOnEveryWrite {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}

	return lsn, nil
}

// rotateSegment creates a new segment file
func (w *FileWAL) rotateSegment() error {
	// Flush and close current
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.currentFile != nil {
		w.currentFile.Sync()
		w.currentFile.Close()
	}

	// Clean up old segments if needed
	if len(w.segments) >= w.config.MaxSegments {
		if err := w.cleanupOldSegments(); err != nil {
			return err
		}
	}

	// Create new segment
	w.segmentNum++
	path := w.segmentPath(w.segmentNum)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.currentFile = file
	w.writer = bufio.NewWriter(file)
	w.segments = append(w.segments, path)
	w.currentSize = 0

	return nil
}

// cleanupOldSegments removes oldest segments
func (w *FileWAL) cleanupOldSegments() error {
	// Keep at least 2 segments for recovery
	if len(w.segments) <= 2 {
		return nil
	}

	// Remove oldest segments (keep newest MaxSegments-1, plus current)
	toRemove := len(w.segments) - w.config.MaxSegments
	if toRemove <= 0 {
		return nil
	}

	for i := 0; i < toRemove; i++ {
		path := w.segments[0]
		if err := os.Remove(path); err != nil {
			return err
		}
		w.segments = w.segments[1:]
	}

	return nil
}

// Sync forces WAL data to disk
func (w *FileWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *FileWAL) syncLocked() error {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.currentFile != nil {
		return w.currentFile.Sync()
	}
	return nil
}

// Read reads entries starting from the given LSN
func (w *FileWAL) Read(startLSN uint64) ([]*LogEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var entries []*LogEntry

	// Read from all segments
	for _, path := range w.segments {
		segmentEntries, err := w.readSegment(path, startLSN)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segmentEntries...)
	}

	return entries, nil
}

// readSegment reads entries from a single segment
func (w *FileWAL) readSegment(path string, minLSN uint64) ([]*LogEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var entries []*LogEntry
	reader := bufio.NewReader(file)

	for {
		// Read length prefix
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return entries, nil // Partial entry at end
		}

		entryLen := binary.BigEndian.Uint32(lenBuf)

		// Read entry data
		entryBuf := make([]byte, entryLen)
		if _, err := io.ReadFull(reader, entryBuf); err != nil {
			// Partial/corrupted entry - stop here
			break
		}

		// Decode entry
		entry, err := Decode(append(lenBuf, entryBuf...))
		if err != nil {
			// Corrupted entry - skip or stop based on policy
			continue
		}

		// Filter by LSN
		if entry.LSN >= minLSN {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// ReadAll reads all entries from the WAL
func (w *FileWAL) ReadAll() ([]*LogEntry, error) {
	return w.Read(0)
}

// Truncate removes entries before the given LSN
func (w *FileWAL) Truncate(beforeLSN uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Find which segments can be removed
	var newSegments []string
	for _, path := range w.segments {
		maxLSN, err := w.getMaxLSNFromSegment(path)
		if err != nil {
			continue
		}

		if maxLSN < beforeLSN {
			// Remove this segment
			os.Remove(path)
		} else {
			newSegments = append(newSegments, path)
		}
	}

	w.segments = newSegments
	w.firstLSN = beforeLSN

	return nil
}

// Close closes the WAL
func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		w.writer.Flush()
	}
	if w.currentFile != nil {
		w.currentFile.Sync()
		return w.currentFile.Close()
	}
	return nil
}

// CurrentLSN returns the latest LSN
func (w *FileWAL) CurrentLSN() uint64 {
	return atomic.LoadUint64(&w.currentLSN)
}

// GetFirstLSN returns the first LSN
func (w *FileWAL) GetFirstLSN() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.firstLSN
}

// Stats returns WAL statistics
func (w *FileWAL) Stats() Stats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return Stats{
		CurrentLSN:   w.currentLSN,
		FirstLSN:     w.firstLSN,
		SegmentCount: len(w.segments),
		CurrentSize:  w.currentSize,
	}
}

// Stats holds WAL statistics
type Stats struct {
	CurrentLSN   uint64
	FirstLSN     uint64
	SegmentCount int
	CurrentSize  int64
}
