// Package wal provides Write-Ahead Log functionality for ACID transactions.
// The WAL ensures durability by recording all changes before they are applied to the database.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

// LogEntryType defines the type of WAL entry
type LogEntryType byte

const (
	// Transaction control entries
	LogEntryBeginTx  LogEntryType = 0x01
	LogEntryCommitTx LogEntryType = 0x02
	LogEntryAbortTx  LogEntryType = 0x03

	// Document operations
	LogEntryInsert LogEntryType = 0x10
	LogEntryUpdate LogEntryType = 0x11
	LogEntryDelete LogEntryType = 0x12

	// Index operations
	LogEntryIndexInsert LogEntryType = 0x20
	LogEntryIndexDelete LogEntryType = 0x21

	// Vector operations
	LogEntryVectorInsert LogEntryType = 0x30
	LogEntryVectorUpdate LogEntryType = 0x31
	LogEntryVectorDelete LogEntryType = 0x32

	// Checkpoint
	LogEntryCheckpoint LogEntryType = 0x40
)

// String returns human-readable name for LogEntryType
func (t LogEntryType) String() string {
	switch t {
	case LogEntryBeginTx:
		return "BEGIN_TX"
	case LogEntryCommitTx:
		return "COMMIT_TX"
	case LogEntryAbortTx:
		return "ABORT_TX"
	case LogEntryInsert:
		return "INSERT"
	case LogEntryUpdate:
		return "UPDATE"
	case LogEntryDelete:
		return "DELETE"
	case LogEntryIndexInsert:
		return "INDEX_INSERT"
	case LogEntryIndexDelete:
		return "INDEX_DELETE"
	case LogEntryVectorInsert:
		return "VECTOR_INSERT"
	case LogEntryVectorUpdate:
		return "VECTOR_UPDATE"
	case LogEntryVectorDelete:
		return "VECTOR_DELETE"
	case LogEntryCheckpoint:
		return "CHECKPOINT"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", byte(t))
	}
}

// LogEntry represents a single WAL entry
type LogEntry struct {
	LSN         uint64       // Log Sequence Number - monotonically increasing
	TxID        string       // Transaction ID
	Type        LogEntryType // Entry type
	Timestamp   int64        // Unix nanoseconds
	Collection  string       // Collection name
	Key         []byte       // Document key
	OldValue    []byte       // Previous value (for rollback)
	NewValue    []byte       // New value
	Checksum    uint32       // CRC32 checksum
}

// IsTransactionControl returns true if entry is BEGIN, COMMIT, or ABORT
func (e *LogEntry) IsTransactionControl() bool {
	return e.Type == LogEntryBeginTx || e.Type == LogEntryCommitTx || e.Type == LogEntryAbortTx
}

// IsDataOperation returns true if entry modifies data
func (e *LogEntry) IsDataOperation() bool {
	return e.Type >= LogEntryInsert && e.Type <= LogEntryDelete
}

// ValidateChecksum verifies the entry's integrity
func (e *LogEntry) ValidateChecksum() bool {
	// Recalculate checksum excluding the checksum field itself
	data := e.serializeWithoutChecksum()
	calculated := crc32.ChecksumIEEE(data)
	return calculated == e.Checksum
}

// serializeWithoutChecksum encodes entry without checksum field
func (e *LogEntry) serializeWithoutChecksum() []byte {
	buf := make([]byte, 0, 1024)

	// Header: Type (1) + LSN (8) + Timestamp (8)
	buf = append(buf, byte(e.Type))
	buf = binary.BigEndian.AppendUint64(buf, e.LSN)
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.Timestamp))

	// Transaction ID
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.TxID)))
	buf = append(buf, []byte(e.TxID)...)

	// Collection
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.Collection)))
	buf = append(buf, []byte(e.Collection)...)

	// Key
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Key)))
	buf = append(buf, e.Key...)

	// Old Value
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.OldValue)))
	buf = append(buf, e.OldValue...)

	// New Value
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.NewValue)))
	buf = append(buf, e.NewValue...)

	return buf
}

// Encode serializes the entry to binary format
func (e *LogEntry) Encode() []byte {
	// Serialize without checksum first
	buf := e.serializeWithoutChecksum()

	// Calculate and append checksum
	e.Checksum = crc32.ChecksumIEEE(buf)
	buf = binary.BigEndian.AppendUint32(buf, e.Checksum)

	// Prepend total length (4 bytes)
	finalBuf := binary.BigEndian.AppendUint32(nil, uint32(len(buf)))
	finalBuf = append(finalBuf, buf...)

	return finalBuf
}

// Decode deserializes a LogEntry from binary data
func Decode(data []byte) (*LogEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for length header")
	}

	// Read total length
	length := binary.BigEndian.Uint32(data[:4])
	if uint32(len(data)) < length+4 {
		return nil, fmt.Errorf("incomplete entry: expected %d bytes, got %d", length+4, len(data))
	}

	// Work with entry body (excluding length prefix)
	body := data[4 : 4+length]
	offset := 0

	entry := &LogEntry{}

	// Read Type
	if len(body) < 1 {
		return nil, fmt.Errorf("truncated entry: no type")
	}
	entry.Type = LogEntryType(body[0])
	offset++

	// Read LSN
	if len(body) < offset+8 {
		return nil, fmt.Errorf("truncated entry: no LSN")
	}
	entry.LSN = binary.BigEndian.Uint64(body[offset:])
	offset += 8

	// Read Timestamp
	if len(body) < offset+8 {
		return nil, fmt.Errorf("truncated entry: no timestamp")
	}
	entry.Timestamp = int64(binary.BigEndian.Uint64(body[offset:]))
	offset += 8

	// Read TxID length and value
	if len(body) < offset+2 {
		return nil, fmt.Errorf("truncated entry: no txid length")
	}
	txidLen := binary.BigEndian.Uint16(body[offset:])
	offset += 2
	if len(body) < offset+int(txidLen) {
		return nil, fmt.Errorf("truncated entry: incomplete txid")
	}
	entry.TxID = string(body[offset : offset+int(txidLen)])
	offset += int(txidLen)

	// Read Collection length and value
	if len(body) < offset+2 {
		return nil, fmt.Errorf("truncated entry: no collection length")
	}
	collLen := binary.BigEndian.Uint16(body[offset:])
	offset += 2
	if len(body) < offset+int(collLen) {
		return nil, fmt.Errorf("truncated entry: incomplete collection")
	}
	entry.Collection = string(body[offset : offset+int(collLen)])
	offset += int(collLen)

	// Read Key length and value
	if len(body) < offset+4 {
		return nil, fmt.Errorf("truncated entry: no key length")
	}
	keyLen := binary.BigEndian.Uint32(body[offset:])
	offset += 4
	if len(body) < offset+int(keyLen) {
		return nil, fmt.Errorf("truncated entry: incomplete key")
	}
	entry.Key = body[offset : offset+int(keyLen)]
	offset += int(keyLen)

	// Read OldValue length and value
	if len(body) < offset+4 {
		return nil, fmt.Errorf("truncated entry: no old value length")
	}
	oldLen := binary.BigEndian.Uint32(body[offset:])
	offset += 4
	if len(body) < offset+int(oldLen) {
		return nil, fmt.Errorf("truncated entry: incomplete old value")
	}
	entry.OldValue = body[offset : offset+int(oldLen)]
	offset += int(oldLen)

	// Read NewValue length and value
	if len(body) < offset+4 {
		return nil, fmt.Errorf("truncated entry: no new value length")
	}
	newLen := binary.BigEndian.Uint32(body[offset:])
	offset += 4
	if len(body) < offset+int(newLen) {
		return nil, fmt.Errorf("truncated entry: incomplete new value")
	}
	entry.NewValue = body[offset : offset+int(newLen)]
	offset += int(newLen)

	// Read Checksum (last 4 bytes)
	if len(body) < offset+4 {
		return nil, fmt.Errorf("truncated entry: no checksum")
	}
	entry.Checksum = binary.BigEndian.Uint32(body[offset:])

	// Validate checksum
	if !entry.ValidateChecksum() {
		return nil, fmt.Errorf("checksum mismatch - entry corrupted")
	}

	return entry, nil
}

// WAL defines the write-ahead log interface
type WAL interface {
	// Append writes a log entry and returns the assigned LSN
	Append(entry *LogEntry) (uint64, error)

	// Read reads entries starting from the given LSN
	Read(startLSN uint64) ([]*LogEntry, error)

	// ReadAll reads all entries from the WAL
	ReadAll() ([]*LogEntry, error)

	// Truncate removes entries before the given LSN (typically after checkpoint)
	Truncate(beforeLSN uint64) error

	// Close closes the WAL
	Close() error

	// CurrentLSN returns the latest LSN
	CurrentLSN() uint64

	// Sync forces WAL data to disk
	Sync() error

	// GetFirstLSN returns the LSN of the first entry
	GetFirstLSN() uint64
}

// SyncPolicy defines when WAL syncs to disk
type SyncPolicy int

const (
	// SyncOnEveryWrite syncs after every append (safest, slowest)
	SyncOnEveryWrite SyncPolicy = iota
	// SyncOnBatch syncs after a batch of entries
	SyncOnBatch
	// SyncOnCommit syncs only on transaction commit (balance)
	SyncOnCommit
	// SyncAsync syncs asynchronously in background (fastest, less safe)
	SyncAsync
)

// Config holds WAL configuration
type Config struct {
	Directory   string     // WAL file directory
	SegmentSize int64      // Max size of each segment file
	SyncPolicy  SyncPolicy // Sync behavior
	MaxSegments int        // Max number of segment files to keep
}

// DefaultConfig returns default WAL configuration
func DefaultConfig(dataDir string) *Config {
	return &Config{
		Directory:   dataDir + "/wal",
		SegmentSize: 100 * 1024 * 1024, // 100MB
		SyncPolicy:  SyncOnCommit,
		MaxSegments: 10,
	}
}

// CreateLogEntry is a helper to create properly initialized log entries
func CreateLogEntry(txID string, entryType LogEntryType, collection string, key []byte, oldVal, newVal []byte) *LogEntry {
	return &LogEntry{
		TxID:       txID,
		Type:       entryType,
		Timestamp:  time.Now().UnixNano(),
		Collection: collection,
		Key:        key,
		OldValue:   oldVal,
		NewValue:   newVal,
	}
}
