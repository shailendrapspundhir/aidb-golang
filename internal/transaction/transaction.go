// Package transaction provides ACID transaction support for AIDB.
// It implements auto-transaction mode where each operation is automatically
// wrapped in a transaction that commits on success or rolls back on failure.
package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"aidb/internal/document"
	"aidb/internal/wal"
	"github.com/google/uuid"
)

// TxID is a unique transaction identifier
type TxID string

// GenerateTxID creates a new unique transaction ID
func GenerateTxID() TxID {
	return TxID(uuid.New().String())
}

// TxState represents the state of a transaction
type TxState int

const (
	TxStateActive TxState = iota
	TxStateCommitting
	TxStateCommitted
	TxStateAborting
	TxStateAborted
)

func (s TxState) String() string {
	switch s {
	case TxStateActive:
		return "ACTIVE"
	case TxStateCommitting:
		return "COMMITTING"
	case TxStateCommitted:
		return "COMMITTED"
	case TxStateAborting:
		return "ABORTING"
	case TxStateAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// IsolationLevel defines transaction isolation levels
type IsolationLevel int

const (
	// ReadUncommitted allows dirty reads (not implemented - falls back to ReadCommitted)
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted prevents dirty reads (default)
	ReadCommitted
	// RepeatableRead prevents non-repeatable reads
	RepeatableRead
	// Serializable prevents phantom reads
	Serializable
)

func (i IsolationLevel) String() string {
	switch i {
	case ReadUncommitted:
		return "READ_UNCOMMITTED"
	case ReadCommitted:
		return "READ_COMMITTED"
	case RepeatableRead:
		return "REPEATABLE_READ"
	case Serializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// OperationType defines the type of operation within a transaction
type OperationType int

const (
	OpInsert OperationType = iota
	OpUpdate
	OpDelete
	OpIndexInsert
	OpIndexDelete
	OpVectorInsert
	OpVectorUpdate
	OpVectorDelete
)

func (o OperationType) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpIndexInsert:
		return "INDEX_INSERT"
	case OpIndexDelete:
		return "INDEX_DELETE"
	case OpVectorInsert:
		return "VECTOR_INSERT"
	case OpVectorUpdate:
		return "VECTOR_UPDATE"
	case OpVectorDelete:
		return "VECTOR_DELETE"
	default:
		return "UNKNOWN"
	}
}

// Operation represents a single operation within a transaction
type Operation struct {
	Type       OperationType
	Collection string
	DocumentID string
	OldValue   *document.Document // For rollback
	NewValue   *document.Document // New state
	LSN        uint64             // WAL log sequence number
}

// Transaction represents a database transaction
type Transaction struct {
	ID            TxID
	State         TxState
	Isolation     IsolationLevel
	StartTime     time.Time
	Timeout       time.Duration
	AutoCommit    bool // If true, transaction commits automatically on success

	// Operation tracking
	operations []Operation
	mu         sync.RWMutex

	// Read/Write sets for isolation
	readSet  map[string]*document.Document
	writeSet map[string]*document.Document

	// WAL reference
	wal      wal.WAL
	startLSN uint64

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Error tracking
	err error
}

// NewTransaction creates a new transaction
func NewTransaction(id TxID, isolation IsolationLevel, wal wal.WAL, timeout time.Duration, autoCommit bool) *Transaction {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &Transaction{
		ID:         id,
		State:      TxStateActive,
		Isolation:  isolation,
		StartTime:  time.Now(),
		Timeout:    timeout,
		AutoCommit: autoCommit,
		operations: make([]Operation, 0),
		readSet:    make(map[string]*document.Document),
		writeSet:   make(map[string]*document.Document),
		wal:        wal,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// AddOperation records an operation in the transaction
// This writes to WAL first (Write-Ahead Log protocol)
func (tx *Transaction) AddOperation(op Operation) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxStateActive {
		return fmt.Errorf("transaction %s is not active (state: %s)", tx.ID, tx.State)
	}

	// Check context cancellation
	select {
	case <-tx.ctx.Done():
		return fmt.Errorf("transaction %s timed out", tx.ID)
	default:
	}

	// Determine WAL entry type
	var entryType wal.LogEntryType
	switch op.Type {
	case OpInsert:
		entryType = wal.LogEntryInsert
	case OpUpdate:
		entryType = wal.LogEntryUpdate
	case OpDelete:
		entryType = wal.LogEntryDelete
	case OpIndexInsert:
		entryType = wal.LogEntryIndexInsert
	case OpIndexDelete:
		entryType = wal.LogEntryIndexDelete
	case OpVectorInsert:
		entryType = wal.LogEntryVectorInsert
	case OpVectorUpdate:
		entryType = wal.LogEntryVectorUpdate
	case OpVectorDelete:
		entryType = wal.LogEntryVectorDelete
	}

	// Serialize values
	var oldVal, newVal []byte
	var err error
	if op.OldValue != nil {
		oldVal, err = op.OldValue.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to serialize old value: %w", err)
		}
	}
	if op.NewValue != nil {
		newVal, err = op.NewValue.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to serialize new value: %w", err)
		}
	}

	// Create and append WAL entry
	entry := wal.CreateLogEntry(
		string(tx.ID),
		entryType,
		op.Collection,
		[]byte(op.DocumentID),
		oldVal,
		newVal,
	)

	lsn, err := tx.wal.Append(entry)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	op.LSN = lsn
	tx.operations = append(tx.operations, op)

	// Track in write set
	key := op.Collection + ":" + op.DocumentID
	if op.Type == OpDelete {
		tx.writeSet[key] = nil // Mark as deleted
	} else {
		tx.writeSet[key] = op.NewValue
	}

	return nil
}

// Commit commits the transaction
// Returns the LSN of the commit record
func (tx *Transaction) Commit() (uint64, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxStateActive {
		return 0, fmt.Errorf("transaction %s is not active (state: %s)", tx.ID, tx.State)
	}

	tx.State = TxStateCommitting

	// Write commit record to WAL
	entry := wal.CreateLogEntry(
		string(tx.ID),
		wal.LogEntryCommitTx,
		"",
		nil,
		nil,
		nil,
	)

	lsn, err := tx.wal.Append(entry)
	if err != nil {
		tx.State = TxStateAborting
		return 0, fmt.Errorf("failed to write commit record: %w", err)
	}

	// Sync WAL to ensure durability
	if err := tx.wal.Sync(); err != nil {
		tx.State = TxStateAborting
		return 0, fmt.Errorf("failed to sync WAL: %w", err)
	}

	tx.State = TxStateCommitted
	tx.cancel() // Cancel context to release resources

	return lsn, nil
}

// Rollback aborts the transaction and undoes all operations
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxStateActive && tx.State != TxStateCommitting {
		// Already committed or aborted
		return nil
	}

	tx.State = TxStateAborting

	// Write abort record to WAL
	entry := wal.CreateLogEntry(
		string(tx.ID),
		wal.LogEntryAbortTx,
		"",
		nil,
		nil,
		nil,
	)
	tx.wal.Append(entry)
	tx.wal.Sync()

	// Note: Actual undo of operations happens at the storage layer
	// The WAL contains enough information to redo/undo operations during recovery

	tx.State = TxStateAborted
	tx.cancel()

	return nil
}

// GetOperations returns a copy of all operations in the transaction
func (tx *Transaction) GetOperations() []Operation {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	ops := make([]Operation, len(tx.operations))
	copy(ops, tx.operations)
	return ops
}

// GetWriteSet returns the write set (modified documents)
func (tx *Transaction) GetWriteSet() map[string]*document.Document {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	ws := make(map[string]*document.Document)
	for k, v := range tx.writeSet {
		ws[k] = v
	}
	return ws
}

// IsActive returns true if the transaction is active
func (tx *Transaction) IsActive() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.State == TxStateActive
}

// IsCommitted returns true if the transaction committed successfully
func (tx *Transaction) IsCommitted() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.State == TxStateCommitted
}

// Duration returns how long the transaction has been running
func (tx *Transaction) Duration() time.Duration {
	return time.Since(tx.StartTime)
}

// GetFromWriteBuffer checks if a document has been modified in this transaction's
// write buffer. Returns (document, isDeleted, found).
// If found is false, the document is not in the buffer — check storage.
// If found is true and deleted is true, the document was deleted in this transaction.
// The returned document is a deep copy to prevent callers from corrupting buffer data.
func (tx *Transaction) GetFromWriteBuffer(collection, docID string) (doc *document.Document, deleted bool, found bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	key := collection + ":" + docID
	val, exists := tx.writeSet[key]
	if !exists {
		return nil, false, false
	}
	if val == nil {
		return nil, true, true // Deleted in this transaction
	}
	// Return a deep copy to prevent callers from modifying buffer data
	docCopy := &document.Document{
		ID:        val.ID,
		CreatedAt: val.CreatedAt,
		UpdatedAt: val.UpdatedAt,
		Data:      make(map[string]interface{}),
	}
	for k, v := range val.Data {
		docCopy.Data[k] = v
	}
	return docCopy, false, true
}

// ClearWriteBuffer discards the write buffer (used on rollback with deferred writes).
func (tx *Transaction) ClearWriteBuffer() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.writeSet = make(map[string]*document.Document)
	tx.operations = tx.operations[:0]
}

// Info returns transaction information
func (tx *Transaction) Info() TxInfo {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	return TxInfo{
		ID:        string(tx.ID),
		State:     tx.State.String(),
		Isolation: tx.Isolation.String(),
		StartTime: tx.StartTime,
		Duration:  tx.Duration(),
		OpCount:   len(tx.operations),
		AutoCommit: tx.AutoCommit,
	}
}

// TxInfo contains transaction metadata
type TxInfo struct {
	ID         string        `json:"id"`
	State      string        `json:"state"`
	Isolation  string        `json:"isolation"`
	StartTime  time.Time     `json:"startTime"`
	Duration   time.Duration `json:"duration"`
	OpCount    int           `json:"opCount"`
	AutoCommit bool          `json:"autoCommit"`
}
