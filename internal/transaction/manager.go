package transaction

import (
	"fmt"
	"sync"
	"time"

	"aidb/internal/wal"
)

// StorageApplier is implemented by the collection layer to apply pending
// transaction writes to storage during commit (deferred-write model).
// Lives in the transaction package as an interface to avoid circular imports.
type StorageApplier interface {
	// ApplyOperations applies all operations atomically to storage.
	// On partial failure, already-applied ops are undone automatically.
	ApplyOperations(ops []Operation) error
	// UndoOperations reverses previously applied operations (best effort).
	UndoOperations(ops []Operation)
}

// Manager handles transaction lifecycle and auto-transaction mode
type Manager struct {
	mu       sync.RWMutex
	activeTx map[TxID]*Transaction
	wal      wal.WAL

	// Configuration
	config *ManagerConfig

	// StorageApplier flushes deferred writes during commit
	storageApplier StorageApplier
}

// ManagerConfig holds transaction manager configuration
type ManagerConfig struct {
	DefaultIsolation   IsolationLevel
	DefaultTimeout     time.Duration
	MaxActiveTransactions int
	AutoCommitEnabled  bool // If true, all operations use auto-transaction mode
}

// DefaultManagerConfig returns default configuration
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		DefaultIsolation:      ReadCommitted,
		DefaultTimeout:        30 * time.Second,
		MaxActiveTransactions: 1000,
		AutoCommitEnabled:     true,
	}
}

// NewManager creates a new transaction manager
func NewManager(wal wal.WAL, config *ManagerConfig) *Manager {
	if config == nil {
		config = DefaultManagerConfig()
	}

	return &Manager{
		activeTx: make(map[TxID]*Transaction),
		wal:      wal,
		config:   config,
	}
}

// Begin starts a new transaction
func (m *Manager) Begin(opts ...BeginOption) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check max active transactions
	if len(m.activeTx) >= m.config.MaxActiveTransactions {
		return nil, fmt.Errorf("too many active transactions (%d)", len(m.activeTx))
	}

	// Default options
	isolation := m.config.DefaultIsolation
	timeout := m.config.DefaultTimeout
	autoCommit := m.config.AutoCommitEnabled

	// Apply options
	for _, opt := range opts {
		opt(&isolation, &timeout, &autoCommit)
	}

	// Generate transaction ID
	id := GenerateTxID()

	// Write begin record to WAL
	entry := wal.CreateLogEntry(
		string(id),
		wal.LogEntryBeginTx,
		"",
		nil,
		nil,
		nil,
	)

	lsn, err := m.wal.Append(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to write begin record: %w", err)
	}

	// Create transaction
	tx := NewTransaction(id, isolation, m.wal, timeout, autoCommit)
	tx.startLSN = lsn

	// Register as active
	m.activeTx[id] = tx

	return tx, nil
}

// SetStorageApplier sets the storage applier used to flush deferred writes on commit.
// Must be called after both Manager and collection.Manager are created.
func (m *Manager) SetStorageApplier(applier StorageApplier) {
	m.storageApplier = applier
}

// GetWAL returns the WAL instance (used by recovery).
func (m *Manager) GetWAL() wal.WAL {
	return m.wal
}

// Commit commits a transaction using proper WAL ordering:
// 1. Write COMMIT record to WAL and fsync (makes commit durable)
// 2. Apply deferred writes to storage (idempotent)
// This ensures durability: if crash after WAL sync, recovery will REDO the tx.
func (m *Manager) Commit(tx *Transaction) error {
	ops := tx.GetOperations()

	// Step 1: Write COMMIT record to WAL and fsync FIRST (durability guarantee)
	_, err := tx.Commit()
	if err != nil {
		m.mu.Lock()
		delete(m.activeTx, tx.ID)
		m.mu.Unlock()
		return fmt.Errorf("commit failed — WAL error: %w", err)
	}

	// Step 2: Now safely apply changes to storage (idempotent operations)
	if m.storageApplier != nil && len(ops) > 0 {
		if applyErr := m.storageApplier.ApplyOperations(ops); applyErr != nil {
			// WAL commit is already durable — recovery will REDO on restart
			m.mu.Lock()
			delete(m.activeTx, tx.ID)
			m.mu.Unlock()
			return fmt.Errorf("storage apply failed after durable commit: %w", applyErr)
		}
	}

	// Remove from active transactions
	m.mu.Lock()
	delete(m.activeTx, tx.ID)
	m.mu.Unlock()

	return nil
}

// Rollback aborts a transaction and removes it from active list
func (m *Manager) Rollback(tx *Transaction) error {
	err := tx.Rollback()

	// Remove from active
	m.mu.Lock()
	delete(m.activeTx, tx.ID)
	m.mu.Unlock()

	return err
}

// GetTransaction retrieves an active transaction by ID
func (m *Manager) GetTransaction(id TxID) (*Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tx, ok := m.activeTx[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}

	return tx, nil
}

// GetActiveTransactions returns all active transactions
func (m *Manager) GetActiveTransactions() []*Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()

	txs := make([]*Transaction, 0, len(m.activeTx))
	for _, tx := range m.activeTx {
		txs = append(txs, tx)
	}

	return txs
}

// CleanupExpired aborts transactions that have timed out
func (m *Manager) CleanupExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for id, tx := range m.activeTx {
		if tx.Duration() > tx.Timeout {
			tx.Rollback()
			delete(m.activeTx, id)
			count++
		}
	}

	return count
}

// Stats returns transaction manager statistics
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return ManagerStats{
		ActiveTransactions: len(m.activeTx),
		MaxActive:          m.config.MaxActiveTransactions,
		AutoCommitEnabled:  m.config.AutoCommitEnabled,
	}
}

// ManagerStats holds manager statistics
type ManagerStats struct {
	ActiveTransactions int  `json:"activeTransactions"`
	MaxActive          int  `json:"maxActive"`
	AutoCommitEnabled  bool `json:"autoCommitEnabled"`
}

// Close closes the manager and rolls back all active transactions
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Rollback all active transactions
	for _, tx := range m.activeTx {
		tx.Rollback()
	}

	m.activeTx = make(map[TxID]*Transaction)
	return nil
}

// BeginOption is a functional option for Begin
type BeginOption func(*IsolationLevel, *time.Duration, *bool)

// WithIsolation sets the isolation level
func WithIsolation(level IsolationLevel) BeginOption {
	return func(i *IsolationLevel, t *time.Duration, a *bool) {
		*i = level
	}
}

// WithTimeout sets the transaction timeout
func WithTimeout(timeout time.Duration) BeginOption {
	return func(i *IsolationLevel, t *time.Duration, a *bool) {
		*t = timeout
	}
}

// WithAutoCommit sets auto-commit mode
func WithAutoCommit(enabled bool) BeginOption {
	return func(i *IsolationLevel, t *time.Duration, a *bool) {
		*a = enabled
	}
}

// AutoTransaction executes a function within an auto-transaction.
// If the function returns an error, the transaction is rolled back.
// If the function succeeds, the transaction is committed.
// This provides ACID compliance for single operations.
func (m *Manager) AutoTransaction(fn func(*Transaction) error) error {
	// Begin transaction
	tx, err := m.Begin(WithAutoCommit(true))
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute the function
	if err := fn(tx); err != nil {
		// Rollback on error
		if rbErr := m.Rollback(tx); rbErr != nil {
			return fmt.Errorf("operation failed: %v, rollback also failed: %v", err, rbErr)
		}
		return err
	}

	// Commit on success
	if err := m.Commit(tx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// AutoTransactionWithResult executes a function within an auto-transaction and returns a result
func AutoTransactionWithResult[T any](m *Manager, fn func(*Transaction) (T, error)) (T, error) {
	var result T

	tx, err := m.Begin(WithAutoCommit(true))
	if err != nil {
		return result, fmt.Errorf("failed to begin transaction: %w", err)
	}

	result, err = fn(tx)
	if err != nil {
		if rbErr := m.Rollback(tx); rbErr != nil {
			return result, fmt.Errorf("operation failed: %v, rollback also failed: %v", err, rbErr)
		}
		return result, err
	}

	if err := m.Commit(tx); err != nil {
		return result, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}
