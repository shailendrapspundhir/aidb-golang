package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// AsyncTransaction represents a long-running transaction that can be
// used across multiple API calls with a time limit
type AsyncTransaction struct {
	ID            string
	State         TxState
	Isolation     IsolationLevel
	StartTime     time.Time
	ExpiryTime    time.Time
	Timeout       time.Duration
	Operations    []Operation
	mu            sync.RWMutex
	tx            *Transaction
	manager       *Manager
	commitCalled  bool
}

// IsExpired returns true if the transaction has expired
func (at *AsyncTransaction) IsExpired() bool {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return time.Now().After(at.ExpiryTime)
}

// RemainingTime returns the remaining time before expiry
func (at *AsyncTransaction) RemainingTime() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.ExpiryTime.Sub(time.Now())
}

// AddOperation adds an operation to the async transaction
func (at *AsyncTransaction) AddOperation(op Operation) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.State != TxStateActive {
		return fmt.Errorf("transaction %s is not active (state: %s)", at.ID, at.State)
	}

	if at.IsExpired() {
		at.State = TxStateAborting
		return fmt.Errorf("transaction %s has expired", at.ID)
	}

	// Add to underlying transaction
	if at.tx != nil {
		if err := at.tx.AddOperation(op); err != nil {
			return err
		}
	}

	at.Operations = append(at.Operations, op)
	return nil
}

// Commit commits the async transaction
func (at *AsyncTransaction) Commit() error {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.State != TxStateActive {
		return fmt.Errorf("transaction %s is not active", at.ID)
	}

	if at.IsExpired() {
		at.State = TxStateAborting
		if at.tx != nil {
			at.manager.Rollback(at.tx)
		}
		return fmt.Errorf("transaction %s has expired", at.ID)
	}

	if at.tx != nil {
		_, err := at.tx.Commit()
		if err != nil {
			at.State = TxStateAborted
			return err
		}
	}

	at.State = TxStateCommitted
	at.commitCalled = true
	return nil
}

// Rollback aborts the async transaction
func (at *AsyncTransaction) Rollback() error {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.State != TxStateActive && at.State != TxStateCommitting {
		return nil
	}

	if at.tx != nil {
		if err := at.manager.Rollback(at.tx); err != nil {
			return err
		}
	}

	at.State = TxStateAborted
	return nil
}

// GetState returns the current state
func (at *AsyncTransaction) GetState() TxState {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.State
}

// Info returns transaction info
func (at *AsyncTransaction) Info() TxInfo {
	at.mu.RLock()
	defer at.mu.RUnlock()

	return TxInfo{
		ID:         at.ID,
		State:      at.State.String(),
		Isolation:  at.Isolation.String(),
		StartTime:  at.StartTime,
		Duration:   time.Since(at.StartTime),
		OpCount:    len(at.Operations),
		AutoCommit: false,
	}
}

// AsyncManager manages async transactions with time limits
type AsyncManager struct {
	mu            sync.RWMutex
	transactions  map[string]*AsyncTransaction
	txManager     *Manager
	defaultTimeout time.Duration
	cleanupInterval time.Duration
	stopCleanup   chan bool
}

// NewAsyncManager creates a new async transaction manager
func NewAsyncManager(txManager *Manager, defaultTimeout time.Duration) *AsyncManager {
	if defaultTimeout <= 0 {
		defaultTimeout = 5 * time.Minute
	}

	am := &AsyncManager{
		transactions:    make(map[string]*AsyncTransaction),
		txManager:       txManager,
		defaultTimeout:  defaultTimeout,
		cleanupInterval: 30 * time.Second,
		stopCleanup:     make(chan bool),
	}

	// Start cleanup goroutine
	go am.cleanupLoop()

	return am
}

// StartTransaction starts a new async transaction
func (am *AsyncManager) StartTransaction(timeout time.Duration) (*AsyncTransaction, error) {
	if timeout <= 0 {
		timeout = am.defaultTimeout
	}

	// Create underlying transaction
	tx, err := am.txManager.Begin(WithIsolation(ReadCommitted), WithTimeout(timeout))
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Generate unique ID
	id := uuid.New().String()

	now := time.Now()
	asyncTx := &AsyncTransaction{
		ID:           id,
		State:        TxStateActive,
		Isolation:    ReadCommitted,
		StartTime:    now,
		ExpiryTime:   now.Add(timeout),
		Timeout:      timeout,
		Operations:   make([]Operation, 0),
		tx:           tx,
		manager:      am.txManager,
	}

	am.mu.Lock()
	am.transactions[id] = asyncTx
	am.mu.Unlock()

	return asyncTx, nil
}

// GetTransaction retrieves an active async transaction by ID
func (am *AsyncManager) GetTransaction(id string) (*AsyncTransaction, error) {
	am.mu.RLock()
	asyncTx, exists := am.transactions[id]
	am.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("transaction %s not found", id)
	}

	// Check if expired
	if asyncTx.IsExpired() {
		asyncTx.Rollback()
		am.mu.Lock()
		delete(am.transactions, id)
		am.mu.Unlock()
		return nil, fmt.Errorf("transaction %s has expired", id)
	}

	return asyncTx, nil
}

// CommitTransaction commits an async transaction by ID
func (am *AsyncManager) CommitTransaction(id string) error {
	asyncTx, err := am.GetTransaction(id)
	if err != nil {
		return err
	}

	if err := asyncTx.Commit(); err != nil {
		return err
	}

	// Remove from active transactions
	am.mu.Lock()
	delete(am.transactions, id)
	am.mu.Unlock()

	return nil
}

// RollbackTransaction rolls back an async transaction by ID
func (am *AsyncManager) RollbackTransaction(id string) error {
	asyncTx, err := am.GetTransaction(id)
	if err != nil {
		return err
	}

	if err := asyncTx.Rollback(); err != nil {
		return err
	}

	// Remove from active transactions
	am.mu.Lock()
	delete(am.transactions, id)
	am.mu.Unlock()

	return nil
}

// GetActiveTransactions returns all active transactions
func (am *AsyncManager) GetActiveTransactions() []*AsyncTransaction {
	am.mu.RLock()
	defer am.mu.RUnlock()

	active := make([]*AsyncTransaction, 0)
	for _, tx := range am.transactions {
		if tx.State == TxStateActive && !tx.IsExpired() {
			active = append(active, tx)
		}
	}

	return active
}

// cleanupLoop periodically removes expired transactions
func (am *AsyncManager) cleanupLoop() {
	ticker := time.NewTicker(am.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			am.cleanupExpired()
		case <-am.stopCleanup:
			return
		}
	}
}

// cleanupExpired removes expired transactions
func (am *AsyncManager) cleanupExpired() {
	am.mu.Lock()
	defer am.mu.Unlock()

	for id, tx := range am.transactions {
		if tx.IsExpired() {
			tx.Rollback()
			delete(am.transactions, id)
		}
	}
}

// Close stops the async manager and cleans up
func (am *AsyncManager) Close() error {
	close(am.stopCleanup)

	am.mu.Lock()
	defer am.mu.Unlock()

	// Rollback all active transactions
	for _, tx := range am.transactions {
		tx.Rollback()
	}

	am.transactions = make(map[string]*AsyncTransaction)
	return nil
}

// Stats returns async manager statistics
func (am *AsyncManager) Stats() AsyncManagerStats {
	am.mu.RLock()
	defer am.mu.RUnlock()

	activeCount := 0
	expiredCount := 0

	for _, tx := range am.transactions {
		if tx.State == TxStateActive {
			if tx.IsExpired() {
				expiredCount++
			} else {
				activeCount++
			}
		}
	}

	return AsyncManagerStats{
		ActiveTransactions:  activeCount,
		ExpiredTransactions: expiredCount,
		DefaultTimeout:      am.defaultTimeout,
	}
}

// AsyncManagerStats holds async manager statistics
type AsyncManagerStats struct {
	ActiveTransactions  int           `json:"activeTransactions"`
	ExpiredTransactions int           `json:"expiredTransactions"`
	DefaultTimeout      time.Duration `json:"defaultTimeout"`
}

// TransactionContextKey is the key used to store transaction in context
var TransactionContextKey = "transaction"

// ContextWithTransaction adds a transaction to the context
func ContextWithTransaction(ctx context.Context, tx *AsyncTransaction) context.Context {
	return context.WithValue(ctx, TransactionContextKey, tx)
}

// TransactionFromContext retrieves a transaction from the context
func TransactionFromContext(ctx context.Context) (*AsyncTransaction, bool) {
	tx, ok := ctx.Value(TransactionContextKey).(*AsyncTransaction)
	return tx, ok
}