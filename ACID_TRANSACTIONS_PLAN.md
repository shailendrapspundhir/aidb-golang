# AIDB ACID Transactions Implementation Plan

## Executive Summary

This document provides a comprehensive plan to extend AIDB (AI-Native Database) with full ACID (Atomicity, Consistency, Isolation, Durability) support, including single-document transactions, multi-document transactions, bulk transactions, and rollback capabilities across collections, vectors, and indexes.

---

## 1. Current Architecture Analysis

### 1.1 Existing Components

| Component | Current State | ACID Gap |
|-----------|--------------|----------|
| **BoltDB Storage** | Uses BoltDB transactions (`db.Update`/`db.View`) | No cross-collection transactions |
| **RocksDB Storage** | Column-family based, batch writes | No transaction coordination |
| **Hybrid Storage** | Cache + Disk, no atomicity | Cache/disk inconsistency possible |
| **Collection Manager** | Per-collection operations | No multi-collection atomicity |
| **Vector Storage** | HNSW in-memory index | Not transactional, index out of sync |
| **Full-Text Index** | In-memory inverted index | Not persisted with documents |
| **B-Tree/Hash Indexes** | In-memory structures | Not transaction-aware |
| **Aggregation** | Streaming cursor-based | Read consistency not guaranteed |

### 1.2 Critical Gaps Identified

1. **No Multi-Document Atomicity**: Operations across multiple documents are not atomic
2. **No Cross-Collection Transactions**: Cannot maintain consistency across collections
3. **Index-Storage Divergence**: Secondary indexes can become inconsistent with primary storage
4. **Vector Index Non-Transactional**: HNSW updates are not tied to document operations
5. **Full-Text Index Volatility**: In-memory index lost on restart, not synced with storage
6. **No Rollback Mechanism**: Failed operations leave partial state
7. **No Isolation Levels**: Concurrent operations see intermediate states
8. **No WAL (Write-Ahead Log)**: Durability depends solely on underlying storage

---

## 2. ACID Requirements Definition

### 2.1 Atomicity
- All operations in a transaction complete successfully or none do
- Rollback capability for partial failures
- Point-in-time recovery support

### 2.2 Consistency
- Schema validation within transactions
- Referential integrity across collections
- Index consistency with primary data
- Vector dimension consistency

### 2.3 Isolation Levels (SQL Standard)
| Level | Description | Use Case |
|-------|-------------|----------|
| **READ UNCOMMITTED** | Dirty reads allowed | High throughput, low consistency |
| **READ COMMITTED** | No dirty reads | Default for most operations |
| **REPEATABLE READ** | Snapshot isolation | Long-running queries |
| **SERIALIZABLE** | Full serializability | Critical financial operations |

### 2.4 Durability
- Write-ahead logging (WAL)
- Checkpoint mechanism
- Crash recovery
- Data replication (future)

---

## 3. Implementation Architecture

### 3.1 Core Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TRANSACTION LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ Transaction  │  │ Transaction  │  │ Savepoint    │  │ Distributed  │    │
│  │ Manager      │  │ Coordinator  │  │ Manager      │  │ Transaction│    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
├─────────────────────────────────────────────────────────────────────────────┤
│                           WAL LAYER                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                       │
│  │ WAL Writer   │  │ Log Recovery │  │ Checkpoint   │                       │
│  │              │  │ Manager      │  │ Manager      │                       │
│  └──────────────┘  └──────────────┘  └──────────────┘                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                           STORAGE ADAPTER LAYER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ BoltDB       │  │ RocksDB      │  │ Memory       │  │ Vector       │    │
│  │ Adapter      │  │ Adapter      │  │ Adapter      │  │ Adapter      │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
├─────────────────────────────────────────────────────────────────────────────┤
│                           INDEX LAYER                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                       │
│  │ B-Tree Index │  │ Hash Index   │  │ Full-Text    │                       │
│  │ Manager      │  │ Manager      │  │ Index Manager│                       │
│  └──────────────┘  └──────────────┘  └──────────────┘                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Directory Structure

```
internal/
├── transaction/
│   ├── transaction.go          # Core transaction types and interfaces
│   ├── manager.go              # Transaction manager
│   ├── coordinator.go          # Distributed transaction coordinator
│   ├── savepoint.go            # Savepoint management
│   ├── isolation.go            # Isolation level implementations
│   └── errors.go               # Transaction errors
├── wal/
│   ├── wal.go                  # Write-ahead log interface
│   ├── writer.go               # WAL writer implementation
│   ├── reader.go               # WAL reader for recovery
│   ├── recovery.go             # Recovery manager
│   ├── checkpoint.go           # Checkpoint manager
│   └── encoder.go              # Log entry encoding
├── storage/
│   ├── transactional.go        # Transactional storage wrapper
│   ├── adapter.go              # Storage adapter interface
│   └── ...existing files
└── index/
    ├── transactional_index.go  # Transactional index wrapper
    └── ...existing files
```

---

## 4. Detailed Implementation Plan

### Phase 1: Core WAL Infrastructure (Week 1-2)

#### 4.1.1 WAL Design

```go
// internal/wal/wal.go
package wal

// LogEntryType defines the type of WAL entry
type LogEntryType byte

const (
    LogEntryBeginTx     LogEntryType = 0x01
    LogEntryCommitTx    LogEntryType = 0x02
    LogEntryAbortTx     LogEntryType = 0x03
    LogEntryInsert      LogEntryType = 0x10
    LogEntryUpdate      LogEntryType = 0x11
    LogEntryDelete      LogEntryType = 0x12
    LogEntryIndexInsert LogEntryType = 0x20
    LogEntryIndexDelete LogEntryType = 0x21
    LogEntryVectorInsert LogEntryType = 0x30
    LogEntryVectorDelete LogEntryType = 0x31
    LogEntryCheckpoint  LogEntryType = 0x40
)

// LogEntry represents a single WAL entry
type LogEntry struct {
    LSN         uint64       // Log Sequence Number
    TxID        string       // Transaction ID
    Type        LogEntryType // Entry type
    Timestamp   int64        // Unix nanoseconds
    Collection  string       // Collection name
    Key         []byte       // Document key
    OldValue    []byte       // Previous value (for rollback)
    NewValue    []byte       // New value
    Checksum    uint32       // CRC32 checksum
}

// WAL defines the write-ahead log interface
type WAL interface {
    // Append writes a log entry and returns the LSN
    Append(entry *LogEntry) (uint64, error)
    
    // Read reads entries starting from LSN
    Read(startLSN uint64) ([]*LogEntry, error)
    
    // Truncate removes entries before the given LSN
    Truncate(beforeLSN uint64) error
    
    // Close closes the WAL
    Close() error
    
    // CurrentLSN returns the latest LSN
    CurrentLSN() uint64
    
    // Sync flushes WAL to disk
    Sync() error
}
```

#### 4.1.2 WAL Implementation

```go
// internal/wal/writer.go
package wal

import (
    "bufio"
    "encoding/binary"
    "hash/crc32"
    "os"
    "path/filepath"
    "sync"
    "sync/atomic"
)

// FileWAL implements WAL using append-only files
type FileWAL struct {
    mu         sync.RWMutex
    dir        string
    currentFile *os.File
    writer     *bufio.Writer
    currentLSN uint64
    segmentSize int64
    syncPolicy SyncPolicy
}

type SyncPolicy int

const (
    SyncOnEveryWrite SyncPolicy = iota
    SyncOnBatch
    SyncOnCommit
    SyncAsync
)

func NewFileWAL(dir string, segmentSize int64, policy SyncPolicy) (*FileWAL, error) {
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, err
    }
    
    wal := &FileWAL{
        dir:         dir,
        segmentSize: segmentSize,
        syncPolicy:  policy,
    }
    
    // Find existing segments or create new
    if err := wal.openCurrentSegment(); err != nil {
        return nil, err
    }
    
    return wal, nil
}

func (w *FileWAL) Append(entry *LogEntry) (uint64, error) {
    w.mu.Lock()
    defer w.mu.Unlock()
    
    // Assign LSN
    entry.LSN = atomic.AddUint64(&w.currentLSN, 1)
    entry.Timestamp = time.Now().UnixNano()
    
    // Serialize entry
    data := encodeEntry(entry)
    
    // Check if we need to rotate
    if w.currentFile != nil {
        stat, _ := w.currentFile.Stat()
        if stat.Size()+int64(len(data)) > w.segmentSize {
            if err := w.rotateSegment(); err != nil {
                return 0, err
            }
        }
    }
    
    // Write entry
    if _, err := w.writer.Write(data); err != nil {
        return 0, err
    }
    
    // Sync based on policy
    if w.syncPolicy == SyncOnEveryWrite {
        if err := w.writer.Flush(); err != nil {
            return 0, err
        }
        if err := w.currentFile.Sync(); err != nil {
            return 0, err
        }
    }
    
    return entry.LSN, nil
}

func encodeEntry(entry *LogEntry) []byte {
    // Binary encoding for efficiency
    // Format: [4:len][1:type][8:lsn][8:timestamp][2:txid_len][txid][2:collection_len][collection]
    //         [4:key_len][key][4:old_len][old_value][4:new_len][new_value][4:checksum]
    buf := make([]byte, 0, 1024)
    
    // Header
    buf = append(buf, byte(entry.Type))
    buf = binary.BigEndian.AppendUint64(buf, entry.LSN)
    buf = binary.BigEndian.AppendUint64(buf, uint64(entry.Timestamp))
    
    // Transaction ID
    buf = binary.BigEndian.AppendUint16(buf, uint16(len(entry.TxID)))
    buf = append(buf, []byte(entry.TxID)...)
    
    // Collection
    buf = binary.BigEndian.AppendUint16(buf, uint16(len(entry.Collection)))
    buf = append(buf, []byte(entry.Collection)...)
    
    // Key
    buf = binary.BigEndian.AppendUint32(buf, uint32(len(entry.Key)))
    buf = append(buf, entry.Key...)
    
    // Old value
    buf = binary.BigEndian.AppendUint32(buf, uint32(len(entry.OldValue)))
    buf = append(buf, entry.OldValue...)
    
    // New value
    buf = binary.BigEndian.AppendUint32(buf, uint32(len(entry.NewValue)))
    buf = append(buf, entry.NewValue...)
    
    // Checksum (computed over everything except checksum itself)
    checksum := crc32.ChecksumIEEE(buf)
    buf = binary.BigEndian.AppendUint32(buf, checksum)
    
    // Length prefix for the entire entry
    finalBuf := binary.BigEndian.AppendUint32(nil, uint32(len(buf)))
    finalBuf = append(finalBuf, buf...)
    
    return finalBuf
}
```

### Phase 2: Transaction Manager (Week 2-3)

#### 4.2.1 Core Transaction Types

```go
// internal/transaction/transaction.go
package transaction

import (
    "context"
    "time"
    "aidb/internal/document"
    "aidb/internal/wal"
)

// TxID is a unique transaction identifier
type TxID string

// TxState represents the state of a transaction
type TxState int

const (
    TxStateActive TxState = iota
    TxStatePreparing
    TxStatePrepared
    TxStateCommitting
    TxStateCommitted
    TxStateAborting
    TxStateAborted
)

// IsolationLevel defines transaction isolation levels
type IsolationLevel int

const (
    ReadUncommitted IsolationLevel = iota
    ReadCommitted
    RepeatableRead
    Serializable
)

// Transaction represents a database transaction
type Transaction struct {
    ID             TxID
    State          TxState
    Isolation      IsolationLevel
    StartTime      time.Time
    Timeout        time.Duration
    
    // Operation tracking
    operations     []Operation
    readSet        map[string]DocumentSnapshot
    writeSet       map[string]*document.Document
    
    // Savepoints for partial rollback
    savepoints     []Savepoint
    
    // WAL reference
    wal            wal.WAL
    startLSN       uint64
    
    // Context for cancellation
    ctx            context.Context
    cancel         context.CancelFunc
    
    // Lock management
    heldLocks      map[string]LockType
    lockMu         sync.RWMutex
}

type Operation struct {
    Type       OperationType
    Collection string
    DocumentID string
    OldValue   *document.Document
    NewValue   *document.Document
    LSN        uint64
}

type OperationType int

const (
    OpInsert OperationType = iota
    OpUpdate
    OpDelete
    OpIndexUpdate
    OpVectorInsert
    OpVectorUpdate
    OpVectorDelete
)

// DocumentSnapshot captures document state for MVCC
type DocumentSnapshot struct {
    Document  *document.Document
    Version   uint64
    Timestamp int64
}

func NewTransaction(id TxID, isolation IsolationLevel, wal wal.WAL, timeout time.Duration) *Transaction {
    ctx, cancel := context.WithTimeout(context.Background(), timeout)
    return &Transaction{
        ID:         id,
        State:      TxStateActive,
        Isolation:  isolation,
        StartTime:  time.Now(),
        Timeout:    timeout,
        operations: make([]Operation, 0),
        readSet:    make(map[string]DocumentSnapshot),
        writeSet:   make(map[string]*document.Document),
        savepoints: make([]Savepoint, 0),
        wal:        wal,
        heldLocks:  make(map[string]LockType),
        ctx:        ctx,
        cancel:     cancel,
    }
}

// AddOperation records an operation in the transaction
func (tx *Transaction) AddOperation(op Operation) error {
    if tx.State != TxStateActive {
        return ErrTransactionNotActive
    }
    
    // Write to WAL first (WAL protocol)
    entry := &wal.LogEntry{
        TxID:       string(tx.ID),
        Type:       operationTypeToLogType(op.Type),
        Collection: op.Collection,
        Key:        []byte(op.DocumentID),
    }
    
    if op.OldValue != nil {
        entry.OldValue, _ = op.OldValue.ToJSON()
    }
    if op.NewValue != nil {
        entry.NewValue, _ = op.NewValue.ToJSON()
    }
    
    lsn, err := tx.wal.Append(entry)
    if err != nil {
        return err
    }
    
    op.LSN = lsn
    tx.operations = append(tx.operations, op)
    
    // Track in write set
    if op.NewValue != nil {
        tx.writeSet[op.Collection+":"+op.DocumentID] = op.NewValue
    }
    
    return nil
}

// RollbackToSavepoint rolls back to a specific savepoint
func (tx *Transaction) RollbackToSavepoint(name string) error {
    // Find the savepoint
    var spIndex int
    found := false
    for i, sp := range tx.savepoints {
        if sp.Name == name {
            spIndex = i
            found = true
            break
        }
    }
    
    if !found {
        return ErrSavepointNotFound
    }
    
    // Remove operations after savepoint
    tx.operations = tx.operations[:tx.savepoints[spIndex].OperationIndex]
    
    // Remove savepoints after this one
    tx.savepoints = tx.savepoints[:spIndex+1]
    
    // Rebuild write set
    tx.writeSet = make(map[string]*document.Document)
    for _, op := range tx.operations {
        if op.NewValue != nil {
            tx.writeSet[op.Collection+":"+op.DocumentID] = op.NewValue
        }
    }
    
    return nil
}
```

#### 4.2.2 Transaction Manager

```go
// internal/transaction/manager.go
package transaction

import (
    "sync"
    "sync/atomic"
    "github.com/google/uuid"
    "aidb/internal/wal"
)

// Manager handles transaction lifecycle
type Manager struct {
    mu              sync.RWMutex
    activeTxns      map[TxID]*Transaction
    wal             wal.WAL
    storageAdapter  StorageAdapter
    
    // Transaction ID generation
    txCounter       uint64
    
    // Configuration
    defaultIsolation IsolationLevel
    defaultTimeout   time.Duration
    maxActiveTxns    int
    
    // Lock manager
    lockManager     *LockManager
    
    // MVCC snapshot management
    snapshotManager *SnapshotManager
}

func NewManager(wal wal.WAL, adapter StorageAdapter, config *ManagerConfig) *Manager {
    return &Manager{
        activeTxns:       make(map[TxID]*Transaction),
        wal:              wal,
        storageAdapter:   adapter,
        defaultIsolation: config.DefaultIsolation,
        defaultTimeout:   config.DefaultTimeout,
        maxActiveTxns:    config.MaxActiveTransactions,
        lockManager:      NewLockManager(),
        snapshotManager:  NewSnapshotManager(),
    }
}

// Begin starts a new transaction
func (m *Manager) Begin(opts ...BeginOption) (*Transaction, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Check max active transactions
    if len(m.activeTxns) >= m.maxActiveTxns {
        return nil, ErrTooManyActiveTransactions
    }
    
    // Generate transaction ID
    id := TxID(uuid.New().String())
    
    // Apply options
    isolation := m.defaultIsolation
    timeout := m.defaultTimeout
    
    for _, opt := range opts {
        opt(&isolation, &timeout)
    }
    
    // Create transaction
    tx := NewTransaction(id, isolation, m.wal, timeout)
    
    // Log begin transaction
    entry := &wal.LogEntry{
        TxID: string(id),
        Type: wal.LogEntryBeginTx,
    }
    lsn, err := m.wal.Append(entry)
    if err != nil {
        return nil, err
    }
    tx.startLSN = lsn
    
    // Register transaction
    m.activeTxns[id] = tx
    
    return tx, nil
}

// Commit commits a transaction
func (m *Manager) Commit(tx *Transaction) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    if tx.State != TxStateActive {
        return ErrInvalidTransactionState
    }
    
    tx.State = TxStateCommitting
    
    // Two-phase commit for distributed transactions
    if len(tx.operations) > 0 && m.isDistributed(tx) {
        return m.twoPhaseCommit(tx)
    }
    
    // Single-phase commit
    // 1. Write commit record to WAL
    entry := &wal.LogEntry{
        TxID: string(tx.ID),
        Type: wal.LogEntryCommitTx,
    }
    _, err := m.wal.Append(entry)
    if err != nil {
        tx.State = TxStateAborting
        return err
    }
    
    // 2. Apply changes to storage
    if err := m.applyChanges(tx); err != nil {
        // Attempt rollback
        m.Rollback(tx)
        return err
    }
    
    // 3. Sync WAL
    if err := m.wal.Sync(); err != nil {
        return err
    }
    
    // 4. Release locks
    m.lockManager.ReleaseAll(tx.ID)
    
    tx.State = TxStateCommitted
    delete(m.activeTxns, tx.ID)
    
    return nil
}

// Rollback aborts a transaction
func (m *Manager) Rollback(tx *Transaction) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    if tx.State != TxStateActive && tx.State != TxStateCommitting {
        return ErrInvalidTransactionState
    }
    
    tx.State = TxStateAborting
    
    // Write abort record
    entry := &wal.LogEntry{
        TxID: string(tx.ID),
        Type: wal.LogEntryAbortTx,
    }
    m.wal.Append(entry)
    
    // Undo operations in reverse order
    for i := len(tx.operations) - 1; i >= 0; i-- {
        op := tx.operations[i]
        if err := m.undoOperation(tx, op); err != nil {
            // Log error but continue rollback
            log.Printf("Failed to undo operation: %v", err)
        }
    }
    
    // Release locks
    m.lockManager.ReleaseAll(tx.ID)
    
    tx.State = TxStateAborted
    delete(m.activeTxns, tx.ID)
    
    return nil
}

func (m *Manager) undoOperation(tx *Transaction, op Operation) error {
    switch op.Type {
    case OpInsert:
        // Undo insert = delete
        return m.storageAdapter.Delete(op.Collection, op.DocumentID)
    case OpUpdate, OpDelete:
        // Undo update/delete = restore old value
        if op.OldValue != nil {
            return m.storageAdapter.InsertOrUpdate(op.Collection, op.OldValue)
        }
        return m.storageAdapter.Delete(op.Collection, op.DocumentID)
    case OpIndexUpdate:
        // Revert index changes
        return m.storageAdapter.RevertIndex(op.Collection, op.DocumentID, op.OldValue)
    }
    return nil
}

type BeginOption func(*IsolationLevel, *time.Duration)

func WithIsolation(level IsolationLevel) BeginOption {
    return func(i *IsolationLevel, t *time.Duration) {
        *i = level
    }
}

func WithTimeout(timeout time.Duration) BeginOption {
    return func(i *IsolationLevel, t *time.Duration) {
        *t = timeout
    }
}
```

### Phase 3: Lock Manager and Concurrency Control (Week 3-4)

#### 4.3.1 Lock Manager

```go
// internal/transaction/lock.go
package transaction

import (
    "sync"
    "time"
)

// LockType defines the type of lock
type LockType int

const (
    LockNone LockType = iota
    LockShared          // S lock - for reading
    LockUpdate          // U lock - for read with intent to update
    LockExclusive       // X lock - for writing
    LockIntentShared    // IS lock - intent to acquire S on finer granularity
    LockIntentExclusive // IX lock - intent to acquire X on finer granularity
)

// LockManager handles lock acquisition and deadlock detection
type LockManager struct {
    mu          sync.RWMutex
    locks       map[string]*Lock // resource -> lock
    waitGraph   map[TxID]map[TxID]struct{} // deadlock detection
    txLocks     map[TxID]map[string]LockType // tx -> locks held
}

type Lock struct {
    Resource    string
    Holders     map[TxID]LockType
    WaitQueue   []LockRequest
    mu          sync.Mutex
}

type LockRequest struct {
    TxID      TxID
    Type      LockType
    Timestamp time.Time
    Granted   chan bool
}

func NewLockManager() *LockManager {
    return &LockManager{
        locks:     make(map[string]*Lock),
        waitGraph: make(map[TxID]map[TxID]struct{}),
        txLocks:   make(map[TxID]map[string]LockType),
    }
}

// Acquire attempts to acquire a lock
func (lm *LockManager) Acquire(txID TxID, resource string, lockType LockType, timeout time.Duration) error {
    lm.mu.Lock()
    
    lock, exists := lm.locks[resource]
    if !exists {
        lock = &Lock{
            Resource: resource,
            Holders:  make(map[TxID]LockType),
        }
        lm.locks[resource] = lock
    }
    
    // Check if already holding compatible lock
    if existingType, held := lock.Holders[txID]; held {
        if isCompatible(existingType, lockType) || existingType >= lockType {
            // Already have compatible or stronger lock
            lm.mu.Unlock()
            return nil
        }
        // Need to upgrade lock
        if canUpgrade(existingType, lockType, lock) {
            lock.Holders[txID] = lockType
            lm.mu.Unlock()
            return nil
        }
    }
    
    // Check compatibility with other holders
    if !isCompatibleWithHolders(lockType, lock, txID) {
        // Must wait - add to wait queue
        granted := make(chan bool, 1)
        request := LockRequest{
            TxID:      txID,
            Type:      lockType,
            Timestamp: time.Now(),
            Granted:   granted,
        }
        lock.WaitQueue = append(lock.WaitQueue, request)
        
        // Update wait-for graph for deadlock detection
        lm.updateWaitGraph(txID, lock)
        
        // Check for deadlock
        if lm.detectDeadlock(txID) {
            // Remove from wait queue
            lm.removeFromWaitQueue(lock, txID)
            lm.mu.Unlock()
            return ErrDeadlockDetected
        }
        
        lm.mu.Unlock()
        
        // Wait for lock grant or timeout
        select {
        case <-granted:
            return nil
        case <-time.After(timeout):
            lm.mu.Lock()
            lm.removeFromWaitQueue(lock, txID)
            lm.mu.Unlock()
            return ErrLockTimeout
        }
    }
    
    // Grant lock immediately
    lock.Holders[txID] = lockType
    
    // Track locks held by transaction
    if lm.txLocks[txID] == nil {
        lm.txLocks[txID] = make(map[string]LockType)
    }
    lm.txLocks[txID][resource] = lockType
    
    lm.mu.Unlock()
    return nil
}

func isCompatible(holding, requesting LockType) bool {
    // Compatibility matrix
    switch holding {
    case LockShared:
        return requesting == LockShared || requesting == LockIntentShared
    case LockUpdate:
        return requesting == LockIntentShared
    case LockExclusive:
        return false
    default:
        return true
    }
}

func isCompatibleWithHolders(requesting LockType, lock *Lock, excludeTx TxID) bool {
    for tx, heldType := range lock.Holders {
        if tx == excludeTx {
            continue
        }
        if !isCompatible(heldType, requesting) {
            return false
        }
    }
    return true
}

func (lm *LockManager) detectDeadlock(start TxID) bool {
    visited := make(map[TxID]bool)
    return lm.hasCycle(start, visited, make(map[TxID]bool))
}

func (lm *LockManager) hasCycle(tx TxID, visited, recStack map[TxID]bool) bool {
    visited[tx] = true
    recStack[tx] = true
    
    if waitsFor, ok := lm.waitGraph[tx]; ok {
        for otherTx := range waitsFor {
            if !visited[otherTx] {
                if lm.hasCycle(otherTx, visited, recStack) {
                    return true
                }
            } else if recStack[otherTx] {
                return true
            }
        }
    }
    
    recStack[tx] = false
    return false
}

// ReleaseAll releases all locks held by a transaction
func (lm *LockManager) ReleaseAll(txID TxID) {
    lm.mu.Lock()
    defer lm.mu.Unlock()
    
    locks, ok := lm.txLocks[txID]
    if !ok {
        return
    }
    
    for resource := range locks {
        lm.release(txID, resource)
    }
    
    delete(lm.txLocks, txID)
    delete(lm.waitGraph, txID)
}

func (lm *LockManager) release(txID TxID, resource string) {
    lock, exists := lm.locks[resource]
    if !exists {
        return
    }
    
    delete(lock.Holders, txID)
    
    // Grant locks to waiting transactions
    for len(lock.WaitQueue) > 0 {
        next := lock.WaitQueue[0]
        if isCompatibleWithHolders(next.Type, lock, "") {
            lock.Holders[next.TxID] = next.Type
            lock.WaitQueue = lock.WaitQueue[1:]
            close(next.Granted)
        } else {
            break
        }
    }
    
    // Clean up empty locks
    if len(lock.Holders) == 0 && len(lock.WaitQueue) == 0 {
        delete(lm.locks, resource)
    }
}
```

### Phase 4: Transactional Storage Adapter (Week 4-5)

```go
// internal/storage/transactional.go
package storage

import (
    "aidb/internal/document"
    "aidb/internal/transaction"
)

// TransactionalStorage wraps a storage engine with transaction support
type TransactionalStorage struct {
    underlying Storage
    manager    *transaction.Manager
}

func NewTransactionalStorage(storage Storage, manager *transaction.Manager) *TransactionalStorage {
    return &TransactionalStorage{
        underlying: storage,
        manager:    manager,
    }
}

// InsertTx performs an insert within a transaction
func (ts *TransactionalStorage) InsertTx(tx *transaction.Transaction, doc *document.Document) error {
    // Acquire exclusive lock
    if err := ts.manager.AcquireLock(tx.ID, doc.ID, transaction.LockExclusive, tx.Timeout); err != nil {
        return err
    }
    
    // Record operation
    op := transaction.Operation{
        Type:       transaction.OpInsert,
        Collection: ts.underlying.CollectionName(),
        DocumentID: doc.ID,
        NewValue:   doc,
    }
    
    // Check if document exists (for conflict detection)
    if existing, err := ts.underlying.Get(doc.ID); err == nil {
        op.OldValue = existing
        return ErrDocumentExists
    }
    
    return tx.AddOperation(op)
}

// UpdateTx performs an update within a transaction
func (ts *TransactionalStorage) UpdateTx(tx *transaction.Transaction, doc *document.Document) error {
    // Acquire exclusive lock
    if err := ts.manager.AcquireLock(tx.ID, doc.ID, transaction.LockExclusive, tx.Timeout); err != nil {
        return err
    }
    
    // Get current value for rollback
    oldDoc, err := ts.underlying.Get(doc.ID)
    if err != nil {
        return err
    }
    
    op := transaction.Operation{
        Type:       transaction.OpUpdate,
        Collection: ts.underlying.CollectionName(),
        DocumentID: doc.ID,
        OldValue:   oldDoc,
        NewValue:   doc,
    }
    
    return tx.AddOperation(op)
}

// DeleteTx performs a delete within a transaction
func (ts *TransactionalStorage) DeleteTx(tx *transaction.Transaction, id string) error {
    // Acquire exclusive lock
    if err := ts.manager.AcquireLock(tx.ID, id, transaction.LockExclusive, tx.Timeout); err != nil {
        return err
    }
    
    // Get current value for rollback
    oldDoc, err := ts.underlying.Get(id)
    if err != nil {
        return err
    }
    
    op := transaction.Operation{
        Type:       transaction.OpDelete,
        Collection: ts.underlying.CollectionName(),
        DocumentID: id,
        OldValue:   oldDoc,
    }
    
    return tx.AddOperation(op)
}

// GetTx performs a read within a transaction with proper isolation
func (ts *TransactionalStorage) GetTx(tx *transaction.Transaction, id string) (*document.Document, error) {
    // Check write set first (read-your-writes)
    if doc, ok := tx.WriteSet[ts.underlying.CollectionName()+":"+id]; ok {
        return doc, nil
    }
    
    // Acquire appropriate lock based on isolation level
    lockType := transaction.LockShared
    if tx.Isolation >= transaction.RepeatableRead {
        lockType = transaction.LockShared // Hold until end of transaction
    }
    
    if err := ts.manager.AcquireLock(tx.ID, id, lockType, tx.Timeout); err != nil {
        return nil, err
    }
    
    // Check read set for repeatable read
    if snapshot, ok := tx.ReadSet[id]; ok {
        return snapshot.Document, nil
    }
    
    // Read from storage
    doc, err := ts.underlying.Get(id)
    if err != nil {
        return nil, err
    }
    
    // Record in read set for repeatable read
    if tx.Isolation >= transaction.RepeatableRead {
        tx.ReadSet[id] = transaction.DocumentSnapshot{
            Document:  doc,
            Timestamp: time.Now().UnixNano(),
        }
    }
    
    return doc, nil
}

// FindTx performs a filtered query within a transaction
func (ts *TransactionalStorage) FindTx(tx *transaction.Transaction, filter map[string]interface{}) ([]*document.Document, error) {
    // For Serializable isolation, we need to lock the entire range
    if tx.Isolation == transaction.Serializable {
        // This is a simplified approach - production would use predicate locking
        if err := ts.manager.AcquireLock(tx.ID, "RANGE:"+hashFilter(filter), transaction.LockExclusive, tx.Timeout); err != nil {
            return nil, err
        }
    }
    
    // Query underlying storage
    docs, err := ts.underlying.Find(filter)
    if err != nil {
        return nil, err
    }
    
    // Apply write set visibility
    result := make([]*document.Document, 0, len(docs))
    for _, doc := range docs {
        // Check if modified in transaction
        if modified, ok := tx.WriteSet[ts.underlying.CollectionName()+":"+doc.ID]; ok {
            // Skip deleted documents
            if modified == nil {
                continue
            }
            result = append(result, modified)
        } else {
            result = append(result, doc)
        }
    }
    
    return result, nil
}
```

### Phase 5: Bulk Transactions (Week 5-6)

```go
// internal/transaction/bulk.go
package transaction

import (
    "context"
    "sync"
    "aidb/internal/document"
)

// BulkTransaction supports batch operations with atomicity
type BulkTransaction struct {
    *Transaction
    batchSize    int
    operations   chan Operation
    errors       chan error
    wg           sync.WaitGroup
    ctx          context.Context
    cancel       context.CancelFunc
}

// BulkOptions configures bulk transaction behavior
type BulkOptions struct {
    BatchSize       int
    WorkerCount     int
    ContinueOnError bool
    MaxRetries      int
}

func (m *Manager) BeginBulk(opts BulkOptions) (*BulkTransaction, error) {
    tx, err := m.Begin()
    if err != nil {
        return nil, err
    }
    
    ctx, cancel := context.WithCancel(context.Background())
    
    bt := &BulkTransaction{
        Transaction: tx,
        batchSize:   opts.BatchSize,
        operations:  make(chan Operation, opts.BatchSize*2),
        errors:      make(chan error, opts.BatchSize),
        ctx:         ctx,
        cancel:      cancel,
    }
    
    // Start worker pool
    for i := 0; i < opts.WorkerCount; i++ {
        bt.wg.Add(1)
        go bt.worker(i, opts)
    }
    
    return bt, nil
}

// Insert adds a document to the bulk insert batch
func (bt *BulkTransaction) Insert(collection string, doc *document.Document) error {
    select {
    case bt.operations <- Operation{
        Type:       OpInsert,
        Collection: collection,
        DocumentID: doc.ID,
        NewValue:   doc,
    }:
        return nil
    case <-bt.ctx.Done():
        return bt.ctx.Err()
    }
}

// worker processes operations in batches
func (bt *BulkTransaction) worker(id int, opts BulkOptions) {
    defer bt.wg.Done()
    
    batch := make([]Operation, 0, bt.batchSize)
    
    for {
        select {
        case op, ok := <-bt.operations:
            if !ok {
                // Process remaining batch
                if len(batch) > 0 {
                    bt.processBatch(batch, opts)
                }
                return
            }
            
            batch = append(batch, op)
            
            if len(batch) >= bt.batchSize {
                bt.processBatch(batch, opts)
                batch = make([]Operation, 0, bt.batchSize)
            }
            
        case <-bt.ctx.Done():
            return
        }
    }
}

func (bt *BulkTransaction) processBatch(batch []Operation, opts BulkOptions) {
    // Group operations by collection for efficiency
    byCollection := make(map[string][]Operation)
    for _, op := range batch {
        byCollection[op.Collection] = append(byCollection[op.Collection], op)
    }
    
    // Process each collection batch
    for collection, ops := range byCollection {
        // Acquire locks in sorted order to prevent deadlock
        lockKeys := make([]string, len(ops))
        for i, op := range ops {
            lockKeys[i] = op.DocumentID
        }
        sort.Strings(lockKeys)
        
        for _, key := range lockKeys {
            if err := bt.manager.AcquireLock(bt.ID, key, LockExclusive, bt.Timeout); err != nil {
                bt.errors <- fmt.Errorf("lock failed for %s: %w", key, err)
                if !opts.ContinueOnError {
                    bt.cancel()
                    return
                }
            }
        }
        
        // Record all operations in WAL
        for _, op := range ops {
            if err := bt.AddOperation(op); err != nil {
                bt.errors <- fmt.Errorf("operation failed: %w", err)
                if !opts.ContinueOnError {
                    bt.cancel()
                    return
                }
            }
        }
    }
}

// Commit commits the bulk transaction
func (bt *BulkTransaction) Commit() error {
    close(bt.operations)
    bt.wg.Wait()
    
    // Check for errors
    close(bt.errors)
    var errs []error
    for err := range bt.errors {
        errs = append(errs, err)
    }
    
    if len(errs) > 0 {
        bt.Rollback()
        return &BulkError{Errors: errs}
    }
    
    return bt.Transaction.Commit()
}

type BulkError struct {
    Errors []error
}

func (e *BulkError) Error() string {
    return fmt.Sprintf("bulk transaction failed with %d errors", len(e.Errors))
}
```

### Phase 6: Recovery Manager (Week 6-7)

```go
// internal/wal/recovery.go
package wal

import (
    "aidb/internal/transaction"
)

// RecoveryManager handles crash recovery
type RecoveryManager struct {
    wal           WAL
    storage       StorageAdapter
    checkpointDir string
}

func NewRecoveryManager(wal WAL, storage StorageAdapter, checkpointDir string) *RecoveryManager {
    return &RecoveryManager{
        wal:           wal,
        storage:       storage,
        checkpointDir: checkpointDir,
    }
}

// Recover performs recovery after crash
func (rm *RecoveryManager) Recover() error {
    // 1. Find last checkpoint
    checkpointLSN, err := rm.findLastCheckpoint()
    if err != nil {
        return err
    }
    
    // 2. Read all entries from checkpoint
    entries, err := rm.wal.Read(checkpointLSN)
    if err != nil {
        return err
    }
    
    // 3. Analyze phase - identify active transactions
    activeTxns, committedTxns, abortedTxns := rm.analyze(entries)
    
    // 4. Redo phase - replay committed transactions
    if err := rm.redo(entries, committedTxns); err != nil {
        return err
    }
    
    // 5. Undo phase - rollback uncommitted transactions
    if err := rm.undo(entries, activeTxns); err != nil {
        return err
    }
    
    return nil
}

func (rm *RecoveryManager) analyze(entries []*LogEntry) (active, committed, aborted map[string]bool) {
    active = make(map[string]bool)
    committed = make(map[string]bool)
    aborted = make(map[string]bool)
    
    for _, entry := range entries {
        switch entry.Type {
        case LogEntryBeginTx:
            active[entry.TxID] = true
        case LogEntryCommitTx:
            committed[entry.TxID] = true
            delete(active, entry.TxID)
        case LogEntryAbortTx:
            aborted[entry.TxID] = true
            delete(active, entry.TxID)
        }
    }
    
    return
}

func (rm *RecoveryManager) redo(entries []*LogEntry, committed map[string]bool) error {
    // Replay operations for committed transactions
    for _, entry := range entries {
        if !committed[entry.TxID] {
            continue
        }
        
        switch entry.Type {
        case LogEntryInsert:
            doc, _ := document.FromJSON(entry.NewValue)
            rm.storage.Insert(entry.Collection, doc)
        case LogEntryUpdate:
            doc, _ := document.FromJSON(entry.NewValue)
            rm.storage.Update(entry.Collection, doc)
        case LogEntryDelete:
            rm.storage.Delete(entry.Collection, string(entry.Key))
        }
    }
    return nil
}

func (rm *RecoveryManager) undo(entries []*LogEntry, active map[string]bool) error {
    // Process entries in reverse for uncommitted transactions
    for i := len(entries) - 1; i >= 0; i-- {
        entry := entries[i]
        if !active[entry.TxID] {
            continue
        }
        
        switch entry.Type {
        case LogEntryInsert:
            // Undo insert = delete
            rm.storage.Delete(entry.Collection, string(entry.Key))
        case LogEntryUpdate, LogEntryDelete:
            // Undo = restore old value
            if len(entry.OldValue) > 0 {
                doc, _ := document.FromJSON(entry.OldValue)
                rm.storage.InsertOrUpdate(entry.Collection, doc)
            }
        }
    }
    return nil
}
```

### Phase 7: Transactional Index Support (Week 7-8)

```go
// internal/index/transactional_index.go
package index

import (
    "aidb/internal/document"
    "aidb/internal/transaction"
)

// TransactionalIndex wraps an index with transaction support
type TransactionalIndex struct {
    underlying Index
    pending    map[TxID][]IndexOperation
    mu         sync.RWMutex
}

type IndexOperation struct {
    Type   IndexOpType
    Key    interface{}
    DocID  string
    OldKey interface{} // For updates
}

type IndexOpType int

const (
    IdxOpInsert IndexOpType = iota
    IdxOpDelete
    IdxOpUpdate
)

func (ti *TransactionalIndex) InsertTx(tx *transaction.Transaction, key interface{}, docID string) error {
    ti.mu.Lock()
    defer ti.mu.Unlock()
    
    // Don't apply immediately - queue for commit
    ti.pending[tx.ID] = append(ti.pending[tx.ID], IndexOperation{
        Type:  IdxOpInsert,
        Key:   key,
        DocID: docID,
    })
    
    return nil
}

func (ti *TransactionalIndex) CommitTx(txID TxID) error {
    ti.mu.Lock()
    defer ti.mu.Unlock()
    
    ops, ok := ti.pending[txID]
    if !ok {
        return nil
    }
    
    // Apply all pending operations
    for _, op := range ops {
        switch op.Type {
        case IdxOpInsert:
            ti.underlying.Insert(op.Key, op.DocID)
        case IdxOpDelete:
            ti.underlying.Delete(op.Key, op.DocID)
        case IdxOpUpdate:
            ti.underlying.Delete(op.OldKey, op.DocID)
            ti.underlying.Insert(op.Key, op.DocID)
        }
    }
    
    delete(ti.pending, txID)
    return nil
}

func (ti *TransactionalIndex) RollbackTx(txID TxID) error {
    ti.mu.Lock()
    defer ti.mu.Unlock()
    
    // Simply discard pending operations
    delete(ti.pending, txID)
    return nil
}
```

### Phase 8: REST API for Transactions (Week 8-9)

```go
// internal/api/transaction_handlers.go
package api

import (
    "net/http"
    "aidb/internal/transaction"
)

// TransactionRequest starts a new transaction
type BeginTransactionRequest struct {
    Isolation string `json:"isolation,omitempty"` // read_committed, repeatable_read, serializable
    Timeout   int    `json:"timeout,omitempty"`   // seconds
}

type TransactionResponse struct {
    TransactionID string `json:"transactionId"`
    State         string `json:"state"`
    StartTime     int64  `json:"startTime"`
}

// BeginTransaction handles POST /api/v1/transactions
func (h *Handler) BeginTransaction(w http.ResponseWriter, r *http.Request) {
    var req BeginTransactionRequest
    if err := parseJSONBody(r, &req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid request")
        return
    }
    
    // Parse isolation level
    isolation := transaction.ReadCommitted
    switch req.Isolation {
    case "read_uncommitted":
        isolation = transaction.ReadUncommitted
    case "repeatable_read":
        isolation = transaction.RepeatableRead
    case "serializable":
        isolation = transaction.Serializable
    }
    
    timeout := 30 * time.Second
    if req.Timeout > 0 {
        timeout = time.Duration(req.Timeout) * time.Second
    }
    
    tx, err := h.txManager.Begin(
        transaction.WithIsolation(isolation),
        transaction.WithTimeout(timeout),
    )
    if err != nil {
        writeError(w, http.StatusServiceUnavailable, err.Error())
        return
    }
    
    writeSuccess(w, TransactionResponse{
        TransactionID: string(tx.ID),
        State:         "active",
        StartTime:     tx.StartTime.Unix(),
    })
}

// CommitTransaction handles POST /api/v1/transactions/{id}/commit
func (h *Handler) CommitTransaction(w http.ResponseWriter, r *http.Request) {
    txID := r.PathValue("id")
    
    tx, err := h.txManager.GetTransaction(transaction.TxID(txID))
    if err != nil {
        writeError(w, http.StatusNotFound, "transaction not found")
        return
    }
    
    if err := h.txManager.Commit(tx); err != nil {
        writeError(w, http.StatusConflict, err.Error())
        return
    }
    
    writeSuccess(w, map[string]string{
        "transactionId": txID,
        "state":         "committed",
    })
}

// RollbackTransaction handles POST /api/v1/transactions/{id}/rollback
func (h *Handler) RollbackTransaction(w http.ResponseWriter, r *http.Request) {
    txID := r.PathValue("id")
    
    var req struct {
        Savepoint string `json:"savepoint,omitempty"`
    }
    parseJSONBody(r, &req) // Optional
    
    tx, err := h.txManager.GetTransaction(transaction.TxID(txID))
    if err != nil {
        writeError(w, http.StatusNotFound, "transaction not found")
        return
    }
    
    if req.Savepoint != "" {
        // Rollback to savepoint
        if err := tx.RollbackToSavepoint(req.Savepoint); err != nil {
            writeError(w, http.StatusBadRequest, err.Error())
            return
        }
        writeSuccess(w, map[string]string{
            "transactionId": txID,
            "state":         "active",
            "savepoint":     req.Savepoint,
        })
        return
    }
    
    // Full rollback
    if err := h.txManager.Rollback(tx); err != nil {
        writeError(w, http.StatusInternalServerError, err.Error())
        return
    }
    
    writeSuccess(w, map[string]string{
        "transactionId": txID,
        "state":         "aborted",
    })
}

// CreateSavepoint handles POST /api/v1/transactions/{id}/savepoints
func (h *Handler) CreateSavepoint(w http.ResponseWriter, r *http.Request) {
    txID := r.PathValue("id")
    
    var req struct {
        Name string `json:"name"`
    }
    if err := parseJSONBody(r, &req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid request")
        return
    }
    
    tx, err := h.txManager.GetTransaction(transaction.TxID(txID))
    if err != nil {
        writeError(w, http.StatusNotFound, "transaction not found")
        return
    }
    
    if err := tx.CreateSavepoint(req.Name); err != nil {
        writeError(w, http.StatusBadRequest, err.Error())
        return
    }
    
    writeSuccess(w, map[string]string{
        "transactionId": txID,
        "savepoint":     req.Name,
    })
}

// TransactionalDocumentInsert handles POST /api/v1/transactions/{id}/documents
func (h *Handler) TransactionalDocumentInsert(w http.ResponseWriter, r *http.Request) {
    txID := r.PathValue("id")
    collectionName := r.PathValue("name")
    
    tx, err := h.txManager.GetTransaction(transaction.TxID(txID))
    if err != nil {
        writeError(w, http.StatusNotFound, "transaction not found")
        return
    }
    
    var req InsertDocumentRequest
    if err := parseJSONBody(r, &req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid request")
        return
    }
    
    col, err := h.collectionManager.GetCollection(collectionName)
    if err != nil {
        writeError(w, http.StatusNotFound, err.Error())
        return
    }
    
    // Create document
    var doc *document.Document
    if req.ID != "" {
        doc = document.NewDocumentWithID(req.ID, req.Data)
    } else {
        doc = document.NewDocument(req.Data)
    }
    
    // Perform transactional insert
    if err := col.InsertTx(tx, doc); err != nil {
        writeError(w, http.StatusConflict, err.Error())
        return
    }
    
    writeJSON(w, http.StatusCreated, Response{
        Success: true,
        Data:    doc,
    })
}

// BulkTransaction handles POST /api/v1/transactions/bulk
func (h *Handler) BulkTransaction(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Operations []BulkOperation `json:"operations"`
        Options    BulkOptions     `json:"options,omitempty"`
    }
    if err := parseJSONBody(r, &req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid request")
        return
    }
    
    bt, err := h.txManager.BeginBulk(req.Options)
    if err != nil {
        writeError(w, http.StatusServiceUnavailable, err.Error())
        return
    }
    
    // Queue all operations
    for _, op := range req.Operations {
        switch op.Type {
        case "insert":
            doc := document.NewDocumentWithID(op.ID, op.Data)
            if err := bt.Insert(op.Collection, doc); err != nil {
                bt.Rollback()
                writeError(w, http.StatusConflict, err.Error())
                return
            }
        case "update":
            // Similar pattern
        case "delete":
            // Similar pattern
        }
    }
    
    // Commit
    if err := bt.Commit(); err != nil {
        writeError(w, http.StatusConflict, err.Error())
        return
    }
    
    writeSuccess(w, map[string]interface{}{
        "state":     "committed",
        "processed": len(req.Operations),
    })
}
```

---

## 5. API Endpoints Summary

### Transaction Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/transactions` | Begin new transaction |
| GET | `/api/v1/transactions/{id}` | Get transaction status |
| POST | `/api/v1/transactions/{id}/commit` | Commit transaction |
| POST | `/api/v1/transactions/{id}/rollback` | Rollback transaction |
| POST | `/api/v1/transactions/{id}/savepoints` | Create savepoint |
| POST | `/api/v1/transactions/{id}/rollback` | Rollback to savepoint (with body) |
| POST | `/api/v1/transactions/bulk` | Execute bulk transaction |

### Transactional Document Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/transactions/{id}/collections/{name}/documents` | Insert document in transaction |
| PUT | `/api/v1/transactions/{id}/collections/{name}/documents/{docId}` | Update document in transaction |
| DELETE | `/api/v1/transactions/{id}/collections/{name}/documents/{docId}` | Delete document in transaction |
| GET | `/api/v1/transactions/{id}/collections/{name}/documents/{docId}` | Get document with isolation |

---

## 6. Testing Strategy

### 6.1 Unit Tests
- WAL write/read/rotation
- Lock manager compatibility matrix
- Transaction state machine
- Recovery procedures

### 6.2 Integration Tests
- Cross-collection transactions
- Concurrent transaction isolation
- Crash recovery simulation
- Bulk transaction throughput

### 6.3 Concurrency Tests
- Deadlock detection and resolution
- Lock contention scenarios
- MVCC snapshot consistency
- Phantom read prevention

---

## 7. Migration Path

### Phase 1: WAL Infrastructure (v0.6.0)
- Implement WAL writer/reader
- Add checkpoint mechanism
- No breaking changes

### Phase 2: Basic Transactions (v0.7.0)
- Single-document transactions
- Read Committed isolation
- Optional opt-in

### Phase 3: Full ACID (v0.8.0)
- Multi-document transactions
- All isolation levels
- Savepoints

### Phase 4: Advanced Features (v0.9.0)
- Bulk transactions
- Distributed transactions
- Online backups with consistency

---

## 8. Performance Considerations

| Feature | Expected Overhead | Mitigation |
|---------|------------------|------------|
| WAL writes | ~10-20% | Batch writes, async sync |
| Lock acquisition | ~5-15% | Lock elision for single-threaded |
| MVCC snapshots | ~20-30% memory | Incremental cleanup |
| Two-phase commit | ~50-100% latency | Async commit acknowledgment |

---

## 9. Configuration Options

```yaml
# Example configuration
transactions:
  enabled: true
  default_isolation: "read_committed"
  default_timeout: "30s"
  max_active: 1000
  
wal:
  enabled: true
  directory: "./aidb_data/wal"
  segment_size: "100MB"
  sync_policy: "on_commit"  # on_write, on_commit, async
  retention: "7d"
  
locking:
  deadlock_detection: true
  deadlock_timeout: "5s"
  wait_timeout: "30s"
  
checkpoint:
  interval: "5m"
  min_changes: 10000
```

---

## 10. Conclusion

This implementation plan provides a roadmap to add comprehensive ACID transaction support to AIDB while maintaining backward compatibility and performance. The modular design allows incremental adoption and testing at each phase.

Key design decisions:
1. **WAL-first approach** ensures durability and enables recovery
2. **Pluggable isolation levels** allow performance/consistency tradeoffs
3. **Lock manager with deadlock detection** ensures progress
4. **MVCC for snapshots** enables non-blocking reads
5. **Bulk transaction support** maintains high throughput for batch operations
