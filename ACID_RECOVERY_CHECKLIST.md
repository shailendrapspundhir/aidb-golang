# AIDB ACID & Recovery Implementation Checklist

> **Purpose:** Track progress on extending AIDB with stronger ACID compliance, crash recovery,
> multi-document/cross-collection transactions, isolation levels, conflict detection, and concurrency.
>
> **Status Legend:**
> - `[ ]` — Not started
> - `[~]` — In progress
> - `[x]` — Complete
> - `[!]` — Blocked / needs discussion
> - `[-]` — Skipped / deferred

---

## P0 — Critical: Fix Rollback & Crash Recovery

These are correctness and durability bugs that must be resolved before any other work.

### 0.1 Fix Broken Rollback (Storage Writes Are Immediate)

**Problem:** `InsertTx`, `UpdateTx`, `DeleteTx` in `internal/collection/collection.go` apply writes
to BoltDB/storage immediately inside the transaction. If the transaction rolls back, the `Rollback()`
method only writes an ABORT record to the WAL — it does NOT undo storage changes. This means
rolled-back data persists in storage.

Choose ONE of Approach A or Approach B, then complete its sub-items.

#### Approach A: Deferred Writes (Recommended)

- [ ] **0.1.A.1** — Design a per-transaction write-buffer that holds all pending Insert/Update/Delete
      operations without touching storage until `Commit`.
- [ ] **0.1.A.2** — Modify `Transaction` struct to hold a `pendingWrites []PendingWrite` buffer
      alongside the existing `writeSet`.
- [ ] **0.1.A.3** — Modify `InsertTx` in `collection.go` to buffer the insert in the transaction
      instead of calling `c.storage.Insert(doc)`.
- [ ] **0.1.A.4** — Modify `UpdateTx` in `collection.go` to buffer the update in the transaction
      instead of calling `c.storage.Update(doc)`.
- [ ] **0.1.A.5** — Modify `DeleteTx` in `collection.go` to buffer the delete in the transaction
      instead of calling `c.storage.Delete(id)`.
- [ ] **0.1.A.6** — Modify `PatchWithAutoTx` to use the deferred-write model.
- [ ] **0.1.A.7** — Implement read-through logic: reads within a transaction must first check the
      write-buffer (for uncommitted local writes), then fall back to storage.
- [ ] **0.1.A.8** — Implement a `flushWrites(tx *Transaction)` method on the Manager/Coordinator
      that applies all buffered writes to storage atomically during `Commit`.
- [ ] **0.1.A.9** — Ensure index updates (B-tree, Hash, full-text) are also deferred to commit.
- [ ] **0.1.A.10** — On `Rollback`, simply discard the write-buffer (no storage undo needed).
- [ ] **0.1.A.11** — Write unit tests: begin tx → insert → rollback → verify document is NOT in storage.
- [ ] **0.1.A.12** — Write unit tests: begin tx → insert A → update B → commit → verify both in storage.

#### Approach B: Compensating Undo (Simpler Alternative)

- [ ] **0.1.B.1** — Implement `undoOperation(op Operation)` on the Manager that reverses a single
      storage operation using its `OldValue`.
- [ ] **0.1.B.2** — Implement `undoAllOperations(tx *Transaction)` that iterates operations in
      reverse order and calls `undoOperation` for each.
- [ ] **0.1.B.3** — Call `undoAllOperations` from `Manager.Rollback()` before writing the ABORT record.
- [ ] **0.1.B.4** — Handle undo for Insert (→ Delete), Update (→ restore OldValue), Delete (→ re-Insert OldValue).
- [ ] **0.1.B.5** — Handle undo for index operations (revert B-tree/Hash/full-text changes).
- [ ] **0.1.B.6** — Write unit tests: begin tx → insert → rollback → verify document is NOT in storage.
- [ ] **0.1.B.7** — Write unit tests: begin tx → update → rollback → verify original value restored.

---

### 0.2 Implement Crash Recovery (RecoveryManager)

**Problem:** The WAL faithfully records operations with before/after values, but nothing reads the
WAL on startup to replay or undo incomplete transactions. A crash mid-transaction leaves storage in
an inconsistent state.

#### 0.2.1 StorageAdapter Interface for Recovery

- [ ] **0.2.1.1** — Define a `StorageAdapter` interface that the recovery manager can use to write
      back to any collection:
      ```
      InsertOrUpdate(collection string, doc *document.Document) error
      Delete(collection string, docID string) error
      ```
- [ ] **0.2.1.2** — Implement the `StorageAdapter` using the existing `collection.Manager` as the
      backing store.
- [ ] **0.2.1.3** — Ensure the adapter can operate on vector collections as well (for vector
      WAL entries).

#### 0.2.2 RecoveryManager Core (new file: `internal/wal/recovery.go`)

- [ ] **0.2.2.1** — Create `RecoveryManager` struct with references to WAL and StorageAdapter.
- [ ] **0.2.2.2** — Implement WAL scan: read all entries from the last checkpoint (or start of WAL)
      forward. Build a map of `txID → []LogEntry` and `txID → finalState`.
- [ ] **0.2.2.3** — Classify transactions:
      - `COMMITTED` — has a `LogEntryCommitTx` record.
      - `ABORTED` — has a `LogEntryAbortTx` record.
      - `IN-FLIGHT` — has a `LogEntryBeginTx` but no commit or abort (was active at crash time).
- [ ] **0.2.2.4** — Implement **REDO phase**: for COMMITTED transactions, re-apply their operations
      to storage idempotently (using `NewValue`). Skip if data already matches.
- [ ] **0.2.2.5** — Implement **UNDO phase**: for IN-FLIGHT transactions, revert their operations
      using `OldValue`. Apply in reverse order.
- [ ] **0.2.2.6** — Write `ABORT` records to WAL for all IN-FLIGHT transactions after undo.
- [ ] **0.2.2.7** — Return a `RecoveryResult` with counts: committed replayed, aborted, in-flight undone.
- [ ] **0.2.2.8** — Handle corrupted WAL entries gracefully (CRC mismatch → skip entry, log warning).
- [ ] **0.2.2.9** — Handle partial/truncated entries at end of WAL segment (common after crash).

#### 0.2.3 Integrate Recovery into Startup

- [ ] **0.2.3.1** — In `main.go`, after creating the WAL but BEFORE creating the transaction
      manager or accepting HTTP requests, call `recoveryMgr.Recover()`.
- [ ] **0.2.3.2** — Log recovery results: number of transactions recovered, time taken.
- [ ] **0.2.3.3** — If recovery fails, log a fatal error and refuse to start (data may be corrupt).
- [ ] **0.2.3.4** — Add a `--skip-recovery` flag or env var `AIDB_SKIP_RECOVERY` for emergency bypass.

#### 0.2.4 Recovery Tests

- [ ] **0.2.4.1** — Unit test: simulate crash after BEGIN + INSERT (no COMMIT) → recover → verify
      document is NOT in storage.
- [ ] **0.2.4.2** — Unit test: simulate crash after BEGIN + INSERT + COMMIT → recover → verify
      document IS in storage.
- [ ] **0.2.4.3** — Unit test: simulate crash after BEGIN + INSERT + UPDATE (no COMMIT) → recover →
      verify original state restored.
- [ ] **0.2.4.4** — Unit test: simulate crash with multiple interleaved transactions → verify
      correct REDO/UNDO per transaction.
- [ ] **0.2.4.5** — Unit test: recovery with corrupted WAL entry → verify graceful handling.
- [ ] **0.2.4.6** — Integration test: start server → write data → kill process → restart → verify
      data consistency.

---

## P1 — High: Cross-Collection Transactions, Isolation, Conflict Detection

### 1.1 Multi-Document / Cross-Collection Transactions

**Problem:** No mechanism to atomically modify documents across multiple collections. A "transfer"
operation (debit collection A, credit collection B) can leave partial state on failure.

#### 1.1.1 Transaction Coordinator (new file: `internal/transaction/coordinator.go`)

- [ ] **1.1.1.1** — Define `TransactionCoordinator` struct that holds references to the
      `collection.Manager`, `transaction.Manager`, and WAL.
- [ ] **1.1.1.2** — Implement `BeginCrossCollection(opts ...BeginOption) (*Transaction, error)`
      that starts a transaction spanning multiple collections.
- [ ] **1.1.1.3** — Implement `AddCollectionOperation(tx, collectionName, operation)` that
      queues an operation against a specific collection within the transaction.
- [ ] **1.1.1.4** — Implement cross-collection `Commit(tx)` that:
      a. Validates all operations across all involved collections.
      b. Acquires locks on all involved collections (in deterministic order to prevent deadlocks).
      c. Applies all writes atomically (via BoltDB's cross-bucket transaction if applicable).
      d. Releases all locks.
- [ ] **1.1.1.5** — Implement cross-collection `Rollback(tx)` that undoes/discards all
      buffered writes across all collections.
- [ ] **1.1.1.6** — Leverage BoltDB's native cross-bucket atomicity: funnel all writes through
      a single `db.Update(func(tx *bolt.Tx) { ... })` call that touches multiple buckets.

#### 1.1.2 API Layer for Cross-Collection Transactions

- [ ] **1.1.2.1** — Extend `POST /api/v1/transactions/begin` to accept an optional
      `collections` array specifying which collections will be involved.
- [ ] **1.1.2.2** — Extend `POST /api/v1/transactions/{id}/operations` to accept a `collection`
      field in the request body, allowing operations against different collections.
- [ ] **1.1.2.3** — Ensure `POST /api/v1/transactions/{id}/commit` triggers the coordinator's
      cross-collection commit logic.
- [ ] **1.1.2.4** — Return clear error messages when cross-collection commit fails (which
      collection, which operation, why).

#### 1.1.3 Cross-Collection Transaction Tests

- [ ] **1.1.3.1** — Test: insert into collection A + insert into collection B → commit → both present.
- [ ] **1.1.3.2** — Test: insert into collection A + insert into collection B → rollback → neither present.
- [ ] **1.1.3.3** — Test: insert into A succeeds, insert into B fails validation → entire tx rolls back,
      A is clean.
- [ ] **1.1.3.4** — Test: concurrent cross-collection transactions on overlapping collections.

---

### 1.2 Enforce Isolation Levels

**Problem:** `IsolationLevel` is defined as an enum but never enforced. The `readSet` is never
populated. Concurrent transactions can see each other's intermediate state (dirty reads).

#### 1.2.1 Document Versioning

- [ ] **1.2.1.1** — Add `Version uint64` field to `document.Document` struct.
- [ ] **1.2.1.2** — Increment `Version` on every successful write (Insert sets to 1, Update increments).
- [ ] **1.2.1.3** — Persist `Version` in BoltDB alongside the document.
- [ ] **1.2.1.4** — Update `Document.ToJSON()` / `FromJSON()` to include Version.
- [ ] **1.2.1.5** — Update all storage backends (BoltDB, Memory, Hybrid) to handle Version.

#### 1.2.2 Read Set Tracking

- [ ] **1.2.2.1** — Define a `ReadEntry` struct: `{ Collection, DocID, Version, Timestamp }`.
- [ ] **1.2.2.2** — Populate `readSet` in `Transaction` on every read within a transaction
      (`Get`, `Find`, etc.).
- [ ] **1.2.2.3** — For `ReadCommitted`: verify at read-time that the document is not locked by
      another uncommitted transaction (or use the deferred-write model where uncommitted writes
      aren't visible).
- [ ] **1.2.2.4** — For `RepeatableRead`: record the version at first read; on subsequent reads
      of the same document, return the same version (snapshot).
- [ ] **1.2.2.5** — For `Serializable`: at commit time, verify that every entry in the readSet
      still has the same version (no other transaction has modified it).

#### 1.2.3 Snapshot Manager (new file: `internal/transaction/snapshot.go`)

- [ ] **1.2.3.1** — Create `SnapshotManager` struct that tracks the global commit counter / LSN.
- [ ] **1.2.3.2** — On `Begin`, record the current commit LSN as the transaction's `snapshotLSN`.
- [ ] **1.2.3.3** — Implement `IsVisible(docVersion, docCommitLSN, txSnapshotLSN) bool` that
      determines whether a document version is visible to a transaction.
- [ ] **1.2.3.4** — Integrate snapshot visibility into read paths.

#### 1.2.4 MVCC Storage (new file: `internal/storage/mvcc.go`) — Optional/Advanced

- [ ] **1.2.4.1** — Design multi-version document storage: key = `(docID, version)`.
- [ ] **1.2.4.2** — Implement version-aware `Get(docID, snapshotLSN)` that returns the latest
      version visible to the given snapshot.
- [ ] **1.2.4.3** — Implement garbage collection: remove old versions that are no longer visible
      to any active transaction.
- [ ] **1.2.4.4** — Integrate MVCC storage as an optional layer between Collection and Storage.

#### 1.2.5 Isolation Level Tests

- [ ] **1.2.5.1** — Test `ReadCommitted`: tx1 writes but doesn't commit → tx2 reads → must NOT
      see tx1's write.
- [ ] **1.2.5.2** — Test `ReadCommitted`: tx1 writes and commits → tx2 reads → must see tx1's write.
- [ ] **1.2.5.3** — Test `RepeatableRead`: tx1 reads doc → tx2 updates and commits → tx1 reads
      again → must see original value.
- [ ] **1.2.5.4** — Test `Serializable`: tx1 reads range → tx2 inserts into range and commits →
      tx1 commits → must fail (phantom detected).
- [ ] **1.2.5.5** — Test isolation with concurrent goroutines (not just sequential).

---

### 1.3 Conflict Detection

**Problem:** Two concurrent transactions can modify the same document and both commit — last writer
silently wins. No conflict errors are ever raised.

#### 1.3.1 Optimistic Concurrency Control (OCC) — Primary Strategy

- [ ] **1.3.1.1** — Create `internal/transaction/conflict.go` with conflict detection logic.
- [ ] **1.3.1.2** — At commit time, for every entry in the `writeSet`, read the current version
      of the document from storage.
- [ ] **1.3.1.3** — Compare current version with the version recorded when the transaction first
      read/touched the document.
- [ ] **1.3.1.4** — If versions differ → return `ErrWriteConflict` and abort the transaction.
- [ ] **1.3.1.5** — Define `ErrWriteConflict` error type with details: which document, which
      collection, expected version, actual version.
- [ ] **1.3.1.6** — Expose conflict errors through the API with HTTP 409 Conflict status.
- [ ] **1.3.1.7** — Optionally support automatic retry: if conflict detected, re-read and
      re-execute the transaction function (configurable max retries).

#### 1.3.2 Pessimistic Locking — Lock Manager (new file: `internal/transaction/lock.go`)

- [ ] **1.3.2.1** — Define `LockType` enum: `LockNone`, `LockShared`, `LockExclusive`,
      `LockIntentShared`, `LockIntentExclusive`.
- [ ] **1.3.2.2** — Define lock compatibility matrix (S-S compatible, S-X incompatible, etc.).
- [ ] **1.3.2.3** — Implement `LockManager` struct with:
      - `locks map[string]*Lock` (resource → lock state)
      - `txLocks map[TxID]map[string]LockType` (transaction → held locks)
      - `waitQueue` per lock resource
- [ ] **1.3.2.4** — Implement `Acquire(txID, resource, lockType, timeout) error`.
- [ ] **1.3.2.5** — Implement `Release(txID, resource)` and `ReleaseAll(txID)`.
- [ ] **1.3.2.6** — Implement lock upgrade (Shared → Exclusive) with deadlock awareness.
- [ ] **1.3.2.7** — Integrate lock acquisition into `InsertTx`, `UpdateTx`, `DeleteTx` for
      `Serializable` isolation.
- [ ] **1.3.2.8** — Release all locks on `Commit` or `Rollback`.

#### 1.3.3 Deadlock Detection

- [ ] **1.3.3.1** — Maintain a wait-for graph: `txA → txB` when txA is waiting for a lock held by txB.
- [ ] **1.3.3.2** — Implement cycle detection using DFS on the wait-for graph.
- [ ] **1.3.3.3** — Check for deadlocks on every `Acquire` that must wait.
- [ ] **1.3.3.4** — On deadlock, abort the youngest transaction (or the one with fewest operations).
- [ ] **1.3.3.5** — Define `ErrDeadlockDetected` error type.
- [ ] **1.3.3.6** — Write test: tx1 locks A then requests B, tx2 locks B then requests A → one
      must receive deadlock error.

#### 1.3.4 Conflict Detection Tests

- [ ] **1.3.4.1** — Test OCC: tx1 reads doc v1, tx2 updates doc to v2 and commits, tx1 tries to
      update → must get `ErrWriteConflict`.
- [ ] **1.3.4.2** — Test OCC: tx1 and tx2 both read doc v1, tx1 commits update, tx2 tries to
      commit update → tx2 must fail.
- [ ] **1.3.4.3** — Test lock timeout: tx1 holds exclusive lock, tx2 requests exclusive lock with
      1s timeout → must get `ErrLockTimeout`.
- [ ] **1.3.4.4** — Test concurrent goroutines: N goroutines increment a counter in a document →
      final value must be N (no lost updates).

---

## P2 — Medium: Checkpointing, Fine-Grained Concurrency, WAL Optimization, Vector Tx

### 2.1 Checkpoint Manager (new file: `internal/wal/checkpoint.go`)

- [ ] **2.1.1** — Define `CheckpointManager` struct with configurable interval (e.g., every 5 min
      or every N commits).
- [ ] **2.1.2** — Implement `CreateCheckpoint()` that:
      a. Records a `LogEntryCheckpoint` in the WAL with list of active transaction IDs.
      b. Flushes all dirty data to storage (`storage.Flush()`).
      c. Records the checkpoint LSN.
- [ ] **2.1.3** — Implement `TruncateBeforeCheckpoint()` that removes WAL segments whose max LSN
      is older than the oldest active transaction at checkpoint time.
- [ ] **2.1.4** — Run checkpoint as a background goroutine with configurable interval.
- [ ] **2.1.5** — Integrate with `RecoveryManager`: recovery only needs to scan from the last
      checkpoint, not the entire WAL.
- [ ] **2.1.6** — Add config options: `AIDB_CHECKPOINT_INTERVAL_SEC`, `AIDB_CHECKPOINT_ON_COMMIT_COUNT`.
- [ ] **2.1.7** — Write tests: create checkpoint → truncate → verify recovery still works from checkpoint.

---

### 2.2 Document-Level Locking (Replace Collection Mutex)

- [ ] **2.2.1** — Replace `sync.RWMutex` on `Collection` struct with a sharded document-level
      lock map.
- [ ] **2.2.2** — Implement a `DocumentLockMap` using `sync.Map` of per-document `sync.RWMutex`
      (or use the Lock Manager from §1.3.2).
- [ ] **2.2.3** — Multiple transactions can modify different documents in the same collection
      concurrently.
- [ ] **2.2.4** — Reads acquire shared locks; writes acquire exclusive locks.
- [ ] **2.2.5** — Benchmark: compare throughput of collection-level mutex vs document-level locking
      under concurrent load.
- [ ] **2.2.6** — Ensure collection-level operations (FindAll, Clear, Drop) still work correctly
      with document-level locks.

---

### 2.3 WAL Group Commit (Throughput Optimization)

- [ ] **2.3.1** — Implement a commit queue: `tx.Commit()` enqueues a commit request and waits on
      a channel.
- [ ] **2.3.2** — Create a WAL writer goroutine that collects pending commit requests (up to N or
      within T milliseconds).
- [ ] **2.3.3** — Batch-write all commit records, then `fsync` once for the entire batch.
- [ ] **2.3.4** — Notify all waiting transactions of success/failure.
- [ ] **2.3.5** — Add config option: `AIDB_WAL_GROUP_COMMIT_MAX_BATCH` and
      `AIDB_WAL_GROUP_COMMIT_DELAY_MS`.
- [ ] **2.3.6** — Benchmark: compare single-fsync-per-commit vs group commit throughput.

---

### 2.4 Transaction-Aware Vector Operations

**Problem:** `VectorCollection` has no concept of transactions. Vector inserts/updates/deletes go
directly to HNSW storage outside any transaction boundary.

- [ ] **2.4.1** — Add `InsertVectorTx(tx *Transaction, doc *VectorDocument) error` to
      `VectorCollection` that records the operation in the transaction's writeSet with
      `LogEntryVectorInsert`.
- [ ] **2.4.2** — Add `UpdateVectorTx(tx *Transaction, doc *VectorDocument) error`.
- [ ] **2.4.3** — Add `DeleteVectorTx(tx *Transaction, id string) error`.
- [ ] **2.4.4** — Add auto-transaction wrappers: `InsertVectorWithAutoTx`, etc.
- [ ] **2.4.5** — Ensure the `TransactionCoordinator` can handle mixed document + vector
      operations in a single transaction.
- [ ] **2.4.6** — Ensure WAL recovery handles `LogEntryVectorInsert/Update/Delete` entries.
- [ ] **2.4.7** — Write tests: insert document + insert its vector embedding in one tx → commit →
      both present. Rollback → neither present.

---

### 2.5 Transaction-Aware Index Operations

**Problem:** B-tree, Hash, and full-text indexes are standalone in-memory structures with their own
mutexes — no WAL, no rollback support. An index can become inconsistent with storage if a
transaction rolls back after index updates were applied.

- [ ] **2.5.1** — Create a `TransactionalIndex` wrapper (new file: `internal/storage/transactional_index.go`)
      that buffers index mutations in the transaction and only applies them on commit.
- [ ] **2.5.2** — On rollback, discard buffered index mutations.
- [ ] **2.5.3** — Integrate `TransactionalIndex` into `Collection.InsertTx`, `UpdateTx`, `DeleteTx`.
- [ ] **2.5.4** — Write tests: insert doc with indexed field → rollback → verify index does NOT
      contain the entry.
- [ ] **2.5.5** — Ensure full-text index updates are also deferred to commit.

---

## P3 — Lower Priority: Savepoints, Parallel Workers, Advanced Features

### 3.1 Savepoint Support

- [ ] **3.1.1** — Add `savepoints []Savepoint` field to `Transaction` struct. Each savepoint
      records `{ Name string, OperationIndex int }`.
- [ ] **3.1.2** — Implement `CreateSavepoint(name string) error` on `Transaction`.
- [ ] **3.1.3** — Implement `RollbackToSavepoint(name string) error` that truncates operations
      list and rebuilds writeSet.
- [ ] **3.1.4** — Implement `ReleaseSavepoint(name string) error`.
- [ ] **3.1.5** — Add API endpoints:
      - `POST /api/v1/transactions/{id}/savepoint` (create)
      - `POST /api/v1/transactions/{id}/savepoint/{name}/rollback` (rollback to)
- [ ] **3.1.6** — Write tests: create savepoint → do more ops → rollback to savepoint → verify
      only pre-savepoint ops remain.

---

### 3.2 Goroutine Worker Pools for Parallel Operations

- [ ] **3.2.1** — Implement a configurable worker pool using `errgroup.Group` for bulk/batch
      endpoints.
- [ ] **3.2.2** — Parallelize document validation in `BulkInsert` (validate N documents concurrently).
- [ ] **3.2.3** — Parallelize index updates after commit (B-tree, Hash, full-text, HNSW are
      independent of each other).
- [ ] **3.2.4** — Parallelize WAL segment reads during recovery.
- [ ] **3.2.5** — Add config option: `AIDB_WORKER_POOL_SIZE` (default: `runtime.NumCPU()`).
- [ ] **3.2.6** — Ensure goroutine safety: no shared mutable state without proper synchronization.

---

### 3.3 Crash Simulation Tests

- [ ] **3.3.1** — Create `internal/testing/crash_simulator.go` with helpers to simulate crashes
      at specific points (after WAL write, before storage flush, mid-commit, etc.).
- [ ] **3.3.2** — Test: crash after WAL BEGIN + operations but before COMMIT → recovery undoes all.
- [ ] **3.3.3** — Test: crash after WAL COMMIT but before storage flush → recovery redoes all.
- [ ] **3.3.4** — Test: crash during checkpoint → recovery handles partial checkpoint.
- [ ] **3.3.5** — Test: crash with concurrent transactions (some committed, some in-flight) →
      recovery handles each correctly.
- [ ] **3.3.6** — Test: repeated crash-recovery cycles → data remains consistent.
- [ ] **3.3.7** — Test: WAL file corruption (truncated, bit-flipped) → recovery skips corrupted
      entries and logs warnings.

---

### 3.4 Configuration Extensions

- [ ] **3.4.1** — Add `AIDB_DEFAULT_ISOLATION_LEVEL` config (default: `read_committed`).
- [ ] **3.4.2** — Add `AIDB_CONFLICT_DETECTION` config: `optimistic`, `pessimistic`, `none`.
- [ ] **3.4.3** — Add `AIDB_LOCK_TIMEOUT_MS` config (default: `5000`).
- [ ] **3.4.4** — Add `AIDB_MAX_RETRIES_ON_CONFLICT` config (default: `3`).
- [ ] **3.4.5** — Add `AIDB_CHECKPOINT_INTERVAL_SEC` config (default: `300`).
- [ ] **3.4.6** — Add `AIDB_RECOVERY_MODE` config: `automatic`, `manual`, `skip`.
- [ ] **3.4.7** — Add `AIDB_VECTOR_TX_ENABLED` config (default: `false` for backward compat).
- [ ] **3.4.8** — Document all new config options in README.md.

---

### 3.5 API Enhancements for Transaction Visibility

- [ ] **3.5.1** — Add `X-Isolation-Level` header support on `POST /api/v1/transactions/begin`.
- [ ] **3.5.2** — Include conflict details in error responses (HTTP 409):
      `{ "error": "write_conflict", "collection": "...", "documentId": "...", "expectedVersion": N, "actualVersion": M }`.
- [ ] **3.5.3** — Add `GET /api/v1/transactions/{id}/operations` endpoint to inspect queued
      operations.
- [ ] **3.5.4** — Add `GET /api/v1/system/recovery-status` endpoint for post-recovery inspection.
- [ ] **3.5.5** — Add `POST /api/v1/system/checkpoint` endpoint to trigger a manual checkpoint.
- [ ] **3.5.6** — Add transaction statistics to `GET /api/v1/health`: active tx count, conflict
      rate, avg commit time.

---

## New Files Summary

| File | Section | Purpose |
|------|---------|---------|
| `internal/transaction/coordinator.go` | §1.1.1 | Cross-collection transaction coordinator |
| `internal/transaction/conflict.go` | §1.3.1 | OCC conflict detection, version comparison |
| `internal/transaction/lock.go` | §1.3.2 | Lock manager with S/X locks, wait queues |
| `internal/transaction/snapshot.go` | §1.2.3 | MVCC snapshot management, visibility rules |
| `internal/transaction/savepoint.go` | §3.1 | Savepoint creation and partial rollback |
| `internal/wal/recovery.go` | §0.2.2 | ARIES-style REDO/UNDO recovery on startup |
| `internal/wal/checkpoint.go` | §2.1 | Periodic checkpoint writes and WAL truncation |
| `internal/storage/mvcc.go` | §1.2.4 | Multi-version document storage (optional) |
| `internal/storage/transactional_index.go` | §2.5 | Transaction-aware index wrapper |
| `internal/vector/transactional.go` | §2.4 | Transaction-aware vector operations |
| `internal/testing/crash_simulator.go` | §3.3 | Crash simulation test helpers |

---

## Existing File Modifications Summary

| File | Section(s) | Changes |
|------|------------|---------|
| `internal/document/document.go` | §1.2.1 | Add `Version uint64` field |
| `internal/transaction/transaction.go` | §0.1, §1.2.2, §3.1 | Add write-buffer / deferred writes, populate readSet, add savepoints |
| `internal/transaction/manager.go` | §0.1, §0.2, §1.3 | Integrate lock manager, conflict detection, recovery hookup, undo logic |
| `internal/collection/collection.go` | §0.1 | Change `InsertTx`/`UpdateTx`/`DeleteTx` to deferred-write or add compensating undo |
| `internal/vector/collection.go` | §2.4 | Add `InsertVectorTx`, `UpdateVectorTx`, `DeleteVectorTx` |
| `internal/storage/memory.go` | §1.2.1 | Handle document Version field |
| `internal/storage/boltdb.go` | §1.2.1 | Handle document Version field |
| `internal/storage/hybrid.go` | §1.2.1 | Handle document Version field |
| `internal/config/config.go` | §3.4 | Add new ACID/recovery config options |
| `internal/api/handlers.go` | §1.1.2, §3.5 | Cross-collection tx API, conflict responses, new endpoints |
| `main.go` | §0.2.3 | Add recovery step before accepting connections |

---

## Implementation Order (Recommended)

```
Phase 1: Fix Correctness
  §0.1  Fix broken rollback (choose Approach A or B)
  §0.2  Implement crash recovery

Phase 2: Core ACID
  §1.2.1  Document versioning
  §1.3.1  OCC conflict detection
  §1.2.2  Read set tracking
  §1.1    Cross-collection transactions

Phase 3: Isolation & Concurrency
  §1.2.3  Snapshot manager
  §1.2.5  Isolation level enforcement + tests
  §2.2    Document-level locking
  §1.3.2  Pessimistic lock manager (for Serializable)
  §1.3.3  Deadlock detection

Phase 4: Performance & Durability
  §2.1    Checkpoint manager
  §2.3    WAL group commit
  §3.2    Goroutine worker pools

Phase 5: Extended Scope
  §2.4    Transaction-aware vector operations
  §2.5    Transaction-aware index operations
  §3.1    Savepoints
  §3.3    Crash simulation tests
  §3.4    Configuration extensions
  §3.5    API enhancements
```
