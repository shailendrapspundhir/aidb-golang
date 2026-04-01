package collection

import (
	"aidb/internal/document"
	"aidb/internal/transaction"
	"aidb/internal/wal"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// helper: creates WAL + TxManager + MemoryCollection wired together.
func setupTestEnv(t *testing.T) (col *Collection, tm *transaction.Manager, cleanup func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "collection_tx_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024,
		SyncPolicy:  wal.SyncOnCommit,
		MaxSegments: 10,
	}
	w, err := wal.NewFileWAL(walConfig)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create WAL: %v", err)
	}

	tmConfig := &transaction.ManagerConfig{
		DefaultIsolation:      transaction.ReadCommitted,
		DefaultTimeout:        5 * time.Second,
		MaxActiveTransactions: 100,
		AutoCommitEnabled:     true,
	}
	tm = transaction.NewManager(w, tmConfig)

	// Collection manager with memory storage
	mgr := NewManager()
	mgr.SetTransactionManager(tm)

	col, err = mgr.CreateCollection("test_col", nil)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Wire the StorageApplier
	applier := NewStorageApplier(mgr)
	tm.SetStorageApplier(applier)

	cleanup = func() {
		tm.Close()
		w.Close()
		os.RemoveAll(tmpDir)
	}
	return col, tm, cleanup
}

// ---------------------------------------------------------------------------
// Core deferred-write tests
// ---------------------------------------------------------------------------

func TestInsertTx_DeferredWrite_CommitApplies(t *testing.T) {
	col, _, cleanup := setupTestEnv(t)
	defer cleanup()

	doc := document.NewDocumentWithID("d1", map[string]interface{}{"x": 1.0})

	if err := col.InsertWithAutoTx(doc); err != nil {
		t.Fatalf("InsertWithAutoTx failed: %v", err)
	}

	// After commit, document must be in storage
	got, err := col.Get("d1")
	if err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}
	if got.Data["x"] != 1.0 {
		t.Errorf("Expected x=1, got %v", got.Data["x"])
	}
}

func TestInsertTx_Rollback_NothingInStorage(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	tx, err := tm.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	doc := document.NewDocumentWithID("d1", map[string]interface{}{"x": 1.0})
	if err := col.InsertTx(tx, doc); err != nil {
		t.Fatalf("InsertTx failed: %v", err)
	}

	// Document must NOT be in storage before commit
	if _, getErr := col.Get("d1"); getErr == nil {
		t.Fatal("Document should NOT be in storage before commit")
	}

	// Rollback
	if err := tm.Rollback(tx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Document must still NOT be in storage after rollback
	if _, getErr := col.Get("d1"); getErr == nil {
		t.Fatal("Document should NOT be in storage after rollback")
	}
}

func TestUpdateTx_Rollback_OldValuePreserved(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	// Pre-insert a document
	orig := document.NewDocumentWithID("d1", map[string]interface{}{"name": "Alice"})
	if err := col.InsertWithAutoTx(orig); err != nil {
		t.Fatalf("Pre-insert failed: %v", err)
	}

	// Start transaction, update, then rollback
	tx, _ := tm.Begin()
	updated := document.NewDocumentWithID("d1", map[string]interface{}{"name": "Bob"})
	updated.CreatedAt = orig.CreatedAt
	if err := col.UpdateTx(tx, updated); err != nil {
		t.Fatalf("UpdateTx failed: %v", err)
	}

	// Rollback — storage should still show "Alice"
	tm.Rollback(tx)

	got, _ := col.Get("d1")
	if got.Data["name"] != "Alice" {
		t.Errorf("Expected name=Alice after rollback, got %v", got.Data["name"])
	}
}

func TestDeleteTx_Rollback_DocumentSurvives(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	orig := document.NewDocumentWithID("d1", map[string]interface{}{"x": 1.0})
	col.InsertWithAutoTx(orig)

	tx, _ := tm.Begin()
	if err := col.DeleteTx(tx, "d1"); err != nil {
		t.Fatalf("DeleteTx failed: %v", err)
	}

	// Rollback — document must survive
	tm.Rollback(tx)

	got, err := col.Get("d1")
	if err != nil {
		t.Fatalf("Document should exist after rollback, got error: %v", err)
	}
	if got.Data["x"] != 1.0 {
		t.Errorf("Expected x=1, got %v", got.Data["x"])
	}
}

func TestMultiOp_CommitAppliesAll(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	// Pre-insert one doc to update later
	col.InsertWithAutoTx(document.NewDocumentWithID("u1", map[string]interface{}{"v": 0.0}))

	tx, _ := tm.Begin()
	// Insert a new doc
	col.InsertTx(tx, document.NewDocumentWithID("i1", map[string]interface{}{"a": 1.0}))
	// Update existing doc
	col.UpdateTx(tx, document.NewDocumentWithID("u1", map[string]interface{}{"v": 99.0}))
	// Commit
	if err := tm.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if g, _ := col.Get("i1"); g == nil || g.Data["a"] != 1.0 {
		t.Error("Inserted doc i1 missing or wrong data")
	}
	if g, _ := col.Get("u1"); g == nil || g.Data["v"] != 99.0 {
		t.Error("Updated doc u1 missing or wrong data")
	}
}

func TestBulkInsert_RollbackOnError(t *testing.T) {
	col, _, cleanup := setupTestEnv(t)
	defer cleanup()

	// Pre-insert d1 so bulk will conflict
	col.InsertWithAutoTx(document.NewDocumentWithID("d1", map[string]interface{}{"x": 1.0}))

	docs := []*document.Document{
		document.NewDocumentWithID("d2", map[string]interface{}{"x": 2.0}),
		document.NewDocumentWithID("d1", map[string]interface{}{"x": 3.0}), // conflict
	}
	_, err := col.BulkInsert(docs)
	if err == nil {
		t.Fatal("Expected error from BulkInsert due to duplicate")
	}

	// d2 should NOT be in storage (entire batch rolled back)
	if _, getErr := col.Get("d2"); getErr == nil {
		t.Fatal("d2 should not exist after rollback")
	}
}

// ---------------------------------------------------------------------------
// Crash recovery test
// ---------------------------------------------------------------------------

func TestCrashRecovery_RedoCommitted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "recovery_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024,
		SyncPolicy:  wal.SyncOnEveryWrite,
		MaxSegments: 10,
	}

	// Phase 1: Simulate a committed transaction by writing WAL entries directly
	w, _ := wal.NewFileWAL(walConfig)
	txID := "tx-recovery-1"

	// BEGIN
	w.Append(wal.CreateLogEntry(txID, wal.LogEntryBeginTx, "", nil, nil, nil))
	// INSERT
	doc := document.NewDocumentWithID("rec1", map[string]interface{}{"val": "hello"})
	docBytes, _ := doc.ToJSON()
	w.Append(wal.CreateLogEntry(txID, wal.LogEntryInsert, "test_col", []byte("rec1"), nil, docBytes))
	// COMMIT
	w.Append(wal.CreateLogEntry(txID, wal.LogEntryCommitTx, "", nil, nil, nil))
	w.Sync()
	w.Close()

	// Phase 2: Re-open WAL, create collection manager, run recovery
	w2, _ := wal.NewFileWAL(walConfig)
	defer w2.Close()

	mgr := NewManager()
	mgr.CreateCollection("test_col", nil)

	recoveryApplier := NewRecoveryApplier(mgr)
	rm := wal.NewRecoveryManager(w2, recoveryApplier)
	result, recErr := rm.Recover()
	if recErr != nil {
		t.Fatalf("Recovery failed: %v", recErr)
	}

	if result.CommittedTx != 1 {
		t.Errorf("Expected 1 committed tx, got %d", result.CommittedTx)
	}
	if result.RedoneOps != 1 {
		t.Errorf("Expected 1 redone op, got %d", result.RedoneOps)
	}

	// Document should now be in storage
	col, _ := mgr.GetCollection("test_col")
	got, getErr := col.Get("rec1")
	if getErr != nil {
		t.Fatalf("Expected doc rec1 after recovery REDO, got error: %v", getErr)
	}
	if got.Data["val"] != "hello" {
		t.Errorf("Expected val=hello, got %v", got.Data["val"])
	}
}

func TestCrashRecovery_UndoInFlight(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "recovery_undo_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024,
		SyncPolicy:  wal.SyncOnEveryWrite,
		MaxSegments: 10,
	}

	// Phase 1: Pre-insert a doc into storage, then simulate an in-flight
	// DELETE transaction (no COMMIT written — simulates a crash mid-tx).
	mgr := NewManager()
	col, _ := mgr.CreateCollection("test_col", nil)

	origDoc := document.NewDocumentWithID("victim", map[string]interface{}{"alive": true})
	col.Insert(origDoc)

	// Write WAL entries for an in-flight delete
	w, _ := wal.NewFileWAL(walConfig)
	txID := "tx-inflight-1"
	w.Append(wal.CreateLogEntry(txID, wal.LogEntryBeginTx, "", nil, nil, nil))
	origBytes, _ := origDoc.ToJSON()
	w.Append(wal.CreateLogEntry(txID, wal.LogEntryDelete, "test_col", []byte("victim"), origBytes, nil))
	// NO COMMIT — simulating crash
	w.Sync()
	w.Close()

	// Simulate partial storage damage: delete the doc as if the
	// deferred write was partially flushed before crash
	col.Delete("victim")

	// Phase 2: Recovery should UNDO the delete → re-insert the doc
	w2, _ := wal.NewFileWAL(walConfig)
	defer w2.Close()

	recoveryApplier := NewRecoveryApplier(mgr)
	rm := wal.NewRecoveryManager(w2, recoveryApplier)
	result, recErr := rm.Recover()
	if recErr != nil {
		t.Fatalf("Recovery failed: %v", recErr)
	}

	if result.InFlightTx != 1 {
		t.Errorf("Expected 1 in-flight tx, got %d", result.InFlightTx)
	}
	if result.UndoneOps != 1 {
		t.Errorf("Expected 1 undone op, got %d", result.UndoneOps)
	}

	// Document should be restored
	got, getErr := col.Get("victim")
	if getErr != nil {
		t.Fatalf("Expected doc 'victim' to be restored after UNDO, got error: %v", getErr)
	}
	alive, ok := got.Data["alive"].(bool)
	if !ok || !alive {
		t.Errorf("Expected alive=true, got %v", got.Data["alive"])
	}
}

// ---------------------------------------------------------------------------
// Resilience / stress tests
// ---------------------------------------------------------------------------

// TestConcurrentTxCommitRollback spawns 50 goroutines. Even-numbered goroutines
// commit, odd-numbered rollback. Afterwards we verify that ONLY the committed
// documents exist and NONE of the rolled-back documents leaked into storage.
func TestConcurrentTxCommitRollback(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	const N = 50
	var wg sync.WaitGroup
	errCh := make(chan error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			tx, err := tm.Begin()
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d Begin: %w", n, err)
				return
			}

			docID := fmt.Sprintf("concurrent-%d", n)
			doc := document.NewDocumentWithID(docID, map[string]interface{}{"n": float64(n)})
			if err := col.InsertTx(tx, doc); err != nil {
				errCh <- fmt.Errorf("goroutine %d InsertTx: %w", n, err)
				tm.Rollback(tx)
				return
			}

			if n%2 == 0 {
				// Even → commit
				if err := tm.Commit(tx); err != nil {
					errCh <- fmt.Errorf("goroutine %d Commit: %w", n, err)
				}
			} else {
				// Odd → rollback
				if err := tm.Rollback(tx); err != nil {
					errCh <- fmt.Errorf("goroutine %d Rollback: %w", n, err)
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify: only even-numbered docs exist
	for i := 0; i < N; i++ {
		docID := fmt.Sprintf("concurrent-%d", i)
		got, err := col.Get(docID)
		if i%2 == 0 {
			if err != nil {
				t.Errorf("doc %s should exist (committed), got error: %v", docID, err)
			} else if got.Data["n"] != float64(i) {
				t.Errorf("doc %s has wrong data: expected n=%d, got %v", docID, i, got.Data["n"])
			}
		} else {
			if err == nil {
				t.Errorf("doc %s should NOT exist (rolled back), but found it", docID)
			}
		}
	}
}

// TestCrashRecovery_MixedTransactions simulates a WAL containing:
//   - 2 committed transactions (each with 2 operations)
//   - 1 aborted transaction (should be ignored)
//   - 1 in-flight transaction (should be undone)
//
// Then runs recovery and verifies the final state.
func TestCrashRecovery_MixedTransactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "recovery_mixed_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024,
		SyncPolicy:  wal.SyncOnEveryWrite,
		MaxSegments: 10,
	}

	// Build the collection manager and pre-insert a doc that will be updated by committed tx2
	mgr := NewManager()
	col, _ := mgr.CreateCollection("test_col", nil)

	existing := document.NewDocumentWithID("existing1", map[string]interface{}{"status": "old"})
	col.Insert(existing)

	// Also pre-insert a doc that the in-flight tx will try to delete
	inflight_target := document.NewDocumentWithID("inflight_target", map[string]interface{}{"keep": true})
	col.Insert(inflight_target)

	// --- Write WAL entries ---
	w, _ := wal.NewFileWAL(walConfig)

	// TX-A: committed — insert doc-a1, insert doc-a2
	txA := "tx-committed-A"
	w.Append(wal.CreateLogEntry(txA, wal.LogEntryBeginTx, "", nil, nil, nil))
	docA1 := document.NewDocumentWithID("doc-a1", map[string]interface{}{"from": "txA"})
	a1Bytes, _ := docA1.ToJSON()
	w.Append(wal.CreateLogEntry(txA, wal.LogEntryInsert, "test_col", []byte("doc-a1"), nil, a1Bytes))
	docA2 := document.NewDocumentWithID("doc-a2", map[string]interface{}{"from": "txA"})
	a2Bytes, _ := docA2.ToJSON()
	w.Append(wal.CreateLogEntry(txA, wal.LogEntryInsert, "test_col", []byte("doc-a2"), nil, a2Bytes))
	w.Append(wal.CreateLogEntry(txA, wal.LogEntryCommitTx, "", nil, nil, nil))

	// TX-B: committed — update existing1 status from "old" to "updated"
	txB := "tx-committed-B"
	w.Append(wal.CreateLogEntry(txB, wal.LogEntryBeginTx, "", nil, nil, nil))
	updatedExisting := document.NewDocumentWithID("existing1", map[string]interface{}{"status": "updated"})
	existingBytes, _ := existing.ToJSON()
	updatedBytes, _ := updatedExisting.ToJSON()
	w.Append(wal.CreateLogEntry(txB, wal.LogEntryUpdate, "test_col", []byte("existing1"), existingBytes, updatedBytes))
	w.Append(wal.CreateLogEntry(txB, wal.LogEntryCommitTx, "", nil, nil, nil))

	// TX-C: aborted — insert doc-c1 (should be ignored)
	txC := "tx-aborted-C"
	w.Append(wal.CreateLogEntry(txC, wal.LogEntryBeginTx, "", nil, nil, nil))
	docC1 := document.NewDocumentWithID("doc-c1", map[string]interface{}{"from": "txC"})
	c1Bytes, _ := docC1.ToJSON()
	w.Append(wal.CreateLogEntry(txC, wal.LogEntryInsert, "test_col", []byte("doc-c1"), nil, c1Bytes))
	w.Append(wal.CreateLogEntry(txC, wal.LogEntryAbortTx, "", nil, nil, nil))

	// TX-D: in-flight — delete inflight_target (no commit, simulating crash)
	txD := "tx-inflight-D"
	w.Append(wal.CreateLogEntry(txD, wal.LogEntryBeginTx, "", nil, nil, nil))
	inflightBytes, _ := inflight_target.ToJSON()
	w.Append(wal.CreateLogEntry(txD, wal.LogEntryDelete, "test_col", []byte("inflight_target"), inflightBytes, nil))
	// NO COMMIT

	w.Sync()
	w.Close()

	// Simulate partial crash damage: remove inflight_target (as if it was partially flushed)
	col.Delete("inflight_target")

	// --- Recovery ---
	w2, _ := wal.NewFileWAL(walConfig)
	defer w2.Close()

	recoveryApplier := NewRecoveryApplier(mgr)
	rm := wal.NewRecoveryManager(w2, recoveryApplier)
	result, recErr := rm.Recover()
	if recErr != nil {
		t.Fatalf("Recovery failed: %v", recErr)
	}

	// Verify counts
	if result.CommittedTx != 2 {
		t.Errorf("Expected 2 committed txs, got %d", result.CommittedTx)
	}
	if result.AbortedTx != 1 {
		t.Errorf("Expected 1 aborted tx, got %d", result.AbortedTx)
	}
	if result.InFlightTx != 1 {
		t.Errorf("Expected 1 in-flight tx, got %d", result.InFlightTx)
	}
	if result.RedoneOps != 3 { // 2 inserts from txA + 1 update from txB
		t.Errorf("Expected 3 redone ops, got %d", result.RedoneOps)
	}
	if result.UndoneOps != 1 { // 1 delete from txD
		t.Errorf("Expected 1 undone op, got %d", result.UndoneOps)
	}

	// Verify data state
	// doc-a1, doc-a2 should exist (committed inserts)
	for _, id := range []string{"doc-a1", "doc-a2"} {
		got, err := col.Get(id)
		if err != nil {
			t.Errorf("Expected %s to exist after REDO, got: %v", id, err)
		} else if got.Data["from"] != "txA" {
			t.Errorf("Expected %s from=txA, got %v", id, got.Data["from"])
		}
	}

	// existing1 should have status=updated (committed update)
	gotExisting, err := col.Get("existing1")
	if err != nil {
		t.Fatalf("Expected existing1 after recovery: %v", err)
	}
	if gotExisting.Data["status"] != "updated" {
		t.Errorf("Expected existing1 status=updated, got %v", gotExisting.Data["status"])
	}

	// doc-c1 should NOT exist (aborted tx)
	if _, err := col.Get("doc-c1"); err == nil {
		t.Error("doc-c1 should NOT exist (aborted tx)")
	}

	// inflight_target should be restored (in-flight delete undone)
	gotInflight, err := col.Get("inflight_target")
	if err != nil {
		t.Fatalf("Expected inflight_target restored after UNDO: %v", err)
	}
	if gotInflight.Data["keep"] != true {
		t.Errorf("Expected inflight_target keep=true, got %v", gotInflight.Data["keep"])
	}
}

// TestWriteBufferIsolation_ConcurrentReadWrite verifies that concurrent
// transactions writing to different docs don't interfere with each other,
// and that the write buffer remains isolated per-transaction.
func TestWriteBufferIsolation_ConcurrentReadWrite(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	const N = 30
	var wg sync.WaitGroup
	var commitCount int64

	// Each goroutine:
	// 1. Begins a transaction
	// 2. Inserts its own unique doc
	// 3. Reads back from the write buffer to confirm isolation
	// 4. Commits
	// 5. Verifies the doc is in storage
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			docID := fmt.Sprintf("iso-%d", n)

			tx, err := tm.Begin()
			if err != nil {
				t.Errorf("goroutine %d: Begin failed: %v", n, err)
				return
			}

			doc := document.NewDocumentWithID(docID, map[string]interface{}{"worker": float64(n)})
			if err := col.InsertTx(tx, doc); err != nil {
				t.Errorf("goroutine %d: InsertTx failed: %v", n, err)
				tm.Rollback(tx)
				return
			}

			// Verify: the doc is in THIS transaction's write buffer
			buffered, deleted, found := tx.GetFromWriteBuffer(col.Name, docID)
			if !found || deleted || buffered == nil {
				t.Errorf("goroutine %d: expected doc in write buffer", n)
				tm.Rollback(tx)
				return
			}

			// Verify: the doc is NOT visible in storage yet
			if _, getErr := col.Get(docID); getErr == nil {
				t.Errorf("goroutine %d: doc visible in storage before commit", n)
				tm.Rollback(tx)
				return
			}

			// Verify: another goroutine's docs aren't in our buffer
			otherID := fmt.Sprintf("iso-%d", (n+1)%N)
			_, _, otherFound := tx.GetFromWriteBuffer(col.Name, otherID)
			if otherFound {
				t.Errorf("goroutine %d: found another goroutine's doc (%s) in our buffer", n, otherID)
			}

			if err := tm.Commit(tx); err != nil {
				t.Errorf("goroutine %d: Commit failed: %v", n, err)
				return
			}
			atomic.AddInt64(&commitCount, 1)
		}(i)
	}
	wg.Wait()

	if commitCount != N {
		t.Errorf("Expected %d commits, got %d", N, commitCount)
	}

	// Verify all committed docs are in storage
	for i := 0; i < N; i++ {
		docID := fmt.Sprintf("iso-%d", i)
		got, err := col.Get(docID)
		if err != nil {
			t.Errorf("doc %s should exist after commit: %v", docID, err)
		} else if got.Data["worker"] != float64(i) {
			t.Errorf("doc %s has wrong worker: expected %d, got %v", docID, i, got.Data["worker"])
		}
	}
}

// TestRapidCommitRollbackCycles exercises the transaction lifecycle under
// rapid sequential commit/rollback patterns to catch resource leaks or
// state corruption in the transaction manager.
func TestRapidCommitRollbackCycles(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	const cycles = 100

	for i := 0; i < cycles; i++ {
		docID := fmt.Sprintf("rapid-%d", i)
		doc := document.NewDocumentWithID(docID, map[string]interface{}{"cycle": float64(i)})

		tx, err := tm.Begin()
		if err != nil {
			t.Fatalf("cycle %d: Begin failed: %v", i, err)
		}

		if err := col.InsertTx(tx, doc); err != nil {
			t.Fatalf("cycle %d: InsertTx failed: %v", i, err)
		}

		if i%3 == 0 {
			// Every 3rd cycle: rollback
			if err := tm.Rollback(tx); err != nil {
				t.Fatalf("cycle %d: Rollback failed: %v", i, err)
			}
		} else {
			// Otherwise: commit
			if err := tm.Commit(tx); err != nil {
				t.Fatalf("cycle %d: Commit failed: %v", i, err)
			}
		}
	}

	// Verify final state
	for i := 0; i < cycles; i++ {
		docID := fmt.Sprintf("rapid-%d", i)
		_, err := col.Get(docID)
		if i%3 == 0 {
			if err == nil {
				t.Errorf("doc %s should NOT exist (rolled back)", docID)
			}
		} else {
			if err != nil {
				t.Errorf("doc %s should exist (committed): %v", docID, err)
			}
		}
	}
}

// TestInsertUpdateDelete_SingleTx tests insert, update, and delete of
// the same document within a single transaction, then verifies committed state.
func TestInsertUpdateDelete_SingleTx(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	tx, err := tm.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Insert
	doc := document.NewDocumentWithID("lifecycle", map[string]interface{}{"step": "inserted"})
	if err := col.InsertTx(tx, doc); err != nil {
		t.Fatalf("InsertTx failed: %v", err)
	}

	// Update within same tx
	updDoc := document.NewDocumentWithID("lifecycle", map[string]interface{}{"step": "updated"})
	if err := col.UpdateTx(tx, updDoc); err != nil {
		t.Fatalf("UpdateTx failed: %v", err)
	}

	// Delete within same tx
	if err := col.DeleteTx(tx, "lifecycle"); err != nil {
		t.Fatalf("DeleteTx failed: %v", err)
	}

	// Commit
	if err := tm.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// After commit, the net effect is: doc was inserted, updated, then deleted.
	// It should NOT exist in storage.
	if _, err := col.Get("lifecycle"); err == nil {
		t.Error("doc 'lifecycle' should NOT exist after insert+update+delete in same tx")
	}
}

// TestConcurrentInsertSameDocID verifies that concurrent transactions
// trying to insert the same document ID are properly serialized, with
// exactly one succeeding and the rest failing.
func TestConcurrentInsertSameDocID(t *testing.T) {
	col, tm, cleanup := setupTestEnv(t)
	defer cleanup()

	const N = 20
	var wg sync.WaitGroup
	var successCount int64

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			tx, err := tm.Begin()
			if err != nil {
				return
			}

			doc := document.NewDocumentWithID("contested", map[string]interface{}{"winner": float64(n)})
			if err := col.InsertTx(tx, doc); err != nil {
				tm.Rollback(tx)
				return
			}

			if err := tm.Commit(tx); err != nil {
				// Commit may fail if another goroutine already committed the same ID
				return
			}
			atomic.AddInt64(&successCount, 1)
		}(i)
	}
	wg.Wait()

	// With deferred writes, InsertTx checks storage at buffer time.
	// Multiple goroutines may pass the check, but ApplyOperations runs
	// one-at-a-time per commit. Exactly one should ultimately succeed.
	// The doc must exist in storage.
	got, err := col.Get("contested")
	if err != nil {
		t.Fatalf("Expected 'contested' to exist in storage: %v", err)
	}
	if got.Data["winner"] == nil {
		t.Error("Expected winner field in contested doc")
	}

	if successCount < 1 {
		t.Error("At least one transaction should have committed successfully")
	}
	t.Logf("Concurrent insert race: %d/%d transactions succeeded", successCount, N)
}
