package transaction

import (
	"errors"
	"os"
	"testing"
	"time"

	"aidb/internal/document"
	"aidb/internal/wal"
)

func TestTransactionBasics(t *testing.T) {
	// Create temp directory for WAL
	tmpDir, err := os.MkdirTemp("", "transaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL
	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024, // 10MB
		SyncPolicy:  wal.SyncOnCommit,
		MaxSegments: 10,
	}

	w, err := wal.NewFileWAL(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create transaction manager
	tmConfig := &ManagerConfig{
		DefaultIsolation:      ReadCommitted,
		DefaultTimeout:        5 * time.Second,
		MaxActiveTransactions: 100,
		AutoCommitEnabled:     true,
	}

	tm := NewManager(w, tmConfig)
	defer tm.Close()

	t.Run("BeginCommit", func(t *testing.T) {
		tx, err := tm.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if !tx.IsActive() {
			t.Error("Transaction should be active")
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		if !tx.IsCommitted() {
			t.Error("Transaction should be committed")
		}
	})

	t.Run("BeginRollback", func(t *testing.T) {
		tx, err := tm.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		err = tm.Rollback(tx)
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}
	})

	t.Run("AutoTransactionSuccess", func(t *testing.T) {
		opCount := 0
		err := tm.AutoTransaction(func(tx *Transaction) error {
			opCount++
			return nil
		})
		if err != nil {
			t.Fatalf("AutoTransaction failed: %v", err)
		}
		if opCount != 1 {
			t.Errorf("Expected 1 operation, got %d", opCount)
		}
	})

	t.Run("AutoTransactionRollback", func(t *testing.T) {
		testErr := errors.New("test error")
		err := tm.AutoTransaction(func(tx *Transaction) error {
			return testErr
		})
		if err == nil {
			t.Error("Expected error from AutoTransaction")
		}
	})
}

// ---------------------------------------------------------------------------
// Deferred-write buffer tests
// ---------------------------------------------------------------------------

func TestGetFromWriteBuffer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "writebuf_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024,
		SyncPolicy:  wal.SyncOnCommit,
		MaxSegments: 10,
	}
	w, err := wal.NewFileWAL(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	tmConfig := &ManagerConfig{
		DefaultIsolation:      ReadCommitted,
		DefaultTimeout:        5 * time.Second,
		MaxActiveTransactions: 100,
		AutoCommitEnabled:     true,
	}
	tm := NewManager(w, tmConfig)
	defer tm.Close()

	t.Run("NotFound", func(t *testing.T) {
		tx, _ := tm.Begin()
		defer tm.Rollback(tx)

		doc, deleted, found := tx.GetFromWriteBuffer("col", "missing")
		if found {
			t.Error("Expected not found for empty buffer")
		}
		if deleted {
			t.Error("Expected deleted=false")
		}
		if doc != nil {
			t.Error("Expected nil doc")
		}
	})

	t.Run("InsertThenRead", func(t *testing.T) {
		tx, _ := tm.Begin()
		defer tm.Rollback(tx)

		doc := &document.Document{
			ID:   "doc1",
			Data: map[string]interface{}{"name": "Alice"},
		}
		op := Operation{
			Type:       OpInsert,
			Collection: "users",
			DocumentID: doc.ID,
			NewValue:   doc,
		}
		if err := tx.AddOperation(op); err != nil {
			t.Fatalf("AddOperation failed: %v", err)
		}

		got, deleted, found := tx.GetFromWriteBuffer("users", "doc1")
		if !found {
			t.Fatal("Expected document found in buffer")
		}
		if deleted {
			t.Fatal("Expected deleted=false")
		}
		if got.ID != "doc1" {
			t.Errorf("Expected ID doc1, got %s", got.ID)
		}
		if got.Data["name"] != "Alice" {
			t.Errorf("Expected name=Alice, got %v", got.Data["name"])
		}

		// Modify the returned copy — original buffer must be untouched
		got.Data["name"] = "Bob"
		got2, _, _ := tx.GetFromWriteBuffer("users", "doc1")
		if got2.Data["name"] != "Alice" {
			t.Errorf("Buffer was corrupted: expected Alice, got %v", got2.Data["name"])
		}
	})

	t.Run("DeleteThenRead", func(t *testing.T) {
		tx, _ := tm.Begin()
		defer tm.Rollback(tx)

		op := Operation{
			Type:       OpDelete,
			Collection: "users",
			DocumentID: "doc2",
			OldValue:   &document.Document{ID: "doc2"},
		}
		if err := tx.AddOperation(op); err != nil {
			t.Fatalf("AddOperation failed: %v", err)
		}

		_, deleted, found := tx.GetFromWriteBuffer("users", "doc2")
		if !found {
			t.Fatal("Expected found=true for deleted doc")
		}
		if !deleted {
			t.Fatal("Expected deleted=true")
		}
	})
}

func TestTransactionOperations(t *testing.T) {
	// Create temp directory for WAL
	tmpDir, err := os.MkdirTemp("", "transaction_ops_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL
	walConfig := &wal.Config{
		Directory:   tmpDir + "/wal",
		SegmentSize: 10 * 1024 * 1024,
		SyncPolicy:  wal.SyncOnCommit,
		MaxSegments: 10,
	}

	w, err := wal.NewFileWAL(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create transaction manager
	tmConfig := &ManagerConfig{
		DefaultIsolation:      ReadCommitted,
		DefaultTimeout:        5 * time.Second,
		MaxActiveTransactions: 100,
		AutoCommitEnabled:     true,
	}

	tm := NewManager(w, tmConfig)
	defer tm.Close()

	t.Run("AddOperation", func(t *testing.T) {
		tx, err := tm.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		doc := &document.Document{
			ID:   "test-1",
			Data: map[string]interface{}{"name": "test"},
		}

		op := Operation{
			Type:       OpInsert,
			Collection: "test_collection",
			DocumentID: doc.ID,
			OldValue:   nil,
			NewValue:   doc,
		}

		err = tx.AddOperation(op)
		if err != nil {
			t.Fatalf("Failed to add operation: %v", err)
		}

		if len(tx.GetOperations()) != 1 {
			t.Errorf("Expected 1 operation, got %d", len(tx.GetOperations()))
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}
	})

	t.Run("TransactionInfo", func(t *testing.T) {
		tx, err := tm.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		info := tx.Info()
		if info.ID == "" {
			t.Error("Transaction ID should not be empty")
		}
		if info.State != "ACTIVE" {
			t.Errorf("Expected state ACTIVE, got %s", info.State)
		}
		if info.Isolation != "READ_COMMITTED" {
			t.Errorf("Expected isolation READ_COMMITTED, got %s", info.Isolation)
		}

		tm.Rollback(tx)
	})
}