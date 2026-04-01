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