// Package wal – crash recovery using WAL replay (ARIES-inspired).
package wal

import (
	"aidb/internal/document"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// ---------------------------------------------------------------------------
// RecoveryStorageApplier – interface implemented by collection layer
// ---------------------------------------------------------------------------

// RecoveryStorageApplier applies recovered operations to storage.
// All methods must be idempotent (safe to re-apply).
type RecoveryStorageApplier interface {
	ApplyRecoveryInsert(collection string, doc *document.Document) error
	ApplyRecoveryUpdate(collection string, doc *document.Document) error
	ApplyRecoveryDelete(collection string, docID string) error
}

// ---------------------------------------------------------------------------
// Transaction classification
// ---------------------------------------------------------------------------

// TxFinalState describes the outcome of a transaction found during WAL scan.
type TxFinalState int

const (
	TxFinalCommitted TxFinalState = iota
	TxFinalAborted
	TxFinalInFlight // No COMMIT or ABORT found — incomplete
)

func (s TxFinalState) String() string {
	switch s {
	case TxFinalCommitted:
		return "COMMITTED"
	case TxFinalAborted:
		return "ABORTED"
	case TxFinalInFlight:
		return "IN_FLIGHT"
	default:
		return "UNKNOWN"
	}
}

// RecoveredTx holds state for a single transaction found during WAL scan.
type RecoveredTx struct {
	TxID       string
	State      TxFinalState
	Operations []*LogEntry
}

// ---------------------------------------------------------------------------
// RecoveryResult
// ---------------------------------------------------------------------------

// RecoveryResult summarises what the recovery process did.
type RecoveryResult struct {
	TotalEntries     int
	CommittedTx      int
	AbortedTx        int
	InFlightTx       int
	RedoneOps        int
	UndoneOps        int
	CorruptedEntries int
	Errors           []error
}

// ---------------------------------------------------------------------------
// RecoveryManager
// ---------------------------------------------------------------------------

// RecoveryManager handles crash recovery by scanning the WAL.
type RecoveryManager struct {
	wal            WAL
	storageApplier RecoveryStorageApplier
}

// NewRecoveryManager creates a recovery manager.
func NewRecoveryManager(w WAL, applier RecoveryStorageApplier) *RecoveryManager {
	return &RecoveryManager{
		wal:            w,
		storageApplier: applier,
	}
}

// Recover performs ARIES-style recovery:
//  1. Scan WAL → classify transactions as COMMITTED / ABORTED / IN_FLIGHT
//  2. REDO all committed transaction operations (idempotent)
//  3. UNDO all in-flight transaction operations (reverse order)
//  4. Write ABORT records for in-flight transactions
func (rm *RecoveryManager) Recover() (*RecoveryResult, error) {
	result := &RecoveryResult{Errors: make([]error, 0)}

	// ---- Phase 1: Scan ----
	log.Println("[Recovery] Phase 1: Scanning WAL entries…")
	entries, err := rm.wal.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}

	result.TotalEntries = len(entries)
	if len(entries) == 0 {
		log.Println("[Recovery] WAL is empty — nothing to recover")
		return result, nil
	}

	txMap := make(map[string]*RecoveredTx)

	for _, entry := range entries {
		txID := entry.TxID
		if txID == "" {
			continue
		}
		if _, ok := txMap[txID]; !ok {
			txMap[txID] = &RecoveredTx{
				TxID:       txID,
				State:      TxFinalInFlight,
				Operations: make([]*LogEntry, 0),
			}
		}
		tx := txMap[txID]

		switch entry.Type {
		case LogEntryBeginTx:
			// noted — no action needed
		case LogEntryCommitTx:
			tx.State = TxFinalCommitted
		case LogEntryAbortTx:
			tx.State = TxFinalAborted
		default:
			if entry.IsDataOperation() {
				tx.Operations = append(tx.Operations, entry)
			}
		}
	}

	for _, tx := range txMap {
		switch tx.State {
		case TxFinalCommitted:
			result.CommittedTx++
		case TxFinalAborted:
			result.AbortedTx++
		case TxFinalInFlight:
			result.InFlightTx++
		}
	}

	log.Printf("[Recovery] Found %d transactions: %d committed, %d aborted, %d in-flight",
		len(txMap), result.CommittedTx, result.AbortedTx, result.InFlightTx)

	// ---- Phase 2: REDO committed transactions ----
	if result.CommittedTx > 0 {
		log.Println("[Recovery] Phase 2: REDO committed transactions…")
		for txID, tx := range txMap {
			if tx.State != TxFinalCommitted {
				continue
			}
			for _, entry := range tx.Operations {
				if err := rm.redoEntry(entry); err != nil {
					log.Printf("[Recovery] REDO warning tx=%s: %v", txID, err)
					result.Errors = append(result.Errors, fmt.Errorf("REDO tx=%s: %w", txID, err))
				} else {
					result.RedoneOps++
				}
			}
		}
	}

	// ---- Phase 3: UNDO in-flight transactions ----
	if result.InFlightTx > 0 {
		log.Println("[Recovery] Phase 3: UNDO in-flight transactions…")
		for txID, tx := range txMap {
			if tx.State != TxFinalInFlight {
				continue
			}
			// Undo in reverse order
			for i := len(tx.Operations) - 1; i >= 0; i-- {
				if err := rm.undoEntry(tx.Operations[i]); err != nil {
					log.Printf("[Recovery] UNDO warning tx=%s: %v", txID, err)
					result.Errors = append(result.Errors, fmt.Errorf("UNDO tx=%s: %w", txID, err))
				} else {
					result.UndoneOps++
				}
			}
			// Write ABORT record so this tx is finalised in the WAL
			abortEntry := CreateLogEntry(txID, LogEntryAbortTx, "", nil, nil, nil)
			if _, wErr := rm.wal.Append(abortEntry); wErr != nil {
				log.Printf("[Recovery] failed to write ABORT for tx %s: %v", txID, wErr)
			}
		}
	}

	if err := rm.wal.Sync(); err != nil {
		log.Printf("[Recovery] WAL sync after recovery failed: %v", err)
	}

	log.Printf("[Recovery] Complete: %d ops redone, %d ops undone, %d warnings",
		result.RedoneOps, result.UndoneOps, len(result.Errors))
	return result, nil
}

// ---------------------------------------------------------------------------
// REDO / UNDO helpers
// ---------------------------------------------------------------------------

func (rm *RecoveryManager) redoEntry(entry *LogEntry) error {
	if rm.storageApplier == nil {
		return nil
	}
	switch entry.Type {
	case LogEntryInsert:
		doc, err := deserializeDoc(entry.NewValue)
		if err != nil {
			return fmt.Errorf("deserialize new value: %w", err)
		}
		return rm.storageApplier.ApplyRecoveryInsert(entry.Collection, doc)

	case LogEntryUpdate:
		doc, err := deserializeDoc(entry.NewValue)
		if err != nil {
			return fmt.Errorf("deserialize new value: %w", err)
		}
		return rm.storageApplier.ApplyRecoveryUpdate(entry.Collection, doc)

	case LogEntryDelete:
		return rm.storageApplier.ApplyRecoveryDelete(entry.Collection, string(entry.Key))
	}
	return nil
}

func (rm *RecoveryManager) undoEntry(entry *LogEntry) error {
	if rm.storageApplier == nil {
		return nil
	}
	switch entry.Type {
	case LogEntryInsert:
		// Undo insert → delete the document
		return rm.storageApplier.ApplyRecoveryDelete(entry.Collection, string(entry.Key))

	case LogEntryUpdate:
		// Undo update → restore old value
		if len(entry.OldValue) > 0 {
			doc, err := deserializeDoc(entry.OldValue)
			if err != nil {
				return fmt.Errorf("deserialize old value: %w", err)
			}
			return rm.storageApplier.ApplyRecoveryUpdate(entry.Collection, doc)
		}

	case LogEntryDelete:
		// Undo delete → re-insert old value
		if len(entry.OldValue) > 0 {
			doc, err := deserializeDoc(entry.OldValue)
			if err != nil {
				return fmt.Errorf("deserialize old value: %w", err)
			}
			return rm.storageApplier.ApplyRecoveryInsert(entry.Collection, doc)
		}
	}
	return nil
}

// deserializeDoc unmarshals a document from JSON bytes.
func deserializeDoc(data []byte) (*document.Document, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty document data")
	}
	var doc document.Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// ---------------------------------------------------------------------------
// PITR Support: Recover to specific LSN or Timestamp
// ---------------------------------------------------------------------------

// RecoverToLSN performs recovery and stops replay at the given target LSN (for PITR).
func (rm *RecoveryManager) RecoverToLSN(targetLSN uint64) (*RecoveryResult, error) {
	log.Printf("[PITR] Starting recovery to LSN %d", targetLSN)
	return rm.recoverWithFilter(func(e *LogEntry) bool {
		return e.LSN <= targetLSN
	})
}

// RecoverToTime performs recovery and stops at the first entry after the target time (for PITR).
func (rm *RecoveryManager) RecoverToTime(targetTime time.Time) (*RecoveryResult, error) {
	log.Printf("[PITR] Starting recovery to time %s", targetTime)
	targetNano := targetTime.UnixNano()
	return rm.recoverWithFilter(func(e *LogEntry) bool {
		return e.Timestamp <= targetNano
	})
}

// recoverWithFilter is the internal implementation used by PITR methods.
func (rm *RecoveryManager) recoverWithFilter(shouldApply func(*LogEntry) bool) (*RecoveryResult, error) {
	result := &RecoveryResult{Errors: make([]error, 0)}

	entries, err := rm.wal.ReadAll()
	if err != nil {
		return nil, err
	}

	// Filter entries
	var filtered []*LogEntry
	for _, e := range entries {
		if shouldApply(e) {
			filtered = append(filtered, e)
		}
	}

	// Reuse most of the existing analysis + redo logic on filtered entries
	// (Simplified version for clarity - full version would reuse more code)
	txMap := make(map[string]*RecoveredTx)
	for _, entry := range filtered {
		if entry.TxID == "" {
			continue
		}
		if _, ok := txMap[entry.TxID]; !ok {
			txMap[entry.TxID] = &RecoveredTx{TxID: entry.TxID, State: TxFinalInFlight, Operations: []*LogEntry{}}
		}
		tx := txMap[entry.TxID]
		switch entry.Type {
		case LogEntryCommitTx:
			tx.State = TxFinalCommitted
		case LogEntryAbortTx:
			tx.State = TxFinalAborted
		default:
			if entry.IsDataOperation() {
				tx.Operations = append(tx.Operations, entry)
			}
		}
	}

	for _, tx := range txMap {
		if tx.State == TxFinalCommitted {
			for _, op := range tx.Operations {
				rm.redoEntry(op)
				result.RedoneOps++
			}
			result.CommittedTx++
		} else if tx.State == TxFinalInFlight {
			for i := len(tx.Operations) - 1; i >= 0; i-- {
				rm.undoEntry(tx.Operations[i])
				result.UndoneOps++
			}
			result.InFlightTx++
		}
	}

	log.Printf("[PITR] Recovery complete: %d redone, %d undone", result.RedoneOps, result.UndoneOps)
	return result, nil
}
