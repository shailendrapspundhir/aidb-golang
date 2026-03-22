//go:build !rocksdb
// +build !rocksdb

package storage

import (
	"aidb/internal/document"
	"errors"
)

// ErrRocksDBNotAvailable is returned when RocksDB is not compiled in
var ErrRocksDBNotAvailable = errors.New("RocksDB storage not available - rebuild with -tags rocksdb")

// RocksDBStorage is a stub for when RocksDB is not available
type RocksDBStorage struct{}

// RocksDBOptions contains options for RocksDB storage
type RocksDBOptions struct {
	Path           string
	CollectionName string
	CacheSizeMB    int
	EnableCache    bool
}

// NewRocksDBStorage returns an error when RocksDB is not available
func NewRocksDBStorage(opts RocksDBOptions) (*RocksDBStorage, error) {
	return nil, ErrRocksDBNotAvailable
}

// Stub implementations
func (s *RocksDBStorage) Insert(doc *document.Document) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) Get(id string) (*document.Document, error) { return nil, ErrRocksDBNotAvailable }
func (s *RocksDBStorage) Update(doc *document.Document) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) Delete(id string) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) Find(filter map[string]interface{}) ([]*document.Document, error) {
	return nil, ErrRocksDBNotAvailable
}
func (s *RocksDBStorage) FindAll() ([]*document.Document, error) { return nil, ErrRocksDBNotAvailable }
func (s *RocksDBStorage) Count() int { return 0 }
func (s *RocksDBStorage) Clear() error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) Close() error { return nil }
func (s *RocksDBStorage) Flush() error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) GetRaw(id string) ([]byte, error) { return nil, ErrRocksDBNotAvailable }
func (s *RocksDBStorage) PutRaw(id string, data []byte) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) DeleteRaw(id string) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) ImportDocuments(docs []*document.Document) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) CompactRange(start, end string) error { return ErrRocksDBNotAvailable }
func (s *RocksDBStorage) GetColumnFamilyName() string { return "" }
func (s *RocksDBStorage) Cursor() (Cursor, error) { return nil, ErrRocksDBNotAvailable }
