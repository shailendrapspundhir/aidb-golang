package storage

import (
	"aidb/internal/document"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// BoltDBStorage is a BoltDB-backed implementation of the Storage interface
type BoltDBStorage struct {
	db           *bolt.DB
	bucketName   []byte
	collectionName string
}

// NewBoltDBStorage creates a new BoltDB storage for a collection
func NewBoltDBStorage(db *bolt.DB, collectionName string) (*BoltDBStorage, error) {
	bs := &BoltDBStorage{
		db:           db,
		bucketName:   []byte(collectionName),
		collectionName: collectionName,
	}

	// Create the bucket if it doesn't exist
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bs.bucketName)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return bs, nil
}

// Insert stores a new document
func (s *BoltDBStorage) Insert(doc *document.Document) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		// Check if document already exists
		if v := b.Get([]byte(doc.ID)); v != nil {
			return ErrDocumentExists
		}

		// Marshal and store the document
		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		return b.Put([]byte(doc.ID), data)
	})
}

// Get retrieves a document by ID
func (s *BoltDBStorage) Get(id string) (*document.Document, error) {
	var doc *document.Document

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		v := b.Get([]byte(id))
		if v == nil {
			return ErrDocumentNotFound
		}

		var d document.Document
		if err := json.Unmarshal(v, &d); err != nil {
			return fmt.Errorf("failed to unmarshal document: %w", err)
		}
		doc = &d
		return nil
	})

	return doc, err
}

// Update updates an existing document
func (s *BoltDBStorage) Update(doc *document.Document) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		// Check if document exists
		if v := b.Get([]byte(doc.ID)); v == nil {
			return ErrDocumentNotFound
		}

		// Marshal and store the document
		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		return b.Put([]byte(doc.ID), data)
	})
}

// Delete removes a document by ID
func (s *BoltDBStorage) Delete(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		// Check if document exists
		if v := b.Get([]byte(id)); v == nil {
			return ErrDocumentNotFound
		}

		return b.Delete([]byte(id))
	})
}

// Find retrieves documents matching a filter
func (s *BoltDBStorage) Find(filter map[string]interface{}) ([]*document.Document, error) {
	var results []*document.Document

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		return b.ForEach(func(k, v []byte) error {
			var doc document.Document
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}

			if matchesFilter(&doc, filter) {
				results = append(results, &doc)
			}
			return nil
		})
	})

	return results, err
}

// FindAll retrieves all documents
func (s *BoltDBStorage) FindAll() ([]*document.Document, error) {
	var results []*document.Document

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		return b.ForEach(func(k, v []byte) error {
			var doc document.Document
			if err := json.Unmarshal(v, &doc); err != nil {
				return err
			}
			results = append(results, &doc)
			return nil
		})
	})

	return results, err
}

// Count returns the number of documents
func (s *BoltDBStorage) Count() int {
	count := 0
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b != nil {
			count = b.Stats().KeyN
		}
		return nil
	})
	return count
}

// Clear removes all documents from the collection
func (s *BoltDBStorage) Clear() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return nil
		}

		// Delete all keys
		keys := make([][]byte, 0)
		b.ForEach(func(k, v []byte) error {
			keys = append(keys, k)
			return nil
		})

		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteBucket deletes the entire bucket (for dropping collection)
func (s *BoltDBStorage) DeleteBucket() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(s.bucketName)
	})
}

// ImportDocuments imports multiple documents at once
func (s *BoltDBStorage) ImportDocuments(docs []*document.Document) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		for _, doc := range docs {
			data, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("failed to marshal document %s: %w", doc.ID, err)
			}
			if err := b.Put([]byte(doc.ID), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close closes the storage (BoltDB is closed by the manager)
func (s *BoltDBStorage) Close() error {
	return nil
}

// Flush flushes data to disk
func (s *BoltDBStorage) Flush() error {
	// BoltDB auto-flushes, but we can force a sync
	return nil
}

// GetRaw retrieves raw bytes by ID
func (s *BoltDBStorage) GetRaw(id string) ([]byte, error) {
	var data []byte

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}

		v := b.Get([]byte(id))
		if v == nil {
			return ErrDocumentNotFound
		}

		// Copy the data since it's only valid within the transaction
		data = make([]byte, len(v))
		copy(data, v)
		return nil
	})

	return data, err
}

// PutRaw stores raw bytes by ID
func (s *BoltDBStorage) PutRaw(id string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}
		return b.Put([]byte(id), data)
	})
}

// DeleteRaw removes raw bytes by ID
func (s *BoltDBStorage) DeleteRaw(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}
		return b.Delete([]byte(id))
	})
}

// CompactRange compacts the storage (no-op for BoltDB)
func (s *BoltDBStorage) CompactRange(start, end string) error {
	return nil
}

// boltCursor implements Cursor for BoltDBStorage
type boltCursor struct {
	storage  *BoltDBStorage
	keys     [][]byte
	index    int
	current  *document.Document
	err      error
	closed   bool
}

// Cursor returns a streaming iterator over all documents
func (s *BoltDBStorage) Cursor() (Cursor, error) {
	var keys [][]byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", s.collectionName)
		}
		return b.ForEach(func(k, v []byte) error {
			// Copy key (k is only valid during ForEach)
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return &boltCursor{
		storage: s,
		keys:    keys,
		index:   -1,
	}, nil
}

func (c *boltCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}
	c.index++
	if c.index >= len(c.keys) {
		c.current = nil
		return false
	}

	doc, err := c.storage.Get(string(c.keys[c.index]))
	if err != nil {
		c.err = err
		c.current = nil
		return false
	}
	c.current = doc
	return true
}

func (c *boltCursor) Current() *document.Document {
	return c.current
}

func (c *boltCursor) Err() error {
	return c.err
}

func (c *boltCursor) Close() error {
	c.closed = true
	c.current = nil
	return nil
}
