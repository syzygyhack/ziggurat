package store

import (
	"fmt"

	"go.etcd.io/bbolt"
)

var bucketNamespaces = []byte("namespaces")

func (s *Store) setNamespace(key, hashHex string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNamespaces)
		if b == nil {
			return fmt.Errorf("namespaces bucket not initialized")
		}

		// If the key already exists, decrement the old hash's refcount.
		// This runs even when oldHash == hashHex because putOrIncrRef
		// already incremented; the decrement here cancels it out so
		// re-PUTting the same content to the same key is a no-op on
		// refcount.
		if old := b.Get([]byte(key)); old != nil {
			oldHash := string(old)
			if err := decrRefInTx(tx, oldHash); err != nil {
				return fmt.Errorf("decr old ref: %w", err)
			}
		}

		return b.Put([]byte(key), []byte(hashHex))
	})
}

func (s *Store) resolveNamespace(key string) (string, error) {
	var hashHex string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNamespaces)
		if b == nil {
			return fmt.Errorf("namespaces bucket not initialized")
		}
		v := b.Get([]byte(key))
		if v == nil {
			return fmt.Errorf("namespace key not found: %s", key)
		}
		hashHex = string(v)
		return nil
	})
	return hashHex, err
}

func (s *Store) deleteNamespace(key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNamespaces)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

func (s *Store) listNamespaces(prefix string) ([]string, error) {
	keys := []string{} // non-nil so JSON serializes as [] not null
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNamespaces)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		pfx := []byte(prefix)
		for k, _ := c.Seek(pfx); k != nil; k, _ = c.Next() {
			ks := string(k)
			// Stop when key no longer has the prefix. A key shorter
			// than the prefix cannot match, so break on that too.
			if len(prefix) > 0 && (len(ks) < len(prefix) || ks[:len(prefix)] != prefix) {
				break
			}
			keys = append(keys, ks)
		}
		return nil
	})
	return keys, err
}
