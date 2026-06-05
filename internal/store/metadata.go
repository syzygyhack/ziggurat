package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

var bucketObjects = []byte("objects")

// putOrIncrRef atomically creates object metadata if the hash is new,
// or increments the existing refcount if the hash already exists.
// This prevents Put from resetting refcounts on duplicate content.
func (s *Store) putOrIncrRef(hashHex string, hash [32]byte, size int64, tier model.Tier, nsKey string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return fmt.Errorf("objects bucket not initialized")
		}
		existing := b.Get([]byte(hashHex))

		if existing != nil {
			// Hash already exists — increment refcount only.
			return incrRefInTx(tx, hashHex)
		}

		// New object — create metadata with proper CreatedAt.
		meta := model.ObjectMeta{
			Hash:      hash,
			Size:      size,
			Tier:      tier,
			Strategy:  model.Replicated,
			Namespace: nsKey,
			RefCount:  1,
			CreatedAt: time.Now(),
		}
		data, err := json.Marshal(&meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(hashHex), data)
	})
}

func (s *Store) getMeta(hashHex string) (*model.ObjectMeta, error) {
	var meta model.ObjectMeta
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return fmt.Errorf("objects bucket not initialized")
		}
		data := b.Get([]byte(hashHex))
		if data == nil {
			return fmt.Errorf("object not found: %s", hashHex[:12])
		}
		return json.Unmarshal(data, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *Store) incrRef(hashHex string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return incrRefInTx(tx, hashHex)
	})
}

func (s *Store) decrRef(hashHex string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return decrRefInTx(tx, hashHex)
	})
}

// updateMetaInTx loads the metadata for hashHex, applies mutate, and writes it
// back, all within tx. When the object is absent: missingSkip=true makes it a
// no-op (nil), otherwise it returns an "object not found" error. A missing
// bucket is always an error — the objects bucket is created at store open
// (CreateBucketIfNotExists), so this is an unreachable defensive check.
func updateMetaInTx(tx *bbolt.Tx, hashHex string, missingSkip bool, mutate func(*model.ObjectMeta)) error {
	b := tx.Bucket(bucketObjects)
	if b == nil {
		return fmt.Errorf("objects bucket not initialized")
	}
	data := b.Get([]byte(hashHex))
	if data == nil {
		if missingSkip {
			return nil
		}
		return fmt.Errorf("object not found: %s", hashHex[:12])
	}
	var meta model.ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}
	mutate(&meta)
	updated, err := json.Marshal(&meta)
	if err != nil {
		return err
	}
	return b.Put([]byte(hashHex), updated)
}

// decrRefAndStamp decrements a refcount (clamped at zero) and records
// UnreferencedAt when it reaches zero, starting the GC grace period.
func decrRefAndStamp(meta *model.ObjectMeta) {
	meta.RefCount--
	if meta.RefCount < 0 {
		meta.RefCount = 0
	}
	if meta.RefCount == 0 && meta.UnreferencedAt.IsZero() {
		meta.UnreferencedAt = time.Now()
	}
}

func incrRefInTx(tx *bbolt.Tx, hashHex string) error {
	return updateMetaInTx(tx, hashHex, false, func(meta *model.ObjectMeta) {
		meta.RefCount++
		// Clear unreferenced timestamp — object is referenced again.
		meta.UnreferencedAt = time.Time{}
	})
}

func decrRefInTx(tx *bbolt.Tx, hashHex string) error {
	// Missing object is a no-op: it may have already been collected.
	return updateMetaInTx(tx, hashHex, true, decrRefAndStamp)
}
