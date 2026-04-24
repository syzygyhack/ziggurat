package store

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

// GC runs periodic garbage collection on objects with zero refcount.
type GC struct {
	store *Store
	grace time.Duration
	log   *slog.Logger
}

// NewGC creates a garbage collector for the given store.
func NewGC(s *Store, grace time.Duration, log *slog.Logger) *GC {
	return &GC{store: s, grace: grace, log: log}
}

// Run starts the GC loop. It scans every interval for objects past their
// grace period with refcount <= 0 and deletes them.
func (gc *GC) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gc.sweep()
		}
	}
}

func (gc *GC) sweep() {
	cutoff := time.Now().Add(-gc.grace)
	var toDelete []string

	gc.store.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		return b.ForEach(func(k, v []byte) error {
			var meta model.ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil // skip corrupt entries
			}
			if meta.RefCount <= 0 && !meta.Pinned && !meta.UnreferencedAt.IsZero() && meta.UnreferencedAt.Before(cutoff) {
				toDelete = append(toDelete, string(k))
			}
			return nil
		})
	})

	for _, hashHex := range toDelete {
		if err := DeleteBlob(gc.store.dir, hashHex); err != nil {
			gc.log.Warn("gc: failed to delete blob", "hash", hashHex[:12], "err", err)
			continue
		}
		gc.store.db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket(bucketObjects).Delete([]byte(hashHex))
		})
		gc.log.Info("gc: collected object", "hash", hashHex[:12])
	}
}
