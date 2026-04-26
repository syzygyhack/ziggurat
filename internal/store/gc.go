package store

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

// GC runs periodic garbage collection on objects with zero refcount.
type GC struct {
	store     *Store
	grace     time.Duration
	log       *slog.Logger
	onCollect func(hashHex string, shards []model.ShardPlacement) bool // returns true if peer retirements succeeded
}

// NewGC creates a garbage collector for the given store.
func NewGC(s *Store, grace time.Duration, log *slog.Logger) *GC {
	return &GC{store: s, grace: grace, log: log}
}

// SetOnCollect registers a callback invoked for each object before its
// metadata is removed. The callback receives the shard placements so the
// caller can notify peer nodes to retire their replicas. Return true if
// all reachable peers were retired successfully; false causes GC to skip
// metadata deletion so the object is retried next sweep. Not goroutine-safe
// — call before the GC loop starts.
func (gc *GC) SetOnCollect(fn func(hashHex string, shards []model.ShardPlacement) bool) {
	gc.onCollect = fn
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
		if b == nil {
			return nil
		}
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
		// Notify peers to retire their replicas BEFORE local deletion.
		// If retirement fails, skip this object so we retry next sweep
		// (prevents permanently pinned orphan replicas on peers).
		if gc.onCollect != nil {
			var shards []model.ShardPlacement
			if meta, err := gc.store.getMeta(hashHex); err == nil {
				shards = meta.Shards
			}
			if len(shards) > 0 {
				if !gc.onCollect(hashHex, shards) {
					gc.log.Warn("gc: peer retirement failed, deferring collection", "hash", hashHex[:12])
					continue
				}
			}
		}

		// Delete the full blob (may not exist for EC-only objects).
		// Only treat "not exist" as success — other errors (permissions,
		// I/O) mean the file is still on disk so we must keep metadata
		// to avoid orphaning bytes.
		blobErr := DeleteBlob(gc.store.dir, hashHex)
		if blobErr != nil && !errors.Is(blobErr, os.ErrNotExist) {
			gc.log.Warn("gc: blob delete failed, skipping metadata removal", "hash", hashHex[:12], "err", blobErr)
			continue
		}

		// Delete erasure-coded shard directory if present.
		shardErr := DeleteShards(gc.store.dir, hashHex)
		if shardErr != nil && !errors.Is(shardErr, os.ErrNotExist) {
			gc.log.Warn("gc: shard delete failed, skipping metadata removal", "hash", hashHex[:12], "err", shardErr)
			continue
		}

		gc.store.db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(bucketObjects)
			if b == nil {
				return nil
			}
			return b.Delete([]byte(hashHex))
		})

		gc.log.Info("gc: collected object", "hash", hashHex[:12])
	}
}
