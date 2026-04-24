package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

// ShardPusher sends an object's data to a remote node.
type ShardPusher interface {
	PushShard(ctx context.Context, addr string, hash string, size int64, r io.Reader) error
}

// Peer describes a remote node for replication.
type Peer struct {
	NodeID string
	Addr   string // gRPC address
}

// PeerProvider returns nodes eligible for replication, excluding the given node ID.
type PeerProvider interface {
	Peers(exclude string) []Peer
}

// Replicator handles background replication of objects to peer nodes.
type Replicator struct {
	store   *Store
	localID string
	factor  int // desired replication factor
	pusher  ShardPusher
	peers   PeerProvider
	log     *slog.Logger

	mu          sync.Mutex
	underRepMap map[string]struct{} // hashes queued for repair

	// repairWg tracks async repair goroutines spawned by TriggerRepair
	// so WaitForRepairs can block until they complete.
	repairWg sync.WaitGroup
}

// NewReplicator creates a Replicator.
func NewReplicator(s *Store, localID string, factor int, pusher ShardPusher, peers PeerProvider, log *slog.Logger) *Replicator {
	return &Replicator{
		store:       s,
		localID:     localID,
		factor:      factor,
		pusher:      pusher,
		peers:       peers,
		log:         log,
		underRepMap: make(map[string]struct{}),
	}
}

// AfterPut is called after a successful local Put. It records the local
// shard placement and replicates to peers up to replication_factor.
// Runs in the caller's goroutine; use a background worker for async replication.
func (r *Replicator) AfterPut(ctx context.Context, hashHex string) error {
	// Record local placement.
	if err := r.addPlacement(hashHex, r.localID); err != nil {
		return fmt.Errorf("record local placement: %w", err)
	}

	meta, err := r.store.getMeta(hashHex)
	if err != nil {
		return fmt.Errorf("get meta: %w", err)
	}

	needed := r.factor - len(meta.Shards)
	if needed <= 0 {
		return nil // already at desired replication level
	}

	peers := r.peers.Peers(r.localID)
	if len(peers) == 0 {
		if r.factor > 1 {
			r.markUnderReplicated(hashHex)
			r.log.Debug("no peers for replication", "hash", hashHex[:12], "needed", needed)
		}
		return nil
	}

	// Push to peers. Cap at needed replicas.
	pushed := 0
	for _, peer := range peers {
		if pushed >= needed {
			break
		}

		rc, err := r.store.GetByHash(ctx, hashHex)
		if err != nil {
			r.log.Error("failed to open blob for replication", "hash", hashHex[:12], "err", err)
			break
		}

		err = r.pusher.PushShard(ctx, peer.Addr, hashHex, meta.Size, rc)
		rc.Close()
		if err != nil {
			r.log.Warn("replication push failed", "hash", hashHex[:12], "peer", peer.Addr, "err", err)
			continue
		}

		// Record remote shard placement.
		if err := r.addPlacement(hashHex, peer.NodeID); err != nil {
			r.log.Warn("failed to record remote placement", "hash", hashHex[:12], "peer", peer.NodeID, "err", err)
		}

		pushed++
		r.log.Info("replicated shard", "hash", hashHex[:12], "peer", peer.Addr)
	}

	if pushed < needed {
		r.markUnderReplicated(hashHex)
		r.log.Warn("under-replicated object", "hash", hashHex[:12], "have", len(meta.Shards)+pushed, "want", r.factor)
	}

	return nil
}

// addPlacement records a shard placement for a node in object metadata.
func (r *Replicator) addPlacement(hashHex string, nodeID string) error {
	return r.store.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		data := b.Get([]byte(hashHex))
		if data == nil {
			return fmt.Errorf("object not found: %s", hashHex[:12])
		}

		var meta model.ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return err
		}

		// Don't add duplicate placements.
		for _, s := range meta.Shards {
			if s.NodeID == nodeID {
				return nil
			}
		}

		meta.Shards = append(meta.Shards, model.ShardPlacement{
			Index:    0, // single-shard replication; index always 0
			NodeID:   nodeID,
			Verified: time.Now(),
		})

		updated, err := json.Marshal(&meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(hashHex), updated)
	})
}

// markUnderReplicated queues a hash for future repair.
func (r *Replicator) markUnderReplicated(hashHex string) {
	r.mu.Lock()
	r.underRepMap[hashHex] = struct{}{}
	r.mu.Unlock()
}

// UnderReplicatedCount returns the number of objects below replication_factor.
func (r *Replicator) UnderReplicatedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.underRepMap)
}

// Repair attempts to replicate under-replicated objects. Call this when
// new nodes join or periodically. Returns the number of objects repaired.
func (r *Replicator) Repair(ctx context.Context) int {
	r.mu.Lock()
	hashes := make([]string, 0, len(r.underRepMap))
	for h := range r.underRepMap {
		hashes = append(hashes, h)
	}
	r.mu.Unlock()

	repaired := 0
	for _, h := range hashes {
		if ctx.Err() != nil {
			break
		}
		if err := r.AfterPut(ctx, h); err != nil {
			r.log.Warn("repair failed", "hash", h[:12], "err", err)
			continue
		}

		// Check if now fully replicated.
		meta, err := r.store.getMeta(h)
		if err != nil {
			continue
		}
		if len(meta.Shards) >= r.factor {
			r.mu.Lock()
			delete(r.underRepMap, h)
			r.mu.Unlock()
			repaired++
		}
	}
	return repaired
}

// RunRepairLoop periodically attempts to re-replicate under-replicated
// objects. Blocks until ctx is cancelled. Typically launched in a goroutine.
func (r *Replicator) RunRepairLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := r.Repair(ctx); n > 0 {
				r.log.Info("repair loop fixed objects", "count", n)
			}
		}
	}
}

// TriggerRepair runs a single repair pass asynchronously. The goroutine is
// tracked via repairWg so WaitForRepairs can block until it completes.
func (r *Replicator) TriggerRepair(ctx context.Context) {
	r.repairWg.Add(1)
	go func() {
		defer r.repairWg.Done()
		if n := r.Repair(ctx); n > 0 {
			r.log.Info("triggered repair fixed objects", "count", n)
		}
	}()
}

// WaitForRepairs blocks until all in-flight TriggerRepair goroutines complete.
// Call this during shutdown to prevent races with store/DB close.
func (r *Replicator) WaitForRepairs() {
	r.repairWg.Wait()
}

// NodesForHash returns the node IDs that hold a given object. Implements
// the scheduler.ObjectLocator interface.
func (r *Replicator) NodesForHash(hash string) []string {
	meta, err := r.store.getMeta(hash)
	if err != nil {
		return nil
	}
	var nodes []string
	for _, s := range meta.Shards {
		nodes = append(nodes, s.NodeID)
	}
	return nodes
}
