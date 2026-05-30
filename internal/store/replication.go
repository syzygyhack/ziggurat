package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

// ShardPusher sends an object's data to a remote node.
type ShardPusher interface {
	PushShard(ctx context.Context, addr string, hash string, size int64, r io.Reader) error
}

// ECShardPusher pushes an individual erasure-coded shard to a remote node.
// ecMeta is JSON-serialized ErasureParams so the receiver can create metadata.
type ECShardPusher interface {
	PushECShard(ctx context.Context, addr string, hash string, shardIndex int, data []byte, ecMeta []byte) error
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

// RingProvider resolves an object key to ordered node IDs via consistent hashing.
type RingProvider interface {
	GetNodes(key string, n int) []string
}

// Replicator handles background replication of objects to peer nodes.
// For replicated objects it pushes full blobs. For erasure-coded objects
// it distributes individual shards to ring-determined nodes.
type Replicator struct {
	store   *Store
	localID string
	factor  int // desired replication factor
	pusher  ShardPusher
	ecPush  ECShardPusher // optional; nil disables EC-aware distribution
	peers   PeerProvider
	ring    RingProvider // optional; used for EC shard placement
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

// SetECPusher configures the pusher used to distribute individual EC shards.
func (r *Replicator) SetECPusher(p ECShardPusher) {
	r.ecPush = p
}

// SetRing configures the consistent hash ring used for EC shard placement.
func (r *Replicator) SetRing(ring RingProvider) {
	r.ring = ring
}

// AfterPut is called after a successful local Put. It records the local
// shard placement and replicates to peers up to replication_factor.
// For erasure-coded objects with a ring and EC pusher configured, individual
// shards are distributed to ring-determined nodes instead of full blobs.
func (r *Replicator) AfterPut(ctx context.Context, hashHex string) error {
	// Record local placement (index 0 = full blob).
	if err := r.addPlacement(hashHex, r.localID, 0); err != nil {
		return fmt.Errorf("record local placement: %w", err)
	}

	meta, err := r.store.getMeta(hashHex)
	if err != nil {
		return fmt.Errorf("get meta: %w", err)
	}

	// For EC objects with ring + EC pusher and enough distinct nodes,
	// distribute individual shards. Otherwise fall through to full-blob replication.
	if meta.Erasure != nil && r.ecPush != nil && r.ring != nil {
		totalShards := meta.Erasure.DataShards + meta.Erasure.ParityShards
		ringNodes := r.ring.GetNodes(hashHex, totalShards)
		if len(ringNodes) >= totalShards {
			return r.distributeECShards(ctx, hashHex, meta, ringNodes)
		}
		r.log.Info("insufficient nodes for EC distribution, falling back to replication",
			"hash", hashHex[:12], "have_nodes", len(ringNodes), "need", totalShards)
	}

	// Standard full-blob replication path.
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
		if closeErr := rc.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			}
			r.log.Error("integrity check failed during data movement", "hash", hashHex[:12], "err", closeErr)
		}
		if err != nil {
			r.log.Warn("replication push failed", "hash", hashHex[:12], "peer", peer.Addr, "err", err)
			continue
		}

		// Record remote shard placement (index 0 = full blob).
		if err := r.addPlacement(hashHex, peer.NodeID, 0); err != nil {
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

// distributeECShards reads local EC shards and pushes each to a different
// node determined by the consistent hash ring. The caller must ensure
// len(ringNodes) >= totalShards.
//
// EC shard placements ARE recorded in the origin's meta.Shards so that GC
// can notify shard holders to retire, and so cross-node reconstruction can
// locate shards by querying the origin's metadata.
func (r *Replicator) distributeECShards(ctx context.Context, hashHex string, meta *model.ObjectMeta, ringNodes []string) error {
	ec := meta.Erasure
	totalShards := ec.DataShards + ec.ParityShards

	// Build ErasureParams for the wire that includes the ring assignment
	// so receiver nodes can discover shard holders without the origin's
	// meta.Shards (which only the origin records).
	ecForWire := *ec
	ecForWire.ShardNodes = make([]string, totalShards)
	for i := 0; i < totalShards; i++ {
		if i < len(ringNodes) {
			ecForWire.ShardNodes[i] = ringNodes[i]
		}
	}
	ecMeta, err := json.Marshal(&ecForWire)
	if err != nil {
		return fmt.Errorf("serialize erasure params: %w", err)
	}

	// Build a map of nodeID -> gRPC address from the peer provider.
	peers := r.peers.Peers("") // get all peers, including self
	addrMap := make(map[string]string, len(peers))
	for _, p := range peers {
		addrMap[p.NodeID] = p.Addr
	}

	// Read locally available shard indices.
	localIndices, err := ListLocalShardIndices(r.store.dir, hashHex)
	if err != nil {
		return fmt.Errorf("list local shards: %w", err)
	}

	pushed := 0
	for _, idx := range localIndices {
		if ctx.Err() != nil {
			break
		}
		if idx >= totalShards {
			continue
		}

		targetNodeID := ringNodes[idx]

		// If this shard stays local, record placement and move on.
		if targetNodeID == r.localID {
			if err := r.addPlacement(hashHex, r.localID, idx); err != nil {
				r.log.Warn("record local EC shard placement", "hash", hashHex[:12], "index", idx, "err", err)
			}
			continue
		}

		addr, ok := addrMap[targetNodeID]
		if !ok || addr == "" {
			r.log.Debug("no address for ring target", "node", targetNodeID, "shard", idx)
			continue
		}

		// Read the shard from disk.
		path := shardPath(r.store.dir, hashHex, idx)
		data, err := os.ReadFile(path)
		if err != nil {
			r.log.Warn("read local shard for distribution", "hash", hashHex[:12], "index", idx, "err", err)
			continue
		}

		if err := r.ecPush.PushECShard(ctx, addr, hashHex, idx, data, ecMeta); err != nil {
			r.log.Warn("ec shard push failed", "hash", hashHex[:12], "index", idx, "peer", addr, "err", err)
			continue
		}

		// Record remote shard placement so GC can retire and reconstruction
		// can locate this shard.
		if err := r.addPlacement(hashHex, targetNodeID, idx); err != nil {
			r.log.Warn("record remote EC shard placement", "hash", hashHex[:12], "index", idx, "peer", targetNodeID, "err", err)
		}

		pushed++
		r.log.Info("distributed ec shard", "hash", hashHex[:12], "index", idx, "peer", addr)
	}

	// Count shards that stay local (assigned to us by the ring).
	localCount := 0
	for i := 0; i < totalShards; i++ {
		if i < len(ringNodes) && ringNodes[i] == r.localID {
			localCount++
		}
	}
	totalDistributed := pushed + localCount

	if totalDistributed < totalShards {
		r.markUnderReplicated(hashHex)
	} else {
		// All shards placed — clear any prior under-replication mark.
		r.clearUnderReplicated(hashHex)
	}
	return nil
}

// addPlacement records a shard placement for a node in object metadata.
// index is the shard index: 0 for full-blob replication, or the EC shard
// index for erasure-coded objects. Uses a read-only check first to skip
// the write transaction when the placement already exists.
func (r *Replicator) addPlacement(hashHex string, nodeID string, index int) error {
	// Fast path: check if placement already exists without a write lock.
	var exists bool
	r.store.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		data := b.Get([]byte(hashHex))
		if data == nil {
			return nil
		}
		var meta model.ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil
		}
		for _, s := range meta.Shards {
			if s.NodeID == nodeID && s.Index == index {
				exists = true
				break
			}
		}
		return nil
	})
	if exists {
		return nil
	}

	return r.store.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return fmt.Errorf("objects bucket not initialized")
		}
		data := b.Get([]byte(hashHex))
		if data == nil {
			return fmt.Errorf("object not found: %s", hashHex[:12])
		}

		var meta model.ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return err
		}

		// Re-check inside write tx (another goroutine may have added it).
		for _, s := range meta.Shards {
			if s.NodeID == nodeID && s.Index == index {
				return nil
			}
		}

		meta.Shards = append(meta.Shards, model.ShardPlacement{
			Index:    index,
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

// clearUnderReplicated removes a hash from the under-replicated set.
func (r *Replicator) clearUnderReplicated(hashHex string) {
	r.mu.Lock()
	delete(r.underRepMap, hashHex)
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

		// EC objects: distributeECShards calls clearUnderReplicated on
		// success, so check if it was already removed.
		r.mu.Lock()
		_, stillUnder := r.underRepMap[h]
		r.mu.Unlock()
		if !stillUnder {
			repaired++
			continue
		}

		// Replicated objects: check shard placement count.
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

// TriggerRebalance runs a rebalance pass asynchronously for a newly joined
// node. The goroutine is tracked via repairWg so WaitForRepairs blocks until
// it completes, preventing races with store/DB close during shutdown.
func (r *Replicator) TriggerRebalance(ctx context.Context, newNodeID string) {
	r.repairWg.Add(1)
	go func() {
		defer r.repairWg.Done()
		if n := r.Rebalance(ctx, newNodeID); n > 0 {
			r.log.Info("rebalanced objects to new node", "node", newNodeID, "count", n)
		}
	}()
}

// TriggerMigrateAll runs MigrateAll asynchronously. The goroutine is tracked
// via repairWg so WaitForRepairs blocks until it completes.
func (r *Replicator) TriggerMigrateAll(ctx context.Context) {
	r.repairWg.Add(1)
	go func() {
		defer r.repairWg.Done()
		if n := r.MigrateAll(ctx); n > 0 {
			r.log.Info("drain: migrated objects to peers", "count", n)
		}
	}()
}

// WaitForRepairs blocks until all in-flight TriggerRepair goroutines complete.
// Call this during shutdown to prevent races with store/DB close.
func (r *Replicator) WaitForRepairs() {
	r.repairWg.Wait()
}

// RemoveNodePlacements removes all shard placements referencing departedID
// from every object's metadata. Objects that drop below the replication factor
// are marked under-replicated so the next repair pass re-distributes them.
func (r *Replicator) RemoveNodePlacements(departedID string) {
	// Collect keys that need updating first — bbolt forbids mutation
	// inside ForEach (undefined behavior on page splits).
	var toUpdate []string
	r.store.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var meta model.ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil
			}
			for _, sp := range meta.Shards {
				if sp.NodeID == departedID {
					toUpdate = append(toUpdate, string(k))
					break
				}
			}
			return nil
		})
	})

	var affected int
	r.store.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		for _, hashHex := range toUpdate {
			data := b.Get([]byte(hashHex))
			if data == nil {
				continue
			}
			var meta model.ObjectMeta
			if err := json.Unmarshal(data, &meta); err != nil {
				continue
			}
			filtered := make([]model.ShardPlacement, 0, len(meta.Shards))
			for _, sp := range meta.Shards {
				if sp.NodeID != departedID {
					filtered = append(filtered, sp)
				}
			}
			if len(filtered) == len(meta.Shards) {
				continue
			}
			meta.Shards = filtered
			updated, err := json.Marshal(&meta)
			if err != nil {
				continue
			}
			if err := b.Put([]byte(hashHex), updated); err != nil {
				continue
			}
			affected++
		}
		return nil
	})

	if affected > 0 {
		r.log.Info("removed departed node placements", "node", departedID, "objects_affected", affected)
	}

	// Re-scan to mark objects that are now under-replicated.
	r.store.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var meta model.ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil
			}
			hashHex := string(k)
			if meta.Erasure != nil {
				totalShards := meta.Erasure.DataShards + meta.Erasure.ParityShards
				if len(meta.Shards) < totalShards {
					r.markUnderReplicated(hashHex)
				}
			} else if len(meta.Shards) < r.factor {
				r.markUnderReplicated(hashHex)
			}
			return nil
		})
	})
}

// Rebalance pushes local objects to a newly joined node when the consistent
// hash ring indicates they should live there. Only replicated (full-blob)
// objects are considered; EC shard redistribution is handled by the repair
// loop. Returns the number of objects pushed.
func (r *Replicator) Rebalance(ctx context.Context, newNodeID string) int {
	if r.ring == nil {
		return 0
	}

	// Resolve the new node's gRPC address.
	peers := r.peers.Peers(r.localID)
	var newAddr string
	for _, p := range peers {
		if p.NodeID == newNodeID {
			newAddr = p.Addr
			break
		}
	}
	if newAddr == "" {
		r.log.Debug("rebalance: no address for new node", "node", newNodeID)
		return 0
	}

	// Collect all local objects.
	type objEntry struct {
		hashHex string
		meta    model.ObjectMeta
	}
	var objects []objEntry
	r.store.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var meta model.ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil
			}
			objects = append(objects, objEntry{hashHex: string(k), meta: meta})
			return nil
		})
	})

	pushed := 0
	for _, obj := range objects {
		if ctx.Err() != nil {
			break
		}

		// Skip EC objects — those are handled by repair/distribution.
		if obj.meta.Erasure != nil {
			continue
		}

		// Check if the ring assigns this object to the new node.
		ringNodes := r.ring.GetNodes(obj.hashHex, r.factor)
		assigned := false
		for _, n := range ringNodes {
			if n == newNodeID {
				assigned = true
				break
			}
		}
		if !assigned {
			continue
		}

		// Check if the new node already has a placement.
		alreadyPlaced := false
		for _, sp := range obj.meta.Shards {
			if sp.NodeID == newNodeID {
				alreadyPlaced = true
				break
			}
		}
		if alreadyPlaced {
			continue
		}

		// Push full blob to the new node.
		rc, err := r.store.GetByHash(ctx, obj.hashHex)
		if err != nil {
			r.log.Warn("rebalance: open blob failed", "hash", obj.hashHex[:12], "err", err)
			continue
		}
		err = r.pusher.PushShard(ctx, newAddr, obj.hashHex, obj.meta.Size, rc)
		if closeErr := rc.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			}
			r.log.Error("integrity check failed during data movement", "hash", obj.hashHex[:12], "err", closeErr)
		}
		if err != nil {
			r.log.Warn("rebalance: push failed", "hash", obj.hashHex[:12], "peer", newAddr, "err", err)
			continue
		}

		if err := r.addPlacement(obj.hashHex, newNodeID, 0); err != nil {
			r.log.Warn("rebalance: record placement failed", "hash", obj.hashHex[:12], "err", err)
		}

		pushed++
		r.log.Info("rebalanced object to new node", "hash", obj.hashHex[:12], "peer", newAddr)
	}

	return pushed
}

// MigrateAll pushes all local objects to available peers. Used during drain
// to ensure data survives before the node shuts down. Objects that already
// have at least one remote placement are skipped. Returns the number of
// objects successfully migrated.
func (r *Replicator) MigrateAll(ctx context.Context) int {
	peers := r.peers.Peers(r.localID)
	if len(peers) == 0 {
		r.log.Warn("migrate: no peers available")
		return 0
	}

	// Collect all local objects.
	type objEntry struct {
		hashHex string
		meta    model.ObjectMeta
	}
	var objects []objEntry
	r.store.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var meta model.ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil
			}
			objects = append(objects, objEntry{hashHex: string(k), meta: meta})
			return nil
		})
	})

	migrated := 0
	peerIdx := 0
	for _, obj := range objects {
		if ctx.Err() != nil {
			break
		}

		// Skip if any remote node already has a copy.
		hasRemote := false
		for _, sp := range obj.meta.Shards {
			if sp.NodeID != r.localID {
				hasRemote = true
				break
			}
		}
		if hasRemote {
			continue
		}

		// Round-robin across available peers.
		peer := peers[peerIdx%len(peers)]
		peerIdx++

		rc, err := r.store.GetByHash(ctx, obj.hashHex)
		if err != nil {
			r.log.Warn("migrate: open blob failed", "hash", obj.hashHex[:12], "err", err)
			continue
		}
		err = r.pusher.PushShard(ctx, peer.Addr, obj.hashHex, obj.meta.Size, rc)
		if closeErr := rc.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			}
			r.log.Error("integrity check failed during data movement", "hash", obj.hashHex[:12], "err", closeErr)
		}
		if err != nil {
			r.log.Warn("migrate: push failed", "hash", obj.hashHex[:12], "peer", peer.Addr, "err", err)
			continue
		}

		if err := r.addPlacement(obj.hashHex, peer.NodeID, 0); err != nil {
			r.log.Warn("migrate: record placement failed", "hash", obj.hashHex[:12], "err", err)
		}

		migrated++
		r.log.Info("migrated object", "hash", obj.hashHex[:12], "peer", peer.Addr)
	}

	return migrated
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
