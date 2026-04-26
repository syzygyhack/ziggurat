package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/zeebo/blake3"
	"go.etcd.io/bbolt"
)

// ShardFetcher fetches individual EC shards from remote nodes.
// Used by getByHashEC for cross-node reconstruction when local shards
// are insufficient.
type ShardFetcher interface {
	// FetchShard downloads shard shardIndex of the object identified by hashHex
	// from the node identified by nodeID. The implementation resolves nodeID
	// to a network address. Returns raw shard bytes.
	FetchShard(ctx context.Context, nodeID string, hashHex string, shardIndex int) ([]byte, error)
}

// Store is the content-addressed object store with namespace resolution.
type Store struct {
	cfg          config.StorageConfig
	dir          string // root directory for shard files
	db           *bbolt.DB
	log          *slog.Logger
	onPut        func(ctx context.Context, hashHex string) // optional post-Put callback (e.g. replication)
	erasure      *ErasureCodec                              // nil when erasure coding disabled
	shardFetcher ShardFetcher                               // optional; enables cross-node EC reconstruction
}

// New creates a Store rooted at the given data directory.
func New(cfg config.StorageConfig, dataDir string, log *slog.Logger) (*Store, error) {
	storeDir := cfg.DataDir
	if storeDir == "" {
		storeDir = filepath.Join(dataDir, "store")
	}
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}

	metaDir := filepath.Join(dataDir, "metadata")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metadata dir: %w", err)
	}

	db, err := bbolt.Open(filepath.Join(metaDir, "objects.db"), 0o644, nil)
	if err != nil {
		return nil, fmt.Errorf("open metadata db: %w", err)
	}

	// Ensure buckets exist.
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{"objects", "namespaces"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init buckets: %w", err)
	}

	s := &Store{
		cfg: cfg,
		dir: storeDir,
		db:  db,
		log: log,
	}

	// Initialize erasure codec if enabled.
	if cfg.Erasure.Enabled && cfg.Erasure.DataShards > 0 && cfg.Erasure.ParityShards > 0 {
		ec, err := NewErasureCodec(cfg.Erasure.DataShards, cfg.Erasure.ParityShards)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("init erasure codec: %w", err)
		}
		s.erasure = ec
		log.Info("erasure coding enabled", "data_shards", cfg.Erasure.DataShards, "parity_shards", cfg.Erasure.ParityShards)
	}

	return s, nil
}

// Dir returns the root directory for shard files.
func (s *Store) Dir() string {
	return s.dir
}

// SetOnPut registers a callback invoked after every successful Put. Used to
// trigger replication (Replicator.AfterPut). Not goroutine-safe — call before
// the store handles traffic.
func (s *Store) SetOnPut(fn func(ctx context.Context, hashHex string)) {
	s.onPut = fn
}

// SetShardFetcher registers a remote shard fetcher for cross-node EC
// reconstruction. Not goroutine-safe — call before the store handles traffic.
func (s *Store) SetShardFetcher(sf ShardFetcher) {
	s.shardFetcher = sf
}

// Close shuts down the store, closing the metadata database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Put stores data from r under the given namespace key. Returns the content hash.
// Large objects are automatically erasure-coded when the codec is enabled.
func (s *Store) Put(ctx context.Context, nsKey string, r io.Reader) (string, error) {
	hash, size, _, err := WriteBlob(s.dir, r)
	if err != nil {
		return "", fmt.Errorf("write blob: %w", err)
	}

	hashHex := hex.EncodeToString(hash[:])
	tier := classifyTier(size, s.cfg.TierThresholds)

	// Atomically create metadata or increment refcount if hash already exists.
	if err := s.putOrIncrRef(hashHex, hash, size, tier, nsKey); err != nil {
		return "", fmt.Errorf("store metadata: %w", err)
	}

	// For large objects with erasure coding enabled, produce EC shards.
	// Runs after putOrIncrRef so metadata exists for setErasureParams.
	if tier == model.TierLarge && s.erasure != nil {
		if ecErr := s.createErasureShards(hashHex, size); ecErr != nil {
			s.log.Warn("erasure coding failed, falling back to replication",
				"hash", hashHex[:12], "err", ecErr)
		}
	}

	if err := s.setNamespace(nsKey, hashHex); err != nil {
		return "", fmt.Errorf("set namespace: %w", err)
	}

	s.log.Info("object stored", "key", nsKey, "hash", hashHex[:12], "size", size, "tier", tier)

	if s.onPut != nil {
		s.onPut(ctx, hashHex)
	}

	return hashHex, nil
}

// createErasureShards reads a blob back from disk, encodes it, and writes shards.
// Updates the object metadata with ErasureParams.
func (s *Store) createErasureShards(hashHex string, size int64) error {
	rc, err := ReadBlob(s.dir, hashHex)
	if err != nil {
		return fmt.Errorf("open blob for erasure: %w", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("read blob for erasure: %w", err)
	}

	shards, err := s.erasure.Encode(data)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	shardHashes, err := WriteShards(s.dir, hashHex, shards)
	if err != nil {
		return fmt.Errorf("write shards: %w", err)
	}

	// Convert shard hashes to hex strings for metadata.
	hexHashes := make([]string, len(shardHashes))
	for i, h := range shardHashes {
		hexHashes[i] = hex.EncodeToString(h[:])
	}

	shardSize := int64(0)
	if len(shards) > 0 {
		shardSize = int64(len(shards[0]))
	}

	// Store erasure params in metadata.
	return s.setErasureParams(hashHex, &model.ErasureParams{
		DataShards:   s.erasure.DataShards,
		ParityShards: s.erasure.ParityShards,
		OriginalSize: size,
		ShardSize:    shardSize,
		ShardHashes:  hexHashes,
	})
}

// setErasureParams updates ObjectMeta with erasure coding parameters.
func (s *Store) setErasureParams(hashHex string, params *model.ErasureParams) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		data := b.Get([]byte(hashHex))
		if data == nil {
			// Object not yet in metadata — it will be added by putOrIncrRef.
			// Store params in a temporary location and merge later.
			return nil
		}
		var meta model.ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return err
		}
		meta.Strategy = model.ErasureCoded
		meta.Erasure = params
		updated, err := json.Marshal(&meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(hashHex), updated)
	})
}

// Get retrieves an object by namespace key. Returns a reader over the content.
func (s *Store) Get(ctx context.Context, nsKey string) (io.ReadCloser, error) {
	hashHex, err := s.resolveNamespace(nsKey)
	if err != nil {
		return nil, err
	}
	return s.GetByHash(ctx, hashHex)
}

// GetByHash retrieves an object by content hash. For erasure-coded objects,
// it reconstructs from available shards if the full blob is not on disk.
func (s *Store) GetByHash(ctx context.Context, hashHex string) (io.ReadCloser, error) {
	// Try the fast path first: full blob on disk.
	rc, err := ReadBlob(s.dir, hashHex)
	if err == nil {
		return rc, nil
	}

	// If erasure coding is available, try reconstruction from shards.
	if s.erasure != nil {
		return s.getByHashEC(ctx, hashHex)
	}
	return nil, err
}

// getByHashEC reconstructs an erasure-coded object from local shards,
// fetching missing shards from remote nodes when a ShardFetcher is configured.
func (s *Store) getByHashEC(ctx context.Context, hashHex string) (io.ReadCloser, error) {
	meta, err := s.getMeta(hashHex)
	if err != nil {
		return nil, fmt.Errorf("get meta for EC read: %w", err)
	}
	if meta.Erasure == nil {
		return nil, fmt.Errorf("object %s has no erasure params", hashHex[:12])
	}

	ec := meta.Erasure
	totalShards := ec.DataShards + ec.ParityShards

	// Parse shard hashes.
	shardHashes := make([][32]byte, len(ec.ShardHashes))
	for i, h := range ec.ShardHashes {
		decoded, err := hex.DecodeString(h)
		if err != nil {
			return nil, fmt.Errorf("decode shard hash %d: %w", i, err)
		}
		copy(shardHashes[i][:], decoded)
	}

	// Reuse the store's codec when parameters match to avoid repeated allocation.
	codec := s.erasure
	if codec == nil || codec.DataShards != ec.DataShards || codec.ParityShards != ec.ParityShards {
		var err error
		codec, err = NewErasureCodec(ec.DataShards, ec.ParityShards)
		if err != nil {
			return nil, fmt.Errorf("create codec: %w", err)
		}
	}

	// Read locally available shards.
	shards := ReadLocalShards(s.dir, hashHex, totalShards, shardHashes)

	available := 0
	for _, sh := range shards {
		if sh != nil {
			available++
		}
	}

	// If local shards are insufficient and we have a remote fetcher, try
	// to pull missing shards from peer nodes listed in metadata.
	if available < ec.DataShards && s.shardFetcher != nil {
		// Build index -> nodeID from shard placements (origin stores these).
		placementAddr := make(map[int]string)
		for _, sp := range meta.Shards {
			if sp.Index >= 0 && sp.Index < totalShards {
				placementAddr[sp.Index] = sp.NodeID
			}
		}
		// Receiver nodes don't have meta.Shards populated — fall back to
		// ShardNodes from ErasureParams (set by the origin during distribution).
		if len(placementAddr) == 0 && len(ec.ShardNodes) > 0 {
			for idx := 0; idx < totalShards && idx < len(ec.ShardNodes); idx++ {
				if ec.ShardNodes[idx] != "" {
					placementAddr[idx] = ec.ShardNodes[idx]
				}
			}
		}

		for idx := 0; idx < totalShards && available < ec.DataShards; idx++ {
			if shards[idx] != nil {
				continue // already have this shard locally
			}
			nodeID, ok := placementAddr[idx]
			if !ok || nodeID == "" {
				continue
			}
			data, err := s.shardFetcher.FetchShard(ctx, nodeID, hashHex, idx)
			if err != nil {
				s.log.Debug("remote shard fetch failed", "hash", hashHex[:12], "index", idx, "node", nodeID, "err", err)
				continue
			}
			// Verify fetched shard integrity.
			if idx < len(shardHashes) {
				hasher := blake3.New()
				hasher.Write(data)
				var actual [32]byte
				hasher.Sum(actual[:0])
				if actual != shardHashes[idx] {
					s.log.Warn("remote shard integrity mismatch", "hash", hashHex[:12], "index", idx, "node", nodeID)
					continue
				}
			}
			shards[idx] = data
			available++
		}
	}

	if available < ec.DataShards {
		return nil, fmt.Errorf("need %d shards but only %d available for %s", ec.DataShards, available, hashHex[:12])
	}

	decoded, err := codec.Decode(shards, ec.OriginalSize)
	if err != nil {
		return nil, fmt.Errorf("reconstruct %s: %w", hashHex[:12], err)
	}

	// Re-verify the reconstructed blob against its content hash.
	// Without this, a malicious peer could poison shards so the
	// reconstruction produces bytes that don't match the content address.
	hasher := blake3.New()
	hasher.Write(decoded)
	var gotHash [32]byte
	hasher.Sum(gotHash[:0])
	if hex.EncodeToString(gotHash[:]) != hashHex {
		return nil, fmt.Errorf("reconstructed data integrity check failed for %s", hashHex[:12])
	}

	return io.NopCloser(bytes.NewReader(decoded)), nil
}

// ErasureCodec returns the store's erasure codec, or nil if disabled.
func (s *Store) ErasureCodec() *ErasureCodec {
	return s.erasure
}

// Resolve translates a namespace key to its content hash. Returns empty string if not found.
func (s *Store) Resolve(nsKey string) (string, error) {
	return s.resolveNamespace(nsKey)
}

// Delete removes a namespace key and decrements the refcount on the underlying object.
func (s *Store) Delete(ctx context.Context, nsKey string) error {
	hashHex, err := s.resolveNamespace(nsKey)
	if err != nil {
		return err
	}

	if err := s.deleteNamespace(nsKey); err != nil {
		return err
	}

	if err := s.decrRef(hashHex); err != nil {
		return err
	}

	s.log.Info("namespace deleted", "key", nsKey, "hash", hashHex[:12])
	return nil
}

// List returns namespace keys matching the given prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	return s.listNamespaces(prefix)
}

// PutReplica creates ObjectMeta for a blob that was already written to disk
// (e.g. via PushShard). This ensures the replica is visible to GC, Stats, and
// NodesForHash. Idempotent: if metadata already exists and is active (RefCount > 0),
// this is a no-op. If the object was retired (RefCount == 0), it is re-activated.
// This prevents transport retries from inflating refcounts and blocking retirement.
func (s *Store) PutReplica(hashHex string, hash [32]byte, size int64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return fmt.Errorf("objects bucket not initialized")
		}
		if existing := b.Get([]byte(hashHex)); existing != nil {
			var meta model.ObjectMeta
			if err := json.Unmarshal(existing, &meta); err != nil {
				return err
			}
			if meta.RefCount > 0 {
				return nil // already active — idempotent on retry
			}
			// Re-activate a retired replica.
			meta.RefCount = 1
			meta.UnreferencedAt = time.Time{}
			updated, err := json.Marshal(&meta)
			if err != nil {
				return err
			}
			return b.Put([]byte(hashHex), updated)
		}

		tier := classifyTier(size, s.cfg.TierThresholds)
		meta := model.ObjectMeta{
			Hash:      hash,
			Size:      size,
			Tier:      tier,
			Strategy:  model.Replicated,
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

// PutECShardReplica creates ObjectMeta with ErasureParams for an EC shard
// that was received from a remote node. This lets the receiver eventually
// reconstruct the full object once enough shards arrive. If metadata already
// exists and is pinned, this is a no-op (idempotent on retry). If metadata
// exists but was retired, the record is re-pinned with RefCount=1 so GC
// will not reclaim it.
func (s *Store) PutECShardReplica(hashHex string, ecParams *model.ErasureParams) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return fmt.Errorf("objects bucket not initialized")
		}
		if existing := b.Get([]byte(hashHex)); existing != nil {
			var meta model.ObjectMeta
			if err := json.Unmarshal(existing, &meta); err != nil {
				return err
			}
			if meta.Pinned {
				return nil // already active — idempotent on retry
			}
			// Re-pin: a retired replica receiving fresh shard data must be
			// marked active again so GC does not reclaim it.
			meta.Pinned = true
			meta.RefCount = 1
			meta.UnreferencedAt = time.Time{} // clear GC timer
			if ecParams.ShardSize > 0 {
				meta.Size = ecParams.ShardSize
			}
			updated, err := json.Marshal(&meta)
			if err != nil {
				return err
			}
			return b.Put([]byte(hashHex), updated)
		}

		decoded, err := hex.DecodeString(hashHex)
		if err != nil {
			return fmt.Errorf("decode hash: %w", err)
		}
		var h [32]byte
		copy(h[:], decoded)

		// Size reflects the local disk footprint (one shard), not the
		// full original object, so Stats().UsedBytes is accurate.
		localSize := ecParams.ShardSize
		if localSize <= 0 {
			localSize = ecParams.OriginalSize // fallback for legacy metadata
		}

		meta := model.ObjectMeta{
			Hash:      h,
			Size:      localSize,
			Tier:      classifyTier(localSize, s.cfg.TierThresholds),
			Strategy:  model.ErasureCoded,
			Erasure:   ecParams,
			Pinned:    true, // EC shard replicas must not be GC'd — other nodes depend on them for reconstruction
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

// RetireReplica removes a replica's hold on an object by unpinning it and
// decrementing its refcount. This starts the GC grace period for the replica.
// Called when the origin node (or its GC) signals that the object is no longer
// needed cluster-wide. Safe to call multiple times (idempotent unpin).
func (s *Store) RetireReplica(hashHex string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		data := b.Get([]byte(hashHex))
		if data == nil {
			return nil // already gone
		}
		var meta model.ObjectMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return err
		}
		meta.Pinned = false
		meta.RefCount--
		if meta.RefCount < 0 {
			meta.RefCount = 0
		}
		if meta.RefCount == 0 && meta.UnreferencedAt.IsZero() {
			meta.UnreferencedAt = time.Now()
		}
		updated, err := json.Marshal(&meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(hashHex), updated)
	})
}

// IncrRef increments the reference count for the object with the given hash.
func (s *Store) IncrRef(hashHex string) error {
	return s.incrRef(hashHex)
}

// DecrRef decrements the reference count for the object with the given hash.
func (s *Store) DecrRef(hashHex string) error {
	return s.decrRef(hashHex)
}

// StorageStats holds aggregate statistics about the object store.
type StorageStats struct {
	Objects  int   `json:"objects"`
	UsedBytes int64 `json:"used_bytes"`
	Capacity  int64 `json:"capacity"` // 0 = unlimited
}

// Stats returns aggregate storage statistics.
func (s *Store) Stats() StorageStats {
	var objects int
	var usedBytes int64

	s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var meta model.ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return nil
			}
			objects++
			usedBytes += meta.Size
			return nil
		})
	})

	return StorageStats{
		Objects:   objects,
		UsedBytes: usedBytes,
		Capacity:  s.cfg.Capacity,
	}
}

func classifyTier(size int64, thresholds config.TierConfig) model.Tier {
	switch {
	case size >= thresholds.Large:
		return model.TierLarge
	case size >= thresholds.Medium:
		return model.TierMedium
	default:
		return model.TierSmall
	}
}
