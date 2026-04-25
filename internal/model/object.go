package model

import "time"

// Tier classifies objects by size for storage strategy selection.
type Tier int

const (
	TierSmall  Tier = iota // < 1 MB: full replication
	TierMedium             // 1-64 MB: full replication
	TierLarge              // > 64 MB: erasure coding (Phase 1)
)

func (t Tier) String() string {
	switch t {
	case TierSmall:
		return "small"
	case TierMedium:
		return "medium"
	case TierLarge:
		return "large"
	default:
		return "unknown"
	}
}

// StorageStrategy indicates how an object's data is distributed.
type StorageStrategy int

const (
	Replicated   StorageStrategy = iota // full copies on N nodes
	ErasureCoded                        // Reed-Solomon k+m shards (Phase 1)
)

func (s StorageStrategy) String() string {
	switch s {
	case Replicated:
		return "replicated"
	case ErasureCoded:
		return "erasure_coded"
	default:
		return "unknown"
	}
}

// ObjectMeta is the authoritative metadata record for a stored object.
type ObjectMeta struct {
	Hash           [32]byte          `json:"hash"`
	Size           int64             `json:"size"`
	Tier           Tier              `json:"tier"`
	Strategy       StorageStrategy   `json:"strategy"`
	Shards         []ShardPlacement  `json:"shards"`
	Erasure        *ErasureParams    `json:"erasure,omitempty"` // set when Strategy == ErasureCoded
	Namespace      string            `json:"namespace"`
	Pinned         bool              `json:"pinned"`
	RefCount       int32             `json:"ref_count"`
	Tags           map[string]string `json:"tags,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	UnreferencedAt time.Time         `json:"unreferenced_at,omitempty"` // when refcount last reached 0
}

// ShardPlacement records where a shard is stored and when it was last verified.
type ShardPlacement struct {
	Index    int       `json:"index"`
	NodeID   string    `json:"node_id"`
	Verified time.Time `json:"verified"`
}

// ErasureParams records the encoding parameters for erasure-coded objects.
type ErasureParams struct {
	DataShards   int      `json:"data_shards"`
	ParityShards int      `json:"parity_shards"`
	OriginalSize int64    `json:"original_size"`
	ShardSize    int64    `json:"shard_size"`
	ShardHashes  []string `json:"shard_hashes"` // hex-encoded BLAKE3 per shard
}

// StorageClass indicates the type of storage hardware a node provides.
type StorageClass string

const (
	StorageClassNVMe StorageClass = "nvme"
	StorageClassSSD  StorageClass = "ssd"
	StorageClassHDD  StorageClass = "hdd"
	StorageClassS3   StorageClass = "s3"
)
