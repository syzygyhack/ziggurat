package store

import (
	"sort"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// PlacementPolicy determines which nodes should hold shards of an object
// based on its tier, storage strategy, and available node capabilities.

// NodeInfo describes a candidate node for shard placement.
type NodeInfo struct {
	ID           string
	StorageClass model.StorageClass
	FreeBytes    int64
}

// PlacementStrategy holds the parameters for a placement decision.
type PlacementStrategy struct {
	Tier        model.Tier
	Strategy    model.StorageStrategy
	TotalShards int   // 1 for replicated, k+m for erasure coded
	ObjectSize  int64 // total original object size
	ShardSize   int64 // per-shard size (= ObjectSize for replicated)
}

// scoredNode pairs a node ID with its placement score.
type scoredNode struct {
	id   string
	rank int
	free int64
}

// PreferredClasses returns the storage classes preferred for a given tier,
// in priority order. Higher tiers prefer faster storage.
func PreferredClasses(tier model.Tier) []model.StorageClass {
	switch tier {
	case model.TierSmall:
		return []model.StorageClass{model.StorageClassNVMe, model.StorageClassSSD, model.StorageClassHDD}
	case model.TierMedium:
		return []model.StorageClass{model.StorageClassSSD, model.StorageClassNVMe, model.StorageClassHDD}
	case model.TierLarge:
		// Large objects prefer cheaper storage; erasure coding handles resilience.
		return []model.StorageClass{model.StorageClassHDD, model.StorageClassSSD, model.StorageClassNVMe}
	default:
		return []model.StorageClass{model.StorageClassSSD, model.StorageClassHDD, model.StorageClassNVMe}
	}
}

// SelectNodes picks nodes for shard placement. It tries to spread shards across
// distinct nodes, preferring nodes whose storage class matches the tier policy.
// Returns up to count node IDs. If not enough preferred-class nodes exist,
// falls back to any node with sufficient capacity.
func SelectNodes(strategy PlacementStrategy, candidates []NodeInfo, count int) []string {
	if count <= 0 || len(candidates) == 0 {
		return nil
	}

	preferred := PreferredClasses(strategy.Tier)

	var scored []scoredNode
	for _, c := range candidates {
		if c.FreeBytes > 0 && c.FreeBytes < strategy.ShardSize {
			continue // not enough space
		}
		rank := len(preferred) // default: worst rank
		for i, class := range preferred {
			if c.StorageClass == class {
				rank = i
				break
			}
		}
		scored = append(scored, scoredNode{
			id:   c.ID,
			rank: rank,
			free: c.FreeBytes,
		})
	}

	// Sort: best storage class first, then most free space.
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].rank != scored[j].rank {
			return scored[i].rank < scored[j].rank
		}
		return scored[i].free > scored[j].free
	})

	selected := make(map[string]bool, count)
	var result []string
	for _, sc := range scored {
		if len(result) >= count {
			break
		}
		if !selected[sc.id] {
			selected[sc.id] = true
			result = append(result, sc.id)
		}
	}
	return result
}

// StorageClassFromCaps extracts the storage class from a node's capabilities map.
// Reads the "storage.class" key. Returns StorageClassHDD as default.
func StorageClassFromCaps(caps map[string]string) model.StorageClass {
	v, ok := caps["storage.class"]
	if !ok {
		return model.StorageClassHDD
	}
	switch model.StorageClass(v) {
	case model.StorageClassNVMe:
		return model.StorageClassNVMe
	case model.StorageClassSSD:
		return model.StorageClassSSD
	case model.StorageClassHDD:
		return model.StorageClassHDD
	case model.StorageClassS3:
		return model.StorageClassS3
	default:
		return model.StorageClassHDD
	}
}
