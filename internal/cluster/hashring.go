package cluster

import (
	"encoding/binary"
	"sort"
	"sync"

	"github.com/zeebo/blake3"
)

// HashRing implements consistent hashing for deterministic shard placement.
// Given a content hash and a desired replica count, it returns the ordered
// list of node IDs responsible for storing that content. When nodes join or
// leave, only 1/N of keys are remapped.
//
// Each physical node gets VNodesPerNode virtual nodes on the ring to ensure
// even distribution even with small cluster sizes.
type HashRing struct {
	mu       sync.RWMutex
	vnodes   []vnode
	nodeSet  map[string]bool // tracks physical node membership
	replicas int             // virtual nodes per physical node
}

type vnode struct {
	hash   uint64
	nodeID string
}

const defaultVNodes = 128

// NewHashRing creates a ring with the given virtual node count per physical node.
// If replicas <= 0, defaultVNodes (128) is used.
func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = defaultVNodes
	}
	return &HashRing{
		replicas: replicas,
		nodeSet:  make(map[string]bool),
	}
}

// AddNode adds a physical node to the ring. Safe to call multiple times
// for the same node (idempotent).
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodeSet[nodeID] {
		return
	}
	r.nodeSet[nodeID] = true

	for i := 0; i < r.replicas; i++ {
		h := vnodeHash(nodeID, i)
		r.vnodes = append(r.vnodes, vnode{hash: h, nodeID: nodeID})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
func (r *HashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodeSet[nodeID] {
		return
	}
	delete(r.nodeSet, nodeID)

	filtered := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.nodeID != nodeID {
			filtered = append(filtered, vn)
		}
	}
	r.vnodes = filtered
}

// GetNodes returns up to n distinct physical nodes responsible for the given key.
// Walks clockwise from the key's position on the ring, collecting unique node IDs.
// Returns fewer than n if the ring has fewer than n physical nodes.
func (r *HashRing) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	h := keyHash(key)
	start := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})

	seen := make(map[string]bool, n)
	var result []string
	for i := 0; i < len(r.vnodes) && len(result) < n; i++ {
		idx := (start + i) % len(r.vnodes)
		nodeID := r.vnodes[idx].nodeID
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}
	return result
}

// GetNode returns the single primary node for a key. Shorthand for GetNodes(key, 1)[0].
func (r *HashRing) GetNode(key string) string {
	nodes := r.GetNodes(key, 1)
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0]
}

// Members returns all physical node IDs currently on the ring.
func (r *HashRing) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	members := make([]string, 0, len(r.nodeSet))
	for id := range r.nodeSet {
		members = append(members, id)
	}
	sort.Strings(members)
	return members
}

// Size returns the number of physical nodes on the ring.
func (r *HashRing) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodeSet)
}

// vnodeHash produces a deterministic hash for a (nodeID, index) pair.
func vnodeHash(nodeID string, index int) uint64 {
	hasher := blake3.New()
	hasher.Write([]byte(nodeID))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(index))
	hasher.Write(buf[:])
	sum := hasher.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

// keyHash produces a deterministic hash for a content key.
func keyHash(key string) uint64 {
	hasher := blake3.New()
	hasher.Write([]byte(key))
	sum := hasher.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}
