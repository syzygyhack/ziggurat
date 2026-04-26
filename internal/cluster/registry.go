package cluster

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/syzygyhack/ziggurat/internal/model"
)

// Registry tracks live cluster members and their metadata.
// It is updated by memberlist event callbacks.
type Registry struct {
	mu      sync.RWMutex
	nodes   map[string]*model.Node // keyed by node ID
	log     *slog.Logger
	onJoin  func(nodeID string) // optional callback when a new node joins
	onLeave func(nodeID string) // optional callback when a node departs
	Ring    *HashRing            // consistent hash ring, updated on join/leave
}

// NewRegistry creates an empty node registry with a consistent hash ring.
func NewRegistry(log *slog.Logger) *Registry {
	return &Registry{
		nodes: make(map[string]*model.Node),
		log:   log,
		Ring:  NewHashRing(0), // 0 = use default vnodes (128)
	}
}

// OnJoin registers a callback that fires after a new node is added to the
// registry. The callback receives the joined node's ID. Used by the
// replicator to trigger shard rebalancing.
func (r *Registry) OnJoin(fn func(nodeID string)) {
	r.mu.Lock()
	r.onJoin = fn
	r.mu.Unlock()
}

// OnLeave registers a callback that fires after a node is removed from the
// registry. The callback receives the departed node's ID. Used by the
// coordinator to re-queue tasks from failed nodes.
func (r *Registry) OnLeave(fn func(nodeID string)) {
	r.mu.Lock()
	r.onLeave = fn
	r.mu.Unlock()
}

// Add registers a node from memberlist data.
func (r *Registry) Add(mn *memberlist.Node) {
	meta, err := decodeMeta(mn.Meta)
	if err != nil {
		r.log.Warn("cluster: failed to decode node meta", "addr", mn.Addr, "err", err)
		return
	}

	// Build gRPC and HTTP addresses from the memberlist node's IP and advertised ports.
	grpcAddr := ""
	if meta.GRPCPort > 0 {
		grpcAddr = fmt.Sprintf("%s:%d", mn.Addr.String(), meta.GRPCPort)
	}
	httpAddr := ""
	if meta.HTTPPort > 0 {
		httpAddr = fmt.Sprintf("%s:%d", mn.Addr.String(), meta.HTTPPort)
	}

	n := &model.Node{
		ID:           meta.ID,
		Name:         meta.Name,
		Address:      mn.Address(),
		HTTPAddress:  httpAddr,
		GRPCAddress:  grpcAddr,
		Tags:         meta.Tags,
		Capabilities: meta.Caps,
		JoinedAt:     time.Now(),
		LastSeen:     time.Now(),
	}

	switch meta.Role {
	case "coordinator":
		n.Role = model.RoleCoordinator
	case "worker":
		n.Role = model.RoleWorker
	default:
		n.Role = model.RoleHybrid
	}

	r.mu.Lock()
	r.nodes[meta.ID] = n
	fn := r.onJoin
	r.mu.Unlock()

	r.Ring.AddNode(meta.ID)
	r.log.Info("cluster: node joined", "id", meta.ID, "name", meta.Name, "addr", mn.Address())

	if fn != nil {
		fn(meta.ID)
	}
}

// Remove deregisters a node and fires the OnLeave callback if set.
func (r *Registry) Remove(mn *memberlist.Node) {
	meta, err := decodeMeta(mn.Meta)
	if err != nil {
		// Fall back to removing by address.
		r.mu.Lock()
		var removedID string
		for id, n := range r.nodes {
			if n.Address == mn.Address() {
				delete(r.nodes, id)
				removedID = id
				r.log.Info("cluster: node left", "id", id, "addr", mn.Address())
				break
			}
		}
		fn := r.onLeave
		r.mu.Unlock()
		if removedID != "" {
			r.Ring.RemoveNode(removedID)
		}
		if fn != nil && removedID != "" {
			fn(removedID)
		}
		return
	}

	r.mu.Lock()
	delete(r.nodes, meta.ID)
	fn := r.onLeave
	r.mu.Unlock()

	r.Ring.RemoveNode(meta.ID)
	r.log.Info("cluster: node left", "id", meta.ID, "name", meta.Name)
	if fn != nil {
		fn(meta.ID)
	}
}

// Update refreshes metadata for an existing node.
func (r *Registry) Update(mn *memberlist.Node) {
	meta, err := decodeMeta(mn.Meta)
	if err != nil {
		return
	}

	r.mu.Lock()
	if n, ok := r.nodes[meta.ID]; ok {
		n.Tags = meta.Tags
		n.Capabilities = meta.Caps
		n.LastSeen = time.Now()
		n.Address = mn.Address()
		if meta.GRPCPort > 0 {
			n.GRPCAddress = fmt.Sprintf("%s:%d", mn.Addr.String(), meta.GRPCPort)
		}
		if meta.HTTPPort > 0 {
			n.HTTPAddress = fmt.Sprintf("%s:%d", mn.Addr.String(), meta.HTTPPort)
		}
	}
	r.mu.Unlock()
}

// Get returns a single node by ID.
func (r *Registry) Get(id string) (*model.Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// List returns all known nodes as copies.
func (r *Registry) List() []*model.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*model.Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		cp := *n
		result = append(result, &cp)
	}
	return result
}

// Count returns the number of live nodes.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
