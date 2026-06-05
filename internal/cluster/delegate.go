package cluster

import (
	"log/slog"
	"maps"
	"slices"
	"sync"
)

// delegate implements memberlist.Delegate. It provides local node metadata
// to the gossip protocol and receives broadcast messages.
type delegate struct {
	mu          sync.Mutex
	meta        *NodeMeta // current local metadata
	cached      []byte    // meta encoded to fit cachedLimit
	cachedLimit int
	log         *slog.Logger
}

func newDelegate(meta *NodeMeta, log *slog.Logger) (*delegate, error) {
	return &delegate{meta: copyMeta(meta), log: log}, nil
}

// copyMeta returns a deep copy of m with its own Caps map and Tags slice, so
// the delegate never aliases a caller's map. Without this, the periodic
// capability refresh mutates the same map the delegate gossips, racing with
// memberlist's concurrent NodeMeta() marshaling (concurrent map read/write).
func copyMeta(m *NodeMeta) *NodeMeta {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Caps = maps.Clone(m.Caps)
	cp.Tags = slices.Clone(m.Tags)
	return &cp
}

// NodeMeta is called by memberlist to get the local node's metadata, bounded
// by memberlist's MetaMaxSize. The encoding is compressed and, if it still
// exceeds the limit, non-essential capabilities are dropped (with a warning)
// so memberlist always receives valid, decodable bytes — never a truncated
// blob. The result is cached and recomputed only when the limit or meta change.
func (d *delegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cached != nil && d.cachedLimit == limit {
		return d.cached
	}
	data, dropped, err := encodeMetaFitting(d.meta, limit)
	if err != nil {
		if d.log != nil {
			d.log.Error("cluster: failed to encode node metadata", "err", err)
		}
		return nil
	}
	if len(dropped) > 0 && d.log != nil {
		d.log.Warn("cluster: node metadata exceeds gossip limit; dropped capabilities from gossip",
			"dropped", dropped, "limit", limit)
	}
	d.cached = data
	d.cachedLimit = limit
	return data
}

// currentMeta returns a shallow copy of the local metadata, or nil if unset.
func (d *delegate) currentMeta() *NodeMeta {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.meta == nil {
		return nil
	}
	cp := *d.meta
	return &cp
}

// UpdateMeta replaces the local node metadata (e.g. on capability refresh).
// The metadata is deep-copied so the delegate never shares a mutable map with
// the caller (which may keep mutating it on later refresh ticks).
func (d *delegate) UpdateMeta(meta *NodeMeta) {
	cp := copyMeta(meta)
	d.mu.Lock()
	d.meta = cp
	d.cached = nil // invalidate; re-encoded lazily on next NodeMeta call
	d.mu.Unlock()
}

// NotifyMsg is called when a user-sent message is received.
// Phase 0b: no custom messages yet.
func (d *delegate) NotifyMsg([]byte) {}

// GetBroadcasts returns any queued broadcasts. Phase 0b: none.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState is sent on push-pull sync. Phase 0b: empty.
func (d *delegate) LocalState(join bool) []byte { return nil }

// MergeRemoteState handles remote state from push-pull. Phase 0b: no-op.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {}
