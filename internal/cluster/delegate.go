package cluster

import (
	"log/slog"
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
	return &delegate{meta: meta, log: log}, nil
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
func (d *delegate) UpdateMeta(meta *NodeMeta) {
	d.mu.Lock()
	d.meta = meta
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
