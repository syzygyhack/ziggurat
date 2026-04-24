package cluster

import (
	"sync"
)

// delegate implements memberlist.Delegate. It provides local node metadata
// to the gossip protocol and receives broadcast messages.
type delegate struct {
	mu   sync.RWMutex
	meta []byte
}

func newDelegate(meta *NodeMeta) (*delegate, error) {
	data, err := encodeMeta(meta)
	if err != nil {
		return nil, err
	}
	return &delegate{meta: data}, nil
}

// NodeMeta is called by memberlist to get the local node's metadata.
func (d *delegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.meta) > limit {
		return d.meta[:limit]
	}
	return d.meta
}

// UpdateMeta replaces the local node metadata (e.g. on capability refresh).
func (d *delegate) UpdateMeta(meta *NodeMeta) {
	data, err := encodeMeta(meta)
	if err != nil {
		return
	}
	d.mu.Lock()
	d.meta = data
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
