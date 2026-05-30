package cluster

import (
	"github.com/hashicorp/memberlist"
)

// eventDelegate implements memberlist.EventDelegate, forwarding join/leave
// events to the node registry. It also enforces cluster name matching:
// nodes with a mismatched cluster name are rejected (not added to the
// registry, effectively ignoring them).
type eventDelegate struct {
	registry    *Registry
	clusterName string
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	// Validate cluster name — nodes from different clusters must not join.
	if meta, err := decodeMeta(n.Meta); err == nil {
		if meta.ClusterName != "" && meta.ClusterName != e.clusterName {
			return // silently reject foreign-cluster nodes
		}
	}
	e.registry.Add(n)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	e.registry.Remove(n)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	e.registry.Update(n)
}
