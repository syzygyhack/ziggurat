package cluster

import (
	"fmt"

	"github.com/hashicorp/memberlist"
)

// eventDelegate implements memberlist.EventDelegate and AliveDelegate.
// It enforces cluster name and join token validation: nodes with a
// mismatched cluster name or invalid join token are rejected at the
// gossip level (AliveDelegate returns an error, causing memberlist to
// immediately suspect the node) and are never added to the registry.
type eventDelegate struct {
	registry    *Registry
	clusterName string
	joinToken   string // local join token for HMAC validation
}

func (e *eventDelegate) NotifyAlive(n *memberlist.Node) error {
	meta, err := decodeMeta(n.Meta)
	if err != nil {
		// Can't decode metadata — refuse the node.
		return fmt.Errorf("undecodable metadata from %s", n.Name)
	}

	// Validate cluster name.
	if meta.ClusterName != "" && meta.ClusterName != e.clusterName {
		return fmt.Errorf("cluster name mismatch: %s (local: %s)", meta.ClusterName, e.clusterName)
	}

	// Validate join token HMAC.
	if !validateJoinHMAC(n.Name, meta.TokenHMAC, e.joinToken) {
		return fmt.Errorf("join token mismatch for %s", n.Name)
	}

	return nil
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	// Only add to registry if metadata is valid and cluster/token match.
	// NotifyAlive already handles rejection at the gossip level, but this
	// is defense-in-depth for nodes that slip past (e.g., during initial
	// cluster formation before AliveDelegate is fully effective).
	meta, err := decodeMeta(n.Meta)
	if err != nil {
		return
	}
	if meta.ClusterName != "" && meta.ClusterName != e.clusterName {
		return
	}
	if !validateJoinHMAC(n.Name, meta.TokenHMAC, e.joinToken) {
		return
	}
	e.registry.Add(n)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	e.registry.Remove(n)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	e.registry.Update(n)
}
