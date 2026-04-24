package cluster

import (
	"github.com/hashicorp/memberlist"
)

// eventDelegate implements memberlist.EventDelegate, forwarding join/leave
// events to the node registry.
type eventDelegate struct {
	registry *Registry
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	e.registry.Add(n)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	e.registry.Remove(n)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	e.registry.Update(n)
}
