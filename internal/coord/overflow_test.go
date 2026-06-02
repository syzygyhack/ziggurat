package coord

import (
	"context"
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// hybridDispatcherWithRemote configures the test dispatcher as a hybrid node
// (local worker + dispatch) with one remote worker registered.
func hybridDispatcherWithRemote(t *testing.T) (*Dispatcher, *Coordinator, *mockTransport, string) {
	t.Helper()
	d, c, tr := testDispatcher(t)
	d.dispatchAll = false // hybrid: local worker runs work; overflow goes remote
	d.localID = "local-node"
	const remote = "remote-node"
	d.registry = &mockNodeRegistry{nodes: []*model.Node{
		{ID: "local-node", Role: model.RoleHybrid, GRPCAddress: "local:7101", Capabilities: map[string]string{"cpu.cores": "4"}},
		{ID: remote, Role: model.RoleHybrid, GRPCAddress: "remote:7101", Capabilities: map[string]string{"cpu.cores": "8"}},
	}}
	return d, c, tr, remote
}

func enqueue(t *testing.T, c *Coordinator, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := c.Submit(context.Background(), &model.Task{Command: []string{"sleep", "1"}}); err != nil {
			t.Fatal(err)
		}
	}
}

// When the local worker is saturated and a peer has capacity, queued
// locally-runnable tasks are offloaded to the peer.
func TestDispatch_OffloadsOverflowWhenLocalSaturated(t *testing.T) {
	d, c, tr, remote := hybridDispatcherWithRemote(t)

	// Local worker saturated (2 running / limit 2); remote idle with capacity.
	c.workerLoad.SetLimit("local-node", 2)
	c.workerLoad.TaskStarted("local-node")
	c.workerLoad.TaskStarted("local-node")
	c.workerLoad.SetLimit(remote, 8)

	enqueue(t, c, 5)
	d.dispatchBatch(context.Background())

	if len(tr.dispatched) != 5 {
		t.Fatalf("expected 5 overflow tasks dispatched to peer, got %d", len(tr.dispatched))
	}
	if c.QueueLen() != 0 {
		t.Errorf("queue should be drained via offload, len=%d", c.QueueLen())
	}
}

// While the local worker has free slots, nothing is offloaded — work stays
// local for locality.
func TestDispatch_NoOffloadWhenLocalHasCapacity(t *testing.T) {
	d, c, tr, remote := hybridDispatcherWithRemote(t)

	c.workerLoad.SetLimit("local-node", 8) // 0 running / limit 8 → not saturated
	c.workerLoad.SetLimit(remote, 8)

	enqueue(t, c, 5)
	d.dispatchBatch(context.Background())

	if len(tr.dispatched) != 0 {
		t.Errorf("no offload expected while local has capacity, got %d dispatched", len(tr.dispatched))
	}
	if c.QueueLen() != 5 {
		t.Errorf("tasks should remain queued for the local worker, len=%d", c.QueueLen())
	}
}

// When the local worker is saturated but no peer has spare capacity, work is
// not offloaded (it would just move the queue).
func TestDispatch_NoOffloadWhenPeerFull(t *testing.T) {
	d, c, tr, remote := hybridDispatcherWithRemote(t)

	c.workerLoad.SetLimit("local-node", 2)
	c.workerLoad.TaskStarted("local-node")
	c.workerLoad.TaskStarted("local-node")
	// Remote also saturated.
	c.workerLoad.SetLimit(remote, 2)
	c.workerLoad.TaskStarted(remote)
	c.workerLoad.TaskStarted(remote)

	enqueue(t, c, 5)
	d.dispatchBatch(context.Background())

	if len(tr.dispatched) != 0 {
		t.Errorf("no offload expected when peer is full, got %d dispatched", len(tr.dispatched))
	}
	if c.QueueLen() != 5 {
		t.Errorf("tasks should remain queued, len=%d", c.QueueLen())
	}
}

// Offload respects the peer's remaining capacity: only as many tasks as the
// peer can accept are dispatched; the rest stay queued for the local worker.
func TestDispatch_OffloadBoundedByPeerCapacity(t *testing.T) {
	d, c, tr, remote := hybridDispatcherWithRemote(t)

	c.workerLoad.SetLimit("local-node", 2)
	c.workerLoad.TaskStarted("local-node")
	c.workerLoad.TaskStarted("local-node")
	// Remote has 3 free slots (limit 3, 0 running).
	c.workerLoad.SetLimit(remote, 3)

	enqueue(t, c, 10)
	d.dispatchBatch(context.Background())

	if len(tr.dispatched) != 3 {
		t.Fatalf("expected offload bounded to peer capacity (3), got %d", len(tr.dispatched))
	}
	if c.QueueLen() != 7 {
		t.Errorf("remaining 7 tasks should stay queued for local worker, len=%d", c.QueueLen())
	}
}
