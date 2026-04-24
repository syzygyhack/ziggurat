package cluster

import (
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCluster_SingleNode(t *testing.T) {
	log := testLogger()

	c, err := New(Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 0, // auto-assign
		HTTPPort: 7100,
		Tags:     []string{"gpu"},
		Caps:     map[string]string{"os": "linux"},
		Role:     "hybrid",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()

	if c.NumMembers() != 1 {
		t.Fatalf("expected 1 member, got %d", c.NumMembers())
	}

	// Registry should have the local node.
	nodes := c.Registry.List()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node in registry, got %d", len(nodes))
	}
	if nodes[0].ID != "node-1" {
		t.Fatalf("expected node-1, got %s", nodes[0].ID)
	}
	if nodes[0].Capabilities["os"] != "linux" {
		t.Fatalf("expected os=linux cap, got %v", nodes[0].Capabilities)
	}
}

func TestCluster_TwoNodesJoin(t *testing.T) {
	log := testLogger()

	// Start first node.
	c1, err := New(Config{
		NodeID:   "node-1",
		NodeName: "first",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7100,
		Tags:     []string{"gpu"},
		Caps:     map[string]string{"os": "linux", "gpu.vram": "16000000000"},
		Role:     "hybrid",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Shutdown()

	// Start second node, joining the first.
	c2, err := New(Config{
		NodeID:   "node-2",
		NodeName: "second",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7200,
		Seeds:    []string{c1.LocalAddr()},
		Tags:     []string{"cpu"},
		Caps:     map[string]string{"os": "linux"},
		Role:     "worker",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown()

	// Wait for gossip convergence.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c1.NumMembers() == 2 && c2.NumMembers() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if c1.NumMembers() != 2 {
		t.Fatalf("node-1 sees %d members, want 2", c1.NumMembers())
	}
	if c2.NumMembers() != 2 {
		t.Fatalf("node-2 sees %d members, want 2", c2.NumMembers())
	}

	// Both registries should have both nodes.
	nodes1 := c1.Registry.List()
	if len(nodes1) != 2 {
		t.Fatalf("node-1 registry has %d nodes, want 2", len(nodes1))
	}
	nodes2 := c2.Registry.List()
	if len(nodes2) != 2 {
		t.Fatalf("node-2 registry has %d nodes, want 2", len(nodes2))
	}

	// Verify node-2 sees node-1's capabilities.
	n1, ok := c2.Registry.Get("node-1")
	if !ok {
		t.Fatal("node-2 doesn't know about node-1")
	}
	if n1.Capabilities["gpu.vram"] != "16000000000" {
		t.Fatalf("node-1 gpu.vram not propagated: %v", n1.Capabilities)
	}
}

func TestCluster_NodeLeave(t *testing.T) {
	log := testLogger()

	c1, err := New(Config{
		NodeID:   "node-1",
		NodeName: "first",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7100,
		Role:     "hybrid",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Shutdown()

	c2, err := New(Config{
		NodeID:   "node-2",
		NodeName: "second",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7200,
		Seeds:    []string{c1.LocalAddr()},
		Role:     "worker",
	}, log)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for convergence.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c1.NumMembers() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Node-2 leaves gracefully.
	c2.Leave()
	c2.Shutdown()

	// Wait for node-1 to see the departure.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c1.NumMembers() == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if c1.NumMembers() != 1 {
		t.Fatalf("after leave, node-1 sees %d members, want 1", c1.NumMembers())
	}

	nodes := c1.Registry.List()
	if len(nodes) != 1 {
		t.Fatalf("after leave, registry has %d nodes, want 1", len(nodes))
	}
	if nodes[0].ID != "node-1" {
		t.Fatalf("expected remaining node to be node-1, got %s", nodes[0].ID)
	}
}

func TestCluster_OnLeaveCallback(t *testing.T) {
	log := testLogger()

	c1, err := New(Config{
		NodeID:   "node-1",
		NodeName: "first",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7100,
		Role:     "hybrid",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Shutdown()

	// Register OnLeave callback on node-1's registry.
	var departedID atomic.Value
	c1.Registry.OnLeave(func(nodeID string) {
		departedID.Store(nodeID)
	})

	c2, err := New(Config{
		NodeID:   "node-2",
		NodeName: "second",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7200,
		Seeds:    []string{c1.LocalAddr()},
		Role:     "worker",
	}, log)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for convergence.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c1.NumMembers() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if c1.NumMembers() != 2 {
		t.Fatalf("expected 2 members, got %d", c1.NumMembers())
	}

	// Node-2 leaves.
	c2.Leave()
	c2.Shutdown()

	// Wait for callback to fire.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v := departedID.Load(); v != nil {
			if v.(string) == "node-2" {
				return // success
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("OnLeave callback was not invoked with node-2")
}

func TestCluster_UpdateMeta(t *testing.T) {
	log := testLogger()

	c1, err := New(Config{
		NodeID:   "node-1",
		NodeName: "first",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7100,
		Caps:     map[string]string{"os": "linux"},
		Role:     "hybrid",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Shutdown()

	c2, err := New(Config{
		NodeID:   "node-2",
		NodeName: "second",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		HTTPPort: 7200,
		Seeds:    []string{c1.LocalAddr()},
		Role:     "worker",
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown()

	// Wait for convergence.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c1.NumMembers() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Update node-1's capabilities.
	c1.UpdateMeta(map[string]string{"os": "linux", "gpu.count": "4"}, []string{"gpu"})

	// Wait for the update to propagate to node-2's registry.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n1, ok := c2.Registry.Get("node-1")
		if ok && n1.Capabilities["gpu.count"] == "4" {
			return // success
		}
		time.Sleep(100 * time.Millisecond)
	}

	n1, _ := c2.Registry.Get("node-1")
	t.Fatalf("meta update not propagated: caps=%v", n1.Capabilities)
}
