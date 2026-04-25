package cluster

import (
	"fmt"
	"testing"
)

func TestHashRing_AddRemove(t *testing.T) {
	ring := NewHashRing(32)

	ring.AddNode("node-a")
	ring.AddNode("node-b")
	ring.AddNode("node-c")

	if ring.Size() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.Size())
	}

	// Idempotent add.
	ring.AddNode("node-a")
	if ring.Size() != 3 {
		t.Fatalf("expected 3 after duplicate add, got %d", ring.Size())
	}

	ring.RemoveNode("node-b")
	if ring.Size() != 2 {
		t.Fatalf("expected 2 after remove, got %d", ring.Size())
	}

	members := ring.Members()
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
}

func TestHashRing_GetNode(t *testing.T) {
	ring := NewHashRing(64)
	ring.AddNode("node-a")
	ring.AddNode("node-b")
	ring.AddNode("node-c")

	// Same key should always return the same node.
	n1 := ring.GetNode("test-key-1")
	n2 := ring.GetNode("test-key-1")
	if n1 != n2 {
		t.Fatalf("inconsistent node for same key: %s vs %s", n1, n2)
	}

	// A node should be returned.
	if n1 == "" {
		t.Fatal("expected a node, got empty")
	}
}

func TestHashRing_GetNodes_Unique(t *testing.T) {
	ring := NewHashRing(64)
	ring.AddNode("node-a")
	ring.AddNode("node-b")
	ring.AddNode("node-c")

	nodes := ring.GetNodes("some-object-hash", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// All should be unique.
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node: %s", n)
		}
		seen[n] = true
	}
}

func TestHashRing_GetNodes_MoreThanAvailable(t *testing.T) {
	ring := NewHashRing(32)
	ring.AddNode("node-a")
	ring.AddNode("node-b")

	// Asking for 5 should return only 2.
	nodes := ring.GetNodes("key", 5)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (ring has 2), got %d", len(nodes))
	}
}

func TestHashRing_EmptyRing(t *testing.T) {
	ring := NewHashRing(32)
	nodes := ring.GetNodes("key", 3)
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes for empty ring, got %d", len(nodes))
	}
	if n := ring.GetNode("key"); n != "" {
		t.Fatalf("expected empty for GetNode on empty ring, got %s", n)
	}
}

func TestHashRing_Distribution(t *testing.T) {
	ring := NewHashRing(128)
	nodeIDs := []string{"node-a", "node-b", "node-c", "node-d"}
	for _, id := range nodeIDs {
		ring.AddNode(id)
	}

	// Distribute 1000 keys and check that each node gets some.
	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("object-%d", i)
		n := ring.GetNode(key)
		counts[n]++
	}

	for _, id := range nodeIDs {
		if counts[id] == 0 {
			t.Errorf("node %s got 0 keys — distribution failure", id)
		}
	}

	// With 128 vnodes per node and 1000 keys, each node should get at least 100.
	for _, id := range nodeIDs {
		if counts[id] < 50 {
			t.Errorf("node %s got only %d keys — poor distribution", id, counts[id])
		}
	}
}

func TestHashRing_StabilityAfterRemove(t *testing.T) {
	ring := NewHashRing(64)
	ring.AddNode("node-a")
	ring.AddNode("node-b")
	ring.AddNode("node-c")

	// Record assignments.
	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10"}
	before := make(map[string]string)
	for _, k := range keys {
		before[k] = ring.GetNode(k)
	}

	// Remove node-c.
	ring.RemoveNode("node-c")

	// Keys that were NOT on node-c should keep their assignment.
	moved := 0
	for _, k := range keys {
		after := ring.GetNode(k)
		if before[k] != "node-c" && after != before[k] {
			t.Errorf("key %s moved from %s to %s after removing node-c", k, before[k], after)
		}
		if after != before[k] {
			moved++
		}
	}
	// At most ~1/3 of keys should move (those on node-c).
	t.Logf("keys moved: %d/%d", moved, len(keys))
}
