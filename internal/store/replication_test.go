package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
)

// mockPusher records push calls for verification.
type mockPusher struct {
	calls []pushCall
}

type pushCall struct {
	addr string
	hash string
	size int64
	data []byte
}

func (m *mockPusher) PushShard(ctx context.Context, addr string, hash string, size int64, r io.Reader) error {
	data, _ := io.ReadAll(r)
	m.calls = append(m.calls, pushCall{addr: addr, hash: hash, size: size, data: data})
	return nil
}

// mockPeers returns static peers.
type mockPeers struct {
	peers []Peer
}

func (m *mockPeers) Peers(exclude string) []Peer {
	return m.peers
}

func peersFromAddrs(addrs []string) []Peer {
	var peers []Peer
	for i, addr := range addrs {
		peers = append(peers, Peer{
			NodeID: fmt.Sprintf("peer-%d", i+1),
			Addr:   addr,
		})
	}
	return peers
}

func testReplicator(t *testing.T, factor int, peerAddrs []string) (*Replicator, *Store, *mockPusher) {
	t.Helper()
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := DefaultTestConfig()
	s, err := New(storeCfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	pusher := &mockPusher{}
	peers := &mockPeers{peers: peersFromAddrs(peerAddrs)}
	r := NewReplicator(s, "local-node", factor, pusher, peers, log)

	return r, s, pusher
}

func TestReplicator_SingleNode_NoReplication(t *testing.T) {
	r, s, pusher := testReplicator(t, 1, nil)

	data := []byte("test data for replication")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	err = r.AfterPut(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}

	// With factor=1 and no peers, no pushes should happen.
	if len(pusher.calls) != 0 {
		t.Fatalf("expected 0 push calls, got %d", len(pusher.calls))
	}

	// Local placement should be recorded.
	nodes := r.NodesForHash(hash)
	if len(nodes) != 1 || nodes[0] != "local-node" {
		t.Fatalf("expected [local-node], got %v", nodes)
	}

	if r.UnderReplicatedCount() != 0 {
		t.Fatalf("expected 0 under-replicated, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_PushesToPeer(t *testing.T) {
	r, s, pusher := testReplicator(t, 2, []string{"peer1:7101"})

	data := []byte("replicate me")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	err = r.AfterPut(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}

	if len(pusher.calls) != 1 {
		t.Fatalf("expected 1 push, got %d", len(pusher.calls))
	}

	call := pusher.calls[0]
	if call.addr != "peer1:7101" {
		t.Fatalf("expected peer1:7101, got %s", call.addr)
	}
	if call.hash != hash {
		t.Fatalf("hash mismatch: got %s, want %s", call.hash, hash)
	}
	if !bytes.Equal(call.data, data) {
		t.Fatalf("data mismatch")
	}

	// Should not be under-replicated since we successfully pushed.
	if r.UnderReplicatedCount() != 0 {
		t.Fatalf("expected 0 under-replicated, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_DegradedMode_NoPeers(t *testing.T) {
	r, s, _ := testReplicator(t, 3, nil)

	data := []byte("need 3 replicas but no peers")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	err = r.AfterPut(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}

	// Should be marked under-replicated (have 1, want 3).
	if r.UnderReplicatedCount() != 1 {
		t.Fatalf("expected 1 under-replicated, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_DegradedMode_InsufficientPeers(t *testing.T) {
	r, s, pusher := testReplicator(t, 3, []string{"peer1:7101"})

	data := []byte("need 3 replicas but only 1 peer")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	err = r.AfterPut(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}

	// Pushed to 1 peer, but need 2 more replicas (total 3, have 2).
	if len(pusher.calls) != 1 {
		t.Fatalf("expected 1 push, got %d", len(pusher.calls))
	}
	if r.UnderReplicatedCount() != 1 {
		t.Fatalf("expected 1 under-replicated, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_Repair(t *testing.T) {
	r, s, pusher := testReplicator(t, 2, nil) // start with no peers

	data := []byte("repair me later")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	r.AfterPut(context.Background(), hash)

	// Under-replicated because no peers.
	if r.UnderReplicatedCount() != 1 {
		t.Fatalf("expected 1 under-replicated, got %d", r.UnderReplicatedCount())
	}

	// Now add a peer and run repair.
	r.peers = &mockPeers{peers: peersFromAddrs([]string{"new-peer:7101"})}
	repaired := r.Repair(context.Background())

	if repaired != 1 {
		t.Fatalf("expected 1 repaired, got %d", repaired)
	}
	if len(pusher.calls) != 1 {
		t.Fatalf("expected 1 push from repair, got %d", len(pusher.calls))
	}
	if r.UnderReplicatedCount() != 0 {
		t.Fatalf("expected 0 under-replicated after repair, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_NodesForHash(t *testing.T) {
	r, s, _ := testReplicator(t, 1, nil)

	data := []byte("test data")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Before AfterPut, no placements.
	nodes := r.NodesForHash(hash)
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes before placement, got %v", nodes)
	}

	r.AfterPut(context.Background(), hash)

	nodes = r.NodesForHash(hash)
	if len(nodes) != 1 || nodes[0] != "local-node" {
		t.Fatalf("expected [local-node], got %v", nodes)
	}
}

func TestReplicator_DuplicatePlacement(t *testing.T) {
	r, s, _ := testReplicator(t, 1, nil)

	data := []byte("dedup test")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Call AfterPut twice — should not duplicate placement.
	r.AfterPut(context.Background(), hash)
	r.AfterPut(context.Background(), hash)

	nodes := r.NodesForHash(hash)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node (no duplicate), got %v", nodes)
	}
}
