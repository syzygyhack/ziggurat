package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
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

// mockECPusher records EC shard push calls.
type mockECPusher struct {
	calls []ecPushCall
}

type ecPushCall struct {
	addr       string
	hash       string
	shardIndex int
	data       []byte
	ecMeta     []byte
}

func (m *mockECPusher) PushECShard(ctx context.Context, addr string, hash string, shardIndex int, data []byte, ecMeta []byte) error {
	m.calls = append(m.calls, ecPushCall{addr: addr, hash: hash, shardIndex: shardIndex, data: data, ecMeta: ecMeta})
	return nil
}

// mockRing returns a static list of node IDs.
type mockRing struct {
	nodes []string
}

func (m *mockRing) GetNodes(key string, n int) []string {
	if n > len(m.nodes) {
		return m.nodes
	}
	return m.nodes[:n]
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

func TestReplicator_ECFallbackWhenTooFewNodes(t *testing.T) {
	// EC object with RS(4,2) needs 6 nodes. Ring only has 3 — should fall
	// back to full-blob replication and push to peers.
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := DefaultTestConfig()
	cfg.Erasure.Enabled = true
	cfg.Erasure.DataShards = 4
	cfg.Erasure.ParityShards = 2
	cfg.TierThresholds.Large = 1 << 10 // 1 KB

	s, err := New(cfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	blobPusher := &mockPusher{}
	ecPusher := &mockECPusher{}
	peers := &mockPeers{peers: peersFromAddrs([]string{"peer1:7101"})}
	ring := &mockRing{nodes: []string{"local-node", "peer-1", "peer-2"}} // only 3 nodes, need 6

	r := NewReplicator(s, "local-node", 2, blobPusher, peers, log)
	r.SetECPusher(ecPusher)
	r.SetRing(ring)

	// Put a large object that triggers EC encoding.
	data := bytes.Repeat([]byte("a"), 2*1024) // 2 KB > 1 KB threshold
	hash, err := s.Put(context.Background(), "test/ec-fallback", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify EC params were created.
	meta, err := s.getMeta(hash)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Erasure == nil {
		t.Fatal("expected erasure params for large object")
	}

	err = r.AfterPut(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}

	// EC pusher should NOT be called (too few ring nodes).
	if len(ecPusher.calls) != 0 {
		t.Fatalf("expected 0 EC pushes (fallback to blob), got %d", len(ecPusher.calls))
	}

	// Full-blob pusher SHOULD be called (standard replication path).
	if len(blobPusher.calls) != 1 {
		t.Fatalf("expected 1 blob push (fallback), got %d", len(blobPusher.calls))
	}
}

func TestReplicator_ECDistributesWhenEnoughNodes(t *testing.T) {
	// EC object with RS(4,2). Ring has 6 nodes — should distribute shards.
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := DefaultTestConfig()
	cfg.Erasure.Enabled = true
	cfg.Erasure.DataShards = 4
	cfg.Erasure.ParityShards = 2
	cfg.TierThresholds.Large = 1 << 10

	s, err := New(cfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	blobPusher := &mockPusher{}
	ecPusher := &mockECPusher{}

	nodeIDs := []string{"local-node", "n1", "n2", "n3", "n4", "n5"}
	peerList := make([]Peer, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		peerList = append(peerList, Peer{NodeID: id, Addr: id + ":7101"})
	}
	peers := &mockPeers{peers: peerList}
	ring := &mockRing{nodes: nodeIDs}

	r := NewReplicator(s, "local-node", 2, blobPusher, peers, log)
	r.SetECPusher(ecPusher)
	r.SetRing(ring)

	data := bytes.Repeat([]byte("b"), 2*1024)
	hash, err := s.Put(context.Background(), "test/ec-distribute", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	err = r.AfterPut(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}

	// Full-blob pusher should NOT be called (EC path taken).
	if len(blobPusher.calls) != 0 {
		t.Fatalf("expected 0 blob pushes, got %d", len(blobPusher.calls))
	}

	// EC pusher should be called for shards mapped to non-local nodes.
	// Ring: [local-node, n1, n2, n3, n4, n5]. Shards 0→local-node (skip),
	// shards 1-5 → n1-n5 (push). So 5 pushes expected.
	if len(ecPusher.calls) != 5 {
		t.Fatalf("expected 5 EC pushes, got %d", len(ecPusher.calls))
	}

	// Verify each push includes EC metadata.
	for _, call := range ecPusher.calls {
		if len(call.ecMeta) == 0 {
			t.Fatalf("EC push for shard %d missing erasure metadata", call.shardIndex)
		}
	}

	// Verify origin's metadata records EC shard placements for all nodes.
	// local-node holds the full blob (index 0) plus shard 0 (local EC shard),
	// and n1-n5 hold shards 1-5.
	nodes := r.NodesForHash(hash)
	if len(nodes) < 6 {
		t.Fatalf("expected 6 nodes in placements (local + 5 remote EC shards), got %v", nodes)
	}

	// All shards distributed — should NOT be under-replicated.
	if r.UnderReplicatedCount() != 0 {
		t.Fatalf("expected 0 under-replicated after successful EC distribution, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_ECRepairClearsUnderRepMap(t *testing.T) {
	// EC object initially marked under-replicated (too few nodes), then
	// ring grows and repair succeeds — should be removed from underRepMap.
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := DefaultTestConfig()
	cfg.Erasure.Enabled = true
	cfg.Erasure.DataShards = 4
	cfg.Erasure.ParityShards = 2
	cfg.TierThresholds.Large = 1 << 10

	s, err := New(cfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	blobPusher := &mockPusher{}
	ecPusher := &mockECPusher{}

	// Start with only 3 nodes — not enough for RS(4,2)=6.
	// factor=3 with only 1 peer means blob fallback will be under-replicated.
	smallRing := &mockRing{nodes: []string{"local-node", "n1", "n2"}}
	peerList := []Peer{
		{NodeID: "n1", Addr: "n1:7101"},
	}
	peers := &mockPeers{peers: peerList}

	r := NewReplicator(s, "local-node", 3, blobPusher, peers, log)
	r.SetECPusher(ecPusher)
	r.SetRing(smallRing)

	data := bytes.Repeat([]byte("c"), 2*1024)
	hash, err := s.Put(context.Background(), "test/ec-repair", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// First AfterPut falls back to blob replication (too few ring nodes).
	r.AfterPut(context.Background(), hash)

	// Should not have enough placements — under-replicated via blob path.
	// (blobPusher pushed to available peers, but need factor=2 total)
	// Reset blob pusher for repair.
	blobPusher.calls = nil

	// Now grow the ring to 6 nodes.
	fullNodeIDs := []string{"local-node", "n1", "n2", "n3", "n4", "n5"}
	fullPeerList := make([]Peer, 0, len(fullNodeIDs))
	for _, id := range fullNodeIDs {
		fullPeerList = append(fullPeerList, Peer{NodeID: id, Addr: id + ":7101"})
	}
	r.SetRing(&mockRing{nodes: fullNodeIDs})
	r.peers = &mockPeers{peers: fullPeerList}

	// Run repair.
	repaired := r.Repair(context.Background())

	// EC distribution should succeed now; object should be cleared from underRepMap.
	if r.UnderReplicatedCount() != 0 {
		t.Fatalf("expected 0 under-replicated after EC repair, got %d", r.UnderReplicatedCount())
	}
	if repaired != 1 {
		t.Fatalf("expected 1 repaired, got %d", repaired)
	}

	// EC pusher should have been called (shards distributed).
	if len(ecPusher.calls) == 0 {
		t.Fatal("expected EC pushes after ring grew, got 0")
	}
}

func TestStore_PutECShardReplica(t *testing.T) {
	s := testStoreWithEC(t)

	hashHex := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	ecParams := &model.ErasureParams{
		DataShards:   4,
		ParityShards: 2,
		OriginalSize: 8192,
		ShardSize:    2048,
		ShardHashes:  []string{"aa", "bb", "cc", "dd", "ee", "ff"},
	}

	// Create metadata.
	if err := s.PutECShardReplica(hashHex, ecParams); err != nil {
		t.Fatal(err)
	}

	// Verify metadata was created.
	meta, err := s.getMeta(hashHex)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Strategy != model.ErasureCoded {
		t.Fatalf("expected ErasureCoded strategy, got %v", meta.Strategy)
	}
	if !meta.Pinned {
		t.Fatal("expected EC shard replica to be pinned")
	}
	if meta.Erasure == nil {
		t.Fatal("expected erasure params")
	}
	if meta.Erasure.DataShards != 4 {
		t.Fatalf("expected 4 data shards, got %d", meta.Erasure.DataShards)
	}
	// Size should reflect local shard footprint, not original object size.
	if meta.Size != 2048 {
		t.Fatalf("expected size 2048 (shard size), got %d", meta.Size)
	}

	// Idempotent: calling again should not error or change metadata.
	if err := s.PutECShardReplica(hashHex, ecParams); err != nil {
		t.Fatal(err)
	}
	stats := s.Stats()
	if stats.Objects != 1 {
		t.Fatalf("expected 1 object after duplicate PutECShardReplica, got %d", stats.Objects)
	}
}

func TestReplicator_RemoveNodePlacements(t *testing.T) {
	r, s, _ := testReplicator(t, 3, []string{"peer1:7101", "peer2:7101"})

	data := []byte("placement removal test")
	hash, err := s.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Record placements for local + 2 peers.
	r.AfterPut(context.Background(), hash)

	// Manually add peer placements since mock pusher doesn't wire through.
	if err := r.addPlacement(hash, "peer-1", 0); err != nil {
		t.Fatal(err)
	}
	if err := r.addPlacement(hash, "peer-2", 0); err != nil {
		t.Fatal(err)
	}

	nodes := r.NodesForHash(hash)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes before removal, got %v", nodes)
	}

	// Remove departed node.
	r.RemoveNodePlacements("peer-1")

	nodes = r.NodesForHash(hash)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after removal, got %v", nodes)
	}
	for _, n := range nodes {
		if n == "peer-1" {
			t.Fatal("departed node peer-1 should have been removed from placements")
		}
	}

	// Object should be under-replicated (have 2, want 3).
	if r.UnderReplicatedCount() != 1 {
		t.Fatalf("expected 1 under-replicated after node removal, got %d", r.UnderReplicatedCount())
	}
}

func TestReplicator_Rebalance_PushesToNewNode(t *testing.T) {
	r, s, pusher := testReplicator(t, 2, []string{"peer1:7101"})

	// Store an object and replicate it.
	data := []byte("rebalance me to new node")
	hash, err := s.Put(context.Background(), "test/obj1", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.AfterPut(context.Background(), hash)

	// Verify initial state: pushed to peer1, local + peer-1 placements.
	if len(pusher.calls) != 1 {
		t.Fatalf("expected 1 initial push, got %d", len(pusher.calls))
	}
	pusher.calls = nil // reset

	// Set up a ring where the new node is assigned this object.
	// Ring returns [local-node, new-node] — new-node should get a copy.
	r.SetRing(&mockRing{nodes: []string{"local-node", "new-node"}})
	newPeers := []Peer{
		{NodeID: "peer-1", Addr: "peer1:7101"},
		{NodeID: "new-node", Addr: "new-node:7101"},
	}
	r.peers = &mockPeers{peers: newPeers}

	// Rebalance for the new node.
	rebalanced := r.Rebalance(context.Background(), "new-node")

	if rebalanced != 1 {
		t.Fatalf("expected 1 rebalanced, got %d", rebalanced)
	}
	if len(pusher.calls) != 1 {
		t.Fatalf("expected 1 push to new node, got %d", len(pusher.calls))
	}
	if pusher.calls[0].addr != "new-node:7101" {
		t.Fatalf("expected push to new-node:7101, got %s", pusher.calls[0].addr)
	}
}

func TestReplicator_Rebalance_SkipsAlreadyPlaced(t *testing.T) {
	r, s, pusher := testReplicator(t, 2, []string{"peer1:7101"})

	data := []byte("already placed object")
	hash, err := s.Put(context.Background(), "test/obj1", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.AfterPut(context.Background(), hash)
	pusher.calls = nil

	// Manually add placement for new-node (as if it already has a copy).
	if err := r.addPlacement(hash, "new-node", 0); err != nil {
		t.Fatal(err)
	}

	r.SetRing(&mockRing{nodes: []string{"local-node", "new-node"}})
	r.peers = &mockPeers{peers: []Peer{
		{NodeID: "new-node", Addr: "new-node:7101"},
	}}

	rebalanced := r.Rebalance(context.Background(), "new-node")

	// Should skip — new-node already has the object.
	if rebalanced != 0 {
		t.Fatalf("expected 0 rebalanced (already placed), got %d", rebalanced)
	}
	if len(pusher.calls) != 0 {
		t.Fatalf("expected 0 pushes (already placed), got %d", len(pusher.calls))
	}
}

func TestReplicator_Rebalance_SkipsNonRingNode(t *testing.T) {
	r, s, pusher := testReplicator(t, 2, nil)

	data := []byte("not for new node")
	hash, err := s.Put(context.Background(), "test/obj1", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.AfterPut(context.Background(), hash)
	pusher.calls = nil

	// Ring does NOT include new-node — it shouldn't get a copy.
	r.SetRing(&mockRing{nodes: []string{"local-node", "other-node"}})
	r.peers = &mockPeers{peers: []Peer{
		{NodeID: "new-node", Addr: "new-node:7101"},
	}}

	rebalanced := r.Rebalance(context.Background(), "new-node")

	if rebalanced != 0 {
		t.Fatalf("expected 0 rebalanced (not in ring), got %d", rebalanced)
	}
	if len(pusher.calls) != 0 {
		t.Fatalf("expected 0 pushes (not in ring), got %d", len(pusher.calls))
	}
}

func TestStore_PutReplica_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s, err := New(DefaultTestConfig(), tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Write a blob first.
	data := []byte("idempotent replica test")
	hash, _, _, err := WriteBlob(s.dir, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	hashHex := fmt.Sprintf("%x", hash)

	// First PutReplica creates metadata with RefCount=1.
	if err := s.PutReplica(hashHex, hash, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	meta, err := s.getMeta(hashHex)
	if err != nil {
		t.Fatal(err)
	}
	if meta.RefCount != 1 {
		t.Fatalf("expected RefCount=1 after first PutReplica, got %d", meta.RefCount)
	}

	// Second PutReplica (retry) should NOT increment refcount.
	if err := s.PutReplica(hashHex, hash, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	meta, err = s.getMeta(hashHex)
	if err != nil {
		t.Fatal(err)
	}
	if meta.RefCount != 1 {
		t.Fatalf("expected RefCount=1 after idempotent retry, got %d", meta.RefCount)
	}

	// Retire the replica (RefCount -> 0).
	if err := s.RetireReplica(hashHex); err != nil {
		t.Fatal(err)
	}
	meta, err = s.getMeta(hashHex)
	if err != nil {
		t.Fatal(err)
	}
	if meta.RefCount != 0 {
		t.Fatalf("expected RefCount=0 after retire, got %d", meta.RefCount)
	}

	// PutReplica on retired object should re-activate it.
	if err := s.PutReplica(hashHex, hash, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	meta, err = s.getMeta(hashHex)
	if err != nil {
		t.Fatal(err)
	}
	if meta.RefCount != 1 {
		t.Fatalf("expected RefCount=1 after re-activation, got %d", meta.RefCount)
	}
	if !meta.UnreferencedAt.IsZero() {
		t.Fatal("expected UnreferencedAt to be cleared after re-activation")
	}
}
