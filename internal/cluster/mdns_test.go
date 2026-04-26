package cluster

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestMDNS_AdvertiseAndDiscover(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Advertise a node on a random port.
	srv, err := NewMDNSServer(MDNSConfig{
		NodeID:      "test-node-1",
		GossipPort:  17102,
		ClusterName: "test-cluster",
	}, log)
	if err != nil {
		t.Fatalf("failed to start mDNS server: %v", err)
	}
	defer srv.Shutdown()

	// Give mDNS server a moment to register.
	time.Sleep(100 * time.Millisecond)

	// Discover peers.
	peers, err := DiscoverPeers(MDNSDiscoverConfig{
		ClusterName: "test-cluster",
		Timeout:     2 * time.Second,
	}, log)
	if err != nil {
		t.Fatalf("discovery failed: %v", err)
	}

	if len(peers) == 0 {
		t.Fatal("expected to discover at least one peer")
	}

	// Verify the discovered peer has the right port.
	found := false
	for _, p := range peers {
		if p.Port == 17102 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected peer with port 17102, got %v", peers)
	}
}

func TestMDNS_ClusterNameFilter(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Advertise on cluster "alpha".
	srv, err := NewMDNSServer(MDNSConfig{
		NodeID:      "node-alpha",
		GossipPort:  17103,
		ClusterName: "alpha",
	}, log)
	if err != nil {
		t.Fatalf("failed to start mDNS server: %v", err)
	}
	defer srv.Shutdown()

	time.Sleep(100 * time.Millisecond)

	// Discover on cluster "beta" — should find nothing.
	peers, err := DiscoverPeers(MDNSDiscoverConfig{
		ClusterName: "beta",
		Timeout:     1 * time.Second,
	}, log)
	if err != nil {
		t.Fatalf("discovery failed: %v", err)
	}

	if len(peers) != 0 {
		t.Fatalf("expected 0 peers for cluster beta, got %d: %v", len(peers), peers)
	}
}

func TestMDNS_DefaultClusterName(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Advertise with default cluster name.
	srv, err := NewMDNSServer(MDNSConfig{
		NodeID:      "node-default",
		GossipPort:  17104,
		ClusterName: "", // should default to "default"
	}, log)
	if err != nil {
		t.Fatalf("failed to start mDNS server: %v", err)
	}
	defer srv.Shutdown()

	time.Sleep(100 * time.Millisecond)

	// Discover with default cluster name.
	peers, err := DiscoverPeers(MDNSDiscoverConfig{
		ClusterName: "",
		Timeout:     2 * time.Second,
	}, log)
	if err != nil {
		t.Fatalf("discovery failed: %v", err)
	}

	if len(peers) == 0 {
		t.Fatal("expected to discover at least one peer with default cluster name")
	}
}

func TestMDNSServer_Shutdown(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	srv, err := NewMDNSServer(MDNSConfig{
		NodeID:      "node-shutdown",
		GossipPort:  17105,
		ClusterName: "default",
	}, log)
	if err != nil {
		t.Fatalf("failed to start mDNS server: %v", err)
	}

	// Shutdown should not panic or error.
	srv.Shutdown()

	// Double shutdown should be safe.
	srv.Shutdown()
}
