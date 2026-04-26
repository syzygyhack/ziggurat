package cluster

import (
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
)

const (
	mdnsService = "_ziggurat._tcp"
	mdnsDomain  = "local."
)

// MDNSConfig configures the mDNS advertiser.
type MDNSConfig struct {
	NodeID      string
	GossipPort  int
	ClusterName string // empty defaults to "default"
}

// MDNSDiscoverConfig configures mDNS peer discovery.
type MDNSDiscoverConfig struct {
	ClusterName string        // filter to this cluster; empty defaults to "default"
	Timeout     time.Duration // how long to listen for responses
}

// DiscoveredPeer is a gossip endpoint found via mDNS.
type DiscoveredPeer struct {
	Host string
	Port int
}

// String returns the peer as host:port suitable for memberlist.Join.
func (p DiscoveredPeer) String() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

// MDNSServer advertises this node's gossip address via mDNS.
type MDNSServer struct {
	server *mdns.Server
	mu     sync.Mutex
	closed bool
}

// NewMDNSServer starts advertising the node via mDNS. The service record
// includes the gossip port in the SRV record and the cluster name in TXT
// so that nodes on different logical clusters can coexist on the same LAN.
func NewMDNSServer(cfg MDNSConfig, log *slog.Logger) (*MDNSServer, error) {
	clusterName := cfg.ClusterName
	if clusterName == "" {
		clusterName = "default"
	}

	// The instance name includes the node ID for uniqueness.
	instance := fmt.Sprintf("ziggurat-%s", cfg.NodeID)

	// TXT record carries cluster name for filtering during discovery.
	txt := []string{fmt.Sprintf("cluster=%s", clusterName)}

	service, err := mdns.NewMDNSService(
		instance,
		mdnsService,
		mdnsDomain,
		"",            // host — empty = auto-detect
		cfg.GossipPort,
		nil, // IPs — nil = all interfaces
		txt,
	)
	if err != nil {
		return nil, fmt.Errorf("create mDNS service: %w", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return nil, fmt.Errorf("start mDNS server: %w", err)
	}

	log.Info("mDNS: advertising", "service", mdnsService, "port", cfg.GossipPort, "cluster", clusterName)
	return &MDNSServer{server: server}, nil
}

// Shutdown stops the mDNS advertisement. Safe to call multiple times.
func (s *MDNSServer) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.server.Shutdown()
}

// DiscoverPeers queries the LAN for Ziggurat nodes via mDNS and returns
// their gossip addresses. Only peers matching the cluster name are returned.
func DiscoverPeers(cfg MDNSDiscoverConfig, log *slog.Logger) ([]DiscoveredPeer, error) {
	clusterName := cfg.ClusterName
	if clusterName == "" {
		clusterName = "default"
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}

	entries := make(chan *mdns.ServiceEntry, 16)

	var mu sync.Mutex
	var peers []DiscoveredPeer

	go func() {
		for entry := range entries {
			// Filter by cluster name in TXT records.
			if !matchesCluster(entry.InfoFields, clusterName) {
				continue
			}

			host := entry.AddrV4.String()
			if entry.AddrV4 == nil || entry.AddrV4.IsUnspecified() {
				if entry.AddrV6 != nil && !entry.AddrV6.IsUnspecified() {
					host = entry.AddrV6.String()
				} else if entry.Host != "" {
					host = strings.TrimSuffix(entry.Host, ".")
				} else {
					continue
				}
			}

			// Skip loopback — we don't want to "discover" ourselves in tests
			// unless there's truly no other interface.
			peer := DiscoveredPeer{Host: host, Port: entry.Port}

			mu.Lock()
			peers = append(peers, peer)
			mu.Unlock()

			log.Debug("mDNS: discovered peer", "host", host, "port", entry.Port)
		}
	}()

	params := &mdns.QueryParam{
		Service:             mdnsService,
		Domain:              mdnsDomain,
		Timeout:             timeout,
		Entries:             entries,
		WantUnicastResponse: false,
	}

	if err := mdns.Query(params); err != nil {
		// Some systems don't support multicast; this is non-fatal.
		log.Debug("mDNS: query failed", "err", err)
		return nil, nil
	}

	mu.Lock()
	result := peers
	mu.Unlock()

	if len(result) > 0 {
		log.Info("mDNS: discovered peers", "count", len(result))
	}

	return result, nil
}

// DiscoverAddrs is a convenience wrapper that returns gossip addresses as
// strings suitable for passing to memberlist.Join or cluster.Config.Seeds.
func DiscoverAddrs(cfg MDNSDiscoverConfig, log *slog.Logger) ([]string, error) {
	peers, err := DiscoverPeers(cfg, log)
	if err != nil {
		return nil, err
	}
	addrs := make([]string, len(peers))
	for i, p := range peers {
		addrs[i] = net.JoinHostPort(p.Host, fmt.Sprintf("%d", p.Port))
	}
	return addrs, nil
}

// matchesCluster checks if the TXT record info fields contain the expected
// cluster name. Fields are in "key=value" format.
func matchesCluster(fields []string, want string) bool {
	for _, f := range fields {
		if f == "cluster="+want {
			return true
		}
	}
	return false
}
