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
	AdvertiseIP string // LAN IP to advertise and pin multicast to (optional)
}

// MDNSDiscoverConfig configures mDNS peer discovery.
type MDNSDiscoverConfig struct {
	ClusterName string        // filter to this cluster; empty defaults to "default"
	Timeout     time.Duration // how long to listen for responses
	AdvertiseIP string        // pin multicast queries to this IP's interface (optional)
}

// ifaceName returns the interface name for logging, or "default" if nil.
func ifaceName(i *net.Interface) string {
	if i == nil {
		return "default"
	}
	return i.Name
}

// interfaceForIP returns the network interface that owns the given IP, or nil
// if none matches. Used to pin mDNS multicast to the LAN interface on
// multi-homed hosts (so it isn't bound to a virtual adapter).
func interfaceForIP(ipStr string) *net.Interface {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for i := range ifaces {
		addrs, err := ifaces[i].Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var aip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				aip = v.IP
			case *net.IPAddr:
				aip = v.IP
			}
			if aip != nil && aip.Equal(ip) {
				return &ifaces[i]
			}
		}
	}
	return nil
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

	// Advertise only the LAN IP (not every interface's address) and pin the
	// multicast listener to that interface, so peers on other machines receive
	// a reachable address and our responses go out the LAN interface.
	var ips []net.IP
	var iface *net.Interface
	if cfg.AdvertiseIP != "" {
		if ip := net.ParseIP(cfg.AdvertiseIP); ip != nil {
			ips = []net.IP{ip}
		}
		iface = interfaceForIP(cfg.AdvertiseIP)
	}

	service, err := mdns.NewMDNSService(
		instance,
		mdnsService,
		mdnsDomain,
		"", // host — empty = auto-detect
		cfg.GossipPort,
		ips, // nil = all interfaces; set = advertise only the LAN IP
		txt,
	)
	if err != nil {
		return nil, fmt.Errorf("create mDNS service: %w", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service, Iface: iface})
	if err != nil {
		return nil, fmt.Errorf("start mDNS server: %w", err)
	}

	log.Info("mDNS: advertising", "service", mdnsService, "port", cfg.GossipPort, "cluster", clusterName, "iface", ifaceName(iface))
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

	// Drain the channel into a slice while Query runs. A goroutine is
	// needed so that more than 16 entries don't deadlock the library.
	// We do NOT read ServiceEntry fields here — the mdns library may
	// concurrently mutate them (it keeps entries in an inprogress map).
	var received []*mdns.ServiceEntry
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for entry := range entries {
			received = append(received, entry)
		}
	}()

	params := &mdns.QueryParam{
		Service:             mdnsService,
		Domain:              mdnsDomain,
		Timeout:             timeout,
		Entries:             entries,
		WantUnicastResponse: false,
		Interface:           interfaceForIP(cfg.AdvertiseIP), // pin to LAN iface (nil = default)
	}

	queryErr := mdns.Query(params)

	// Close the channel and wait for the drainer goroutine to finish.
	// After this point, Query has returned so the mdns library's select
	// loop has exited — no more concurrent writes to ServiceEntry structs.
	close(entries)
	wg.Wait()

	if queryErr != nil {
		// Some systems don't support multicast; this is non-fatal.
		log.Debug("mDNS: query failed", "err", queryErr)
		return nil, nil
	}

	// Process entries now that the mdns library is done mutating them.
	var peers []DiscoveredPeer
	for _, entry := range received {
		// Filter by cluster name in TXT records.
		if !matchesCluster(entry.InfoFields, clusterName) {
			continue
		}

		var host string
		switch {
		case entry.AddrV4 != nil && !entry.AddrV4.IsUnspecified():
			host = entry.AddrV4.String()
		case entry.AddrV6 != nil && !entry.AddrV6.IsUnspecified():
			host = entry.AddrV6.String()
		case entry.Host != "":
			host = strings.TrimSuffix(entry.Host, ".")
		default:
			continue
		}

		peers = append(peers, DiscoveredPeer{Host: host, Port: entry.Port})
		log.Debug("mDNS: discovered peer", "host", host, "port", entry.Port)
	}

	if len(peers) > 0 {
		log.Info("mDNS: discovered peers", "count", len(peers))
	}

	return peers, nil
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
