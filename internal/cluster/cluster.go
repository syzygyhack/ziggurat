package cluster

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/syzygyhack/ziggurat/internal/config"
)

// Config holds the parameters needed to start a cluster member.
type Config struct {
	NodeID        string
	NodeName      string
	BindAddr      string
	AdvertiseAddr string // external IP when behind NAT; empty = use BindAddr
	BindPort      int    // gossip port; 0 = auto-assign
	HTTPPort      int    // advertised HTTP port for this node
	GRPCPort      int    // advertised gRPC port for inter-node transport
	Seeds         []string
	Tags          []string
	Caps          map[string]string
	Role          string // "hybrid", "coordinator", "worker"
	Discovery     string // "auto", "mdns", "seeds", "static" — controls peer discovery
	ClusterName   string // logical cluster name for mDNS filtering
}

// Cluster manages gossip-based membership and the node registry.
type Cluster struct {
	ml       *memberlist.Memberlist
	Registry *Registry
	delegate *delegate
	mdns     *MDNSServer // nil if mDNS is disabled
	log      *slog.Logger
}

// New creates and starts a cluster member. If seeds is non-empty, the node
// will attempt to join an existing cluster. Otherwise it forms a new cluster
// of one.
func New(cfg Config, log *slog.Logger) (*Cluster, error) {
	registry := NewRegistry(log)

	clusterName := cfg.ClusterName
	if clusterName == "" {
		clusterName = "default"
	}
	meta := &NodeMeta{
		ID:          cfg.NodeID,
		Name:        cfg.NodeName,
		HTTPPort:    cfg.HTTPPort,
		GRPCPort:    cfg.GRPCPort,
		Tags:        cfg.Tags,
		Caps:        cfg.Caps,
		Role:        cfg.Role,
		ClusterName: clusterName,
	}

	del, err := newDelegate(meta)
	if err != nil {
		return nil, fmt.Errorf("create delegate: %w", err)
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeID
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.BindPort
	mlCfg.AdvertisePort = cfg.BindPort
	if cfg.AdvertiseAddr != "" {
		mlCfg.AdvertiseAddr = cfg.AdvertiseAddr
	}
	mlCfg.Delegate = del
	mlCfg.Events = &eventDelegate{registry: registry, clusterName: clusterName}

	// Suppress memberlist's built-in logging — we route through slog.
	mlCfg.LogOutput = &slogWriter{log: log.With("component", "memberlist")}

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}

	c := &Cluster{
		ml:       ml,
		Registry: registry,
		delegate: del,
		log:      log,
	}

	// Determine seeds: explicit seeds take priority, then mDNS discovery.
	seeds := cfg.Seeds
	discovery := cfg.Discovery
	if discovery == "" {
		discovery = "auto"
	}

	if len(seeds) == 0 && (discovery == "auto" || discovery == "mdns") {
		clusterName := cfg.ClusterName
		if clusterName == "" {
			clusterName = "default"
		}
		discovered, err := DiscoverAddrs(MDNSDiscoverConfig{
			ClusterName: clusterName,
			Timeout:     3 * time.Second,
		}, log)
		if err != nil {
			log.Debug("cluster: mDNS discovery failed", "err", err)
		} else if len(discovered) > 0 {
			seeds = discovered
			log.Info("cluster: discovered peers via mDNS", "count", len(seeds))
		}
	}

	// Join existing cluster if seeds are available (explicit or discovered).
	if len(seeds) > 0 {
		n, err := ml.Join(seeds)
		if err != nil {
			log.Warn("cluster: join failed, running standalone", "seeds", seeds, "err", err)
		} else {
			log.Info("cluster: joined", "contacted", n, "members", ml.NumMembers())
		}
	} else {
		log.Info("cluster: started new cluster", "members", ml.NumMembers())
	}

	// Start mDNS advertisement so other nodes can discover us.
	if discovery == "auto" || discovery == "mdns" {
		gossipPort := cfg.BindPort
		if gossipPort == 0 {
			// Memberlist auto-assigned a port; read the actual bound port.
			gossipPort = int(ml.LocalNode().Port)
		}
		clusterName := cfg.ClusterName
		if clusterName == "" {
			clusterName = "default"
		}
		mdnsSrv, err := NewMDNSServer(MDNSConfig{
			NodeID:      cfg.NodeID,
			GossipPort:  gossipPort,
			ClusterName: clusterName,
		}, log)
		if err != nil {
			log.Warn("cluster: mDNS advertisement failed", "err", err)
		} else {
			c.mdns = mdnsSrv
		}
	}

	return c, nil
}

// Join attempts to join nodes at the given addresses.
func (c *Cluster) Join(addrs []string) (int, error) {
	return c.ml.Join(addrs)
}

// Leave gracefully leaves the cluster with a timeout.
func (c *Cluster) Leave() error {
	return c.ml.Leave(0)
}

// Shutdown forcefully stops the memberlist agent and mDNS advertiser.
func (c *Cluster) Shutdown() error {
	if c.mdns != nil {
		c.mdns.Shutdown()
	}
	return c.ml.Shutdown()
}

// NumMembers returns the number of nodes in the cluster.
func (c *Cluster) NumMembers() int {
	return c.ml.NumMembers()
}

// LocalAddr returns the gossip bind address.
func (c *Cluster) LocalAddr() string {
	local := c.ml.LocalNode()
	return net.JoinHostPort(local.Addr.String(), strconv.Itoa(int(local.Port)))
}

// UpdateMeta replaces the node's broadcast metadata (e.g. after cap refresh).
func (c *Cluster) UpdateMeta(caps map[string]string, tags []string) {
	c.delegate.mu.RLock()
	old, _ := decodeMeta(c.delegate.meta)
	c.delegate.mu.RUnlock()

	if old == nil {
		return
	}
	old.Tags = tags
	old.Caps = caps
	c.delegate.UpdateMeta(old)
	c.ml.UpdateNode(0) // trigger gossip re-broadcast
}

// ConfigFromNode builds a cluster Config from the node's existing configuration.
func ConfigFromNode(nodeID string, nodeCfg config.NodeConfig, netCfg config.NetworkConfig, clusterCfg config.ClusterConfig, caps map[string]string) Config {
	bindAddr := netCfg.Bind
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	gossipPort := netCfg.GossipPort
	if gossipPort == 0 {
		gossipPort = 7102
	}

	return Config{
		NodeID:        nodeID,
		NodeName:      nodeCfg.Name,
		BindAddr:      bindAddr,
		AdvertiseAddr: netCfg.Advertise,
		BindPort:      gossipPort,
		HTTPPort:      netCfg.HTTPPort,
		GRPCPort:      netCfg.GRPCPort,
		Seeds:         clusterCfg.Seeds,
		Tags:          nodeCfg.Tags,
		Caps:          caps,
		Role:          nodeRole(nodeCfg.Role),
		Discovery:     clusterCfg.Discovery,
		ClusterName:   clusterCfg.Name,
	}
}

// nodeRole normalises a role string, defaulting to "hybrid".
func nodeRole(r string) string {
	switch r {
	case "coordinator", "worker":
		return r
	default:
		return "hybrid"
	}
}

// slogWriter adapts slog.Logger to io.Writer for memberlist's log output.
type slogWriter struct {
	log *slog.Logger
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	w.log.Debug(string(p))
	return len(p), nil
}
