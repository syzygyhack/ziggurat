package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/syzygyhack/ziggurat/internal/util"
)

// Config mirrors the ziggurat.yaml schema.
type Config struct {
	Node       NodeConfig       `yaml:"node"`
	Network    NetworkConfig    `yaml:"network"`
	Client     ClientConfig     `yaml:"client"`
	Cluster    ClusterConfig    `yaml:"cluster"`
	Storage    StorageConfig    `yaml:"storage"`
	Compute    ComputeConfig    `yaml:"compute"`
	Resilience ResilienceConfig `yaml:"resilience"`
	Security   SecurityConfig   `yaml:"security"`
	Log        LogConfig        `yaml:"log"`
	Metrics    MetricsConfig    `yaml:"metrics"`
}

type NodeConfig struct {
	Name         string            `yaml:"name"`
	Role         string            `yaml:"role"` // "hybrid" (default), "coordinator", "worker"
	Tags         []string          `yaml:"tags"`
	Capabilities map[string]string `yaml:"capabilities"`
	DataDir      string            `yaml:"data_dir"`
}

type NetworkConfig struct {
	Bind       string `yaml:"bind"`
	HTTPPort   int    `yaml:"http_port"`
	GRPCPort   int    `yaml:"grpc_port"`
	GossipPort int    `yaml:"gossip_port"`
	Advertise  string `yaml:"advertise"`
}

type ClientConfig struct {
	Addr string `yaml:"addr"`
}

type ClusterConfig struct {
	Discovery string   `yaml:"discovery"`
	Seeds     []string `yaml:"seeds"`
	Name      string   `yaml:"name"`
}

type StorageConfig struct {
	DataDir           string        `yaml:"data_dir"`
	Capacity          int64         `yaml:"capacity"`
	ReplicationFactor int           `yaml:"replication_factor"`
	Erasure           ErasureConfig `yaml:"erasure"`
	TierThresholds    TierConfig    `yaml:"tier_thresholds"`
	GCGracePeriod     time.Duration `yaml:"gc_grace_period"`
}

type ErasureConfig struct {
	Enabled      bool `yaml:"enabled"`
	DataShards   int  `yaml:"data_shards"`
	ParityShards int  `yaml:"parity_shards"`
}

type TierConfig struct {
	Medium int64 `yaml:"medium"`
	Large  int64 `yaml:"large"`
}

type ComputeConfig struct {
	Concurrency           int           `yaml:"concurrency"`
	MemoryLimit           int64         `yaml:"memory_limit"`
	TaskTimeout           time.Duration `yaml:"task_timeout"`
	MaxOutputSize         int64         `yaml:"max_output_size"`
	CancelGrace           time.Duration `yaml:"cancel_grace"`
	WorkspaceDir          string        `yaml:"workspace_dir"`
	MaxRetainedWorkspaces int           `yaml:"max_retained_workspaces"`
	EnvMaxAge             time.Duration `yaml:"env_max_age"`   // prune envs unused for this long (default 7d)
	EnvMaxCount           int           `yaml:"env_max_count"` // max persistent envs before FIFO eviction (default 50)
}

type ResilienceConfig struct {
	Mode              string        `yaml:"mode"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	SuspicionTimeout  time.Duration `yaml:"suspicion_timeout"`
	TaskRetries       int           `yaml:"task_retries"`
	DeadLetter        bool          `yaml:"dead_letter"`
	MaxQueueDepth     int           `yaml:"max_queue_depth"` // 0 = unlimited
}

type SecurityConfig struct {
	TLS       TLSConfig  `yaml:"tls"`
	JoinToken string     `yaml:"join_token"` // shared secret for cluster join
	APIToken  string     `yaml:"api_token"`  // bearer token for HTTP API (empty = no auth)
}

type TLSConfig struct {
	Enabled bool   `yaml:"enabled"` // enable mTLS for gRPC
	CertsDir string `yaml:"certs_dir"` // override cert directory (default: data_dir/certs)
}

type MetricsConfig struct {
	Enabled bool `yaml:"enabled"`
}

// LogFormat is the log output format.
type LogFormat string

const (
	LogFormatText LogFormat = "text"
	LogFormatJSON LogFormat = "json"
)

type LogConfig struct {
	Format LogFormat `yaml:"format"` // text (default) or json
	Level  string    `yaml:"level"`  // debug, info, warn, error (default: info)
}

// DefaultDataDir returns the platform-standard data directory (~/.ziggurat).
func DefaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".ziggurat")
}

// DefaultConfig returns a Config with sensible defaults for single-node operation.
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			DataDir: DefaultDataDir(),
		},
		Network: NetworkConfig{
			Bind:       "0.0.0.0",
			HTTPPort:   7100,
			GRPCPort:   7101,
			GossipPort: 7102,
		},
		Cluster: ClusterConfig{
			Discovery: "auto",
			Name:      "default",
		},
		Storage: StorageConfig{
			ReplicationFactor: 2,
			Erasure: ErasureConfig{
				Enabled:      true,
				DataShards:   4,
				ParityShards: 2,
			},
			TierThresholds: TierConfig{
				Medium: 1 << 20,  // 1 MB
				Large:  64 << 20, // 64 MB
			},
			GCGracePeriod: 1 * time.Hour,
		},
		Compute: ComputeConfig{
			TaskTimeout:           5 * time.Minute,
			MaxOutputSize:         1 << 30, // 1 GB
			CancelGrace:           10 * time.Second,
			MaxRetainedWorkspaces: 20,
			EnvMaxAge:             7 * 24 * time.Hour, // 7 days
			EnvMaxCount:           50,
		},
		Resilience: ResilienceConfig{
			Mode:              "balanced",
			HeartbeatInterval: 1 * time.Second,
			SuspicionTimeout:  5 * time.Second,
			TaskRetries:       2,
			DeadLetter:        true,
		},
		Metrics: MetricsConfig{
			Enabled: true,
		},
	}
}

// ConfigPath returns the path to the config file inside the data directory.
func ConfigPath() string {
	return filepath.Join(DefaultDataDir(), "ziggurat.yaml")
}

// LoadConfig reads a YAML config file and merges it over defaults.
// If path is empty, it searches: ./ziggurat.yaml then ~/.ziggurat/ziggurat.yaml.
// If neither exists, bare defaults are returned (single-node LAN operation).
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		// 1. Current directory.
		if _, err := os.Stat("ziggurat.yaml"); err == nil {
			path = "ziggurat.yaml"
		} else if p := ConfigPath(); util.FileExists(p) {
			// 2. Data directory (~/.ziggurat/ziggurat.yaml).
			path = p
		} else {
			// No config file anywhere — defaults are fine.
			return cfg, nil
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	// Apply ZIGGURAT_ADDR env var for client connection.
	if addr := os.Getenv("ZIGGURAT_ADDR"); addr != "" && cfg.Client.Addr == "" {
		cfg.Client.Addr = addr
	}

	return cfg, nil
}

// Validate checks the configuration for correctness and returns a
// descriptive error for the first problem found. It is safe to call on nil.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}

	// Node role.
	switch c.Node.Role {
	case "", "hybrid", "coordinator", "worker":
	default:
		return fmt.Errorf("node.role must be one of: hybrid, coordinator, worker (got %q)", c.Node.Role)
	}

	// Port ranges and uniqueness.
	for _, port := range []struct {
		name string
		val  int
	}{
		{"http_port", c.Network.HTTPPort},
		{"grpc_port", c.Network.GRPCPort},
		{"gossip_port", c.Network.GossipPort},
	} {
		if port.val < 1 || port.val > 65535 {
			return fmt.Errorf("network.%s must be in range 1-65535 (got %d)", port.name, port.val)
		}
	}
	if c.Network.HTTPPort == c.Network.GRPCPort {
		return fmt.Errorf("network.http_port and network.grpc_port must be different (both use port %d)", c.Network.HTTPPort)
	}
	if c.Network.HTTPPort == c.Network.GossipPort {
		return fmt.Errorf("network.http_port and network.gossip_port must be different (both use port %d)", c.Network.HTTPPort)
	}
	if c.Network.GRPCPort == c.Network.GossipPort {
		return fmt.Errorf("network.grpc_port and network.gossip_port must be different (both use port %d)", c.Network.GRPCPort)
	}

	// Storage.
	if c.Storage.ReplicationFactor < 1 {
		return fmt.Errorf("storage.replication_factor must be >= 1 (got %d)", c.Storage.ReplicationFactor)
	}
	if c.Storage.Capacity < 0 {
		return fmt.Errorf("storage.capacity must be >= 0 (got %d)", c.Storage.Capacity)
	}
	if c.Storage.Erasure.Enabled {
		if c.Storage.Erasure.DataShards < 1 {
			return fmt.Errorf("storage.erasure.data_shards must be >= 1 (got %d)", c.Storage.Erasure.DataShards)
		}
		if c.Storage.Erasure.ParityShards < 1 {
			return fmt.Errorf("storage.erasure.parity_shards must be >= 1 (got %d)", c.Storage.Erasure.ParityShards)
		}
	}
	if c.Storage.GCGracePeriod <= 0 {
		return fmt.Errorf("storage.gc_grace_period must be > 0 (got %s)", c.Storage.GCGracePeriod)
	}

	// Compute.
	if c.Compute.Concurrency < 0 {
		return fmt.Errorf("compute.concurrency must be >= 0 (got %d)", c.Compute.Concurrency)
	}
	if c.Compute.TaskTimeout <= 0 {
		return fmt.Errorf("compute.task_timeout must be > 0 (got %s)", c.Compute.TaskTimeout)
	}
	if c.Compute.CancelGrace <= 0 {
		return fmt.Errorf("compute.cancel_grace must be > 0 (got %s)", c.Compute.CancelGrace)
	}

	// Resilience.
	if c.Resilience.TaskRetries < 0 {
		return fmt.Errorf("resilience.task_retries must be >= 0 (got %d)", c.Resilience.TaskRetries)
	}
	if c.Resilience.MaxQueueDepth < 0 {
		return fmt.Errorf("resilience.max_queue_depth must be >= 0 (got %d)", c.Resilience.MaxQueueDepth)
	}

	return nil
}

