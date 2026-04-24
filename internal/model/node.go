package model

import "time"

// Role determines a node's function in the cluster.
type Role int

const (
	RoleHybrid      Role = iota // coordinator + worker (default for first node)
	RoleCoordinator             // coordinator only
	RoleWorker                  // worker only
)

func (r Role) String() string {
	switch r {
	case RoleHybrid:
		return "hybrid"
	case RoleCoordinator:
		return "coordinator"
	case RoleWorker:
		return "worker"
	default:
		return "unknown"
	}
}

// Node represents a cluster member.
type Node struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Address      string            `json:"address"`      // gossip address (host:port)
	GRPCAddress  string            `json:"grpc_address"`  // gRPC transport address (host:port)
	Role         Role              `json:"role"`
	Tags         []string          `json:"tags"`
	Capabilities map[string]string `json:"capabilities,omitempty"`
	Load         LoadInfo          `json:"load"`
	Storage      StorageInfo       `json:"storage"`
	JoinedAt     time.Time         `json:"joined_at"`
	LastSeen     time.Time         `json:"last_seen"`
}

// LoadInfo captures current compute load on a node.
type LoadInfo struct {
	CPUPercent   float64 `json:"cpu_percent"`
	MemPercent   float64 `json:"mem_percent"`
	TasksRunning int     `json:"tasks_running"`
	TasksQueued  int     `json:"tasks_queued"`
	Concurrency  int     `json:"concurrency"`
}

// StorageInfo captures current storage usage on a node.
type StorageInfo struct {
	Capacity int64 `json:"capacity"`
	Used     int64 `json:"used"`
	Objects  int   `json:"objects"`
	Shards   int   `json:"shards"`
}
