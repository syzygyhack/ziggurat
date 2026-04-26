package model

import (
	"encoding/json"
	"fmt"
	"time"
)

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

// MarshalJSON serializes Role as a human-readable string.
func (r Role) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// UnmarshalJSON deserializes Role from a string (or numeric for backwards compat).
func (r *Role) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "hybrid", "":
			*r = RoleHybrid
		case "coordinator":
			*r = RoleCoordinator
		case "worker":
			*r = RoleWorker
		default:
			return fmt.Errorf("unknown role: %s", str)
		}
		return nil
	}
	// Fall back to numeric for data persisted before this change.
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("role must be a string or integer")
	}
	*r = Role(n)
	return nil
}

// Node represents a cluster member.
type Node struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Address      string            `json:"address"`       // gossip address (host:port)
	HTTPAddress  string            `json:"http_address"`   // HTTP API address (host:port)
	GRPCAddress  string            `json:"grpc_address"`   // gRPC transport address (host:port)
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
