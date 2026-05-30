package cluster

import (
	"encoding/json"
)

// NodeMeta is the metadata payload broadcast by each node via memberlist.
// It is serialized to JSON and attached to the memberlist Node.Meta field.
// Keep this small — memberlist has a default meta limit of 512 bytes.
type NodeMeta struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	HTTPPort    int               `json:"http_port"`
	GRPCPort    int               `json:"grpc_port"`
	Tags        []string          `json:"tags,omitempty"`
	Caps        map[string]string `json:"caps,omitempty"`
	Role        string            `json:"role"`
	ClusterName string            `json:"cluster"`
}

func encodeMeta(m *NodeMeta) ([]byte, error) {
	return json.Marshal(m)
}

func decodeMeta(data []byte) (*NodeMeta, error) {
	var m NodeMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
