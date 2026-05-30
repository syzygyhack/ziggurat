package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	TokenHMAC   string            `json:"token_hmac,omitempty"` // HMAC-SHA256(join_token, node_id)
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

// computeJoinHMAC returns the hex-encoded HMAC-SHA256 of the join token
// keyed with the node ID. If token is empty, returns empty string (open
// cluster — no token required).
func computeJoinHMAC(nodeID, joinToken string) string {
	if joinToken == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(joinToken))
	mac.Write([]byte(nodeID))
	return hex.EncodeToString(mac.Sum(nil))
}

// validateJoinHMAC checks that the received HMAC matches the expected value
// computed from the local join token. If the local token is empty (open
// cluster), validation always passes. If the received HMAC is empty but the
// local token is non-empty, validation fails.
func validateJoinHMAC(nodeID, receivedHMAC, localJoinToken string) bool {
	expected := computeJoinHMAC(nodeID, localJoinToken)
	if expected == "" {
		return true // open cluster — no token required
	}
	return hmac.Equal([]byte(receivedHMAC), []byte(expected))
}
