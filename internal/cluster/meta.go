package cluster

import (
	"bytes"
	"compress/flate"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
)

// NodeMeta is the metadata payload broadcast by each node via memberlist.
// It is serialized to JSON, DEFLATE-compressed, and attached to the memberlist
// Node.Meta field. memberlist hard-caps Node.Meta at 512 bytes (MetaMaxSize is
// a const), and capability maps (GPU model strings, runtime versions, etc.)
// readily exceed that uncompressed — so we compress. JSON with repetitive keys
// compresses ~2x, keeping realistic nodes well under the limit.
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

// encodeMeta marshals node metadata to JSON and DEFLATE-compresses it so it
// fits within memberlist's 512-byte meta limit.
func encodeMeta(m *NodeMeta) ([]byte, error) {
	j, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(j); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeMeta reverses encodeMeta. For forward/backward compatibility it also
// accepts uncompressed JSON (a leading '{'), so a mixed-version cluster during
// a rolling upgrade still interoperates.
func decodeMeta(data []byte) (*NodeMeta, error) {
	var m NodeMeta
	if len(data) > 0 && data[0] == '{' {
		// Legacy uncompressed JSON.
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return &m, nil
	}
	r := flate.NewReader(bytes.NewReader(data))
	j, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(j, &m); err != nil {
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
