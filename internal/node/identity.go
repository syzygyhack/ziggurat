package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// LoadOrCreateID reads the node UUID from dataDir/node.id, or generates
// and persists a new one on first start.
func LoadOrCreateID(dataDir string) (string, error) {
	path := filepath.Join(dataDir, "node.id")

	data, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(data))
		if id != "" {
			return id, nil
		}
	}

	id := uuid.New().String()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", fmt.Errorf("create data dir %s: %w", dataDir, err)
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("write node id to %s: %w", path, err)
	}

	return id, nil
}
