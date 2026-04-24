package store

import (
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
)

// DefaultTestConfig returns a minimal StorageConfig for use in tests
// across packages.
func DefaultTestConfig() config.StorageConfig {
	return config.StorageConfig{
		ReplicationFactor: 1,
		TierThresholds: config.TierConfig{
			Medium: 1 << 20,
			Large:  64 << 20,
		},
		GCGracePeriod: 1 * time.Hour,
	}
}
