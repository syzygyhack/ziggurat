package model

import "testing"

func TestTier_String(t *testing.T) {
	tests := []struct {
		tier Tier
		want string
	}{
		{TierSmall, "small"},
		{TierMedium, "medium"},
		{TierLarge, "large"},
		{Tier(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.tier.String(); got != tt.want {
			t.Errorf("Tier(%d).String() = %q, want %q", tt.tier, got, tt.want)
		}
	}
}

func TestStorageStrategy_String(t *testing.T) {
	tests := []struct {
		strategy StorageStrategy
		want     string
	}{
		{Replicated, "replicated"},
		{ErasureCoded, "erasure_coded"},
		{StorageStrategy(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.strategy.String(); got != tt.want {
			t.Errorf("StorageStrategy(%d).String() = %q, want %q", tt.strategy, got, tt.want)
		}
	}
}
