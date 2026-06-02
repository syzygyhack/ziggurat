package store

import (
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
)

func TestPreferredClasses(t *testing.T) {
	tests := []struct {
		tier  model.Tier
		first model.StorageClass
	}{
		{model.TierSmall, model.StorageClassNVMe},
		{model.TierMedium, model.StorageClassSSD},
		{model.TierLarge, model.StorageClassHDD},
	}

	for _, tt := range tests {
		classes := PreferredClasses(tt.tier)
		if len(classes) == 0 {
			t.Fatalf("tier %v returned no preferred classes", tt.tier)
		}
		if classes[0] != tt.first {
			t.Errorf("tier %v: expected first class %s, got %s", tt.tier, tt.first, classes[0])
		}
	}
}

func TestSelectNodes_PrefersBestClass(t *testing.T) {
	candidates := []NodeInfo{
		{ID: "hdd-1", StorageClass: model.StorageClassHDD, FreeBytes: 100 << 30},
		{ID: "nvme-1", StorageClass: model.StorageClassNVMe, FreeBytes: 50 << 30},
		{ID: "ssd-1", StorageClass: model.StorageClassSSD, FreeBytes: 80 << 30},
	}

	// Small tier prefers NVMe.
	result := SelectNodes(PlacementStrategy{
		Tier:      model.TierSmall,
		ShardSize: 1 << 20, // 1 MB
	}, candidates, 1)

	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0] != "nvme-1" {
		t.Errorf("small tier should prefer NVMe, got %s", result[0])
	}

	// Large tier prefers HDD.
	result = SelectNodes(PlacementStrategy{
		Tier:      model.TierLarge,
		ShardSize: 1 << 30, // 1 GB
	}, candidates, 1)

	if result[0] != "hdd-1" {
		t.Errorf("large tier should prefer HDD, got %s", result[0])
	}
}

func TestSelectNodes_SkipsInsufficientSpace(t *testing.T) {
	candidates := []NodeInfo{
		{ID: "small", StorageClass: model.StorageClassSSD, FreeBytes: 100}, // too small
		{ID: "big", StorageClass: model.StorageClassSSD, FreeBytes: 10 << 30},
	}

	result := SelectNodes(PlacementStrategy{
		Tier:      model.TierMedium,
		ShardSize: 1 << 20, // 1 MB
	}, candidates, 2)

	if len(result) != 1 {
		t.Fatalf("expected 1 result (small should be excluded), got %d", len(result))
	}
	if result[0] != "big" {
		t.Errorf("expected 'big' node, got %s", result[0])
	}
}

func TestSelectNodes_SpreadAcrossNodes(t *testing.T) {
	candidates := []NodeInfo{
		{ID: "a", StorageClass: model.StorageClassHDD, FreeBytes: 100 << 30},
		{ID: "b", StorageClass: model.StorageClassHDD, FreeBytes: 80 << 30},
		{ID: "c", StorageClass: model.StorageClassHDD, FreeBytes: 60 << 30},
	}

	result := SelectNodes(PlacementStrategy{
		Tier:      model.TierLarge,
		ShardSize: 1 << 30,
	}, candidates, 3)

	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}

	// All unique.
	seen := make(map[string]bool)
	for _, r := range result {
		if seen[r] {
			t.Fatalf("duplicate node %s", r)
		}
		seen[r] = true
	}
}

func TestSelectNodes_Empty(t *testing.T) {
	result := SelectNodes(PlacementStrategy{}, nil, 3)
	if len(result) != 0 {
		t.Fatalf("expected 0 for nil candidates, got %d", len(result))
	}
}

func TestStorageClassFromCaps(t *testing.T) {
	tests := []struct {
		caps   map[string]string
		expect model.StorageClass
	}{
		{nil, model.StorageClassHDD},
		{map[string]string{}, model.StorageClassHDD},
		{map[string]string{"storage.class": "nvme"}, model.StorageClassNVMe},
		{map[string]string{"storage.class": "ssd"}, model.StorageClassSSD},
		{map[string]string{"storage.class": "hdd"}, model.StorageClassHDD},
		{map[string]string{"storage.class": "s3"}, model.StorageClassS3},
		{map[string]string{"storage.class": "unknown"}, model.StorageClassHDD},
	}

	for _, tt := range tests {
		got := StorageClassFromCaps(tt.caps)
		if got != tt.expect {
			t.Errorf("caps %v: expected %s, got %s", tt.caps, tt.expect, got)
		}
	}
}
