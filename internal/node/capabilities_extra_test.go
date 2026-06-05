package node

import (
	"testing"
)

func TestDedup(t *testing.T) {
	tests := []struct {
		input []string
		want  []string
	}{
		{[]string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{[]string{"a", "a", "b"}, []string{"a", "b"}},
		{[]string{"x", "x", "x"}, []string{"x"}},
		{nil, nil},
		{[]string{}, nil},
	}
	for _, tt := range tests {
		got := dedup(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("dedup(%v) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("dedup(%v)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}

func TestDetectCapabilities_WithDataDir(t *testing.T) {
	dir := t.TempDir()
	caps := DetectCapabilities(dir)

	// With a valid data dir, disk.avail should be set.
	if caps["disk.avail"] == "" {
		t.Error("disk.avail should be set when dataDir is provided")
	}
}

func TestRefreshDiskAvail(t *testing.T) {
	dir := t.TempDir()
	caps := map[string]string{}

	RefreshDiskAvail(caps, dir)
	if caps["disk.avail"] == "" {
		t.Error("disk.avail should be set after refresh")
	}

	// Empty dataDir should be no-op.
	caps2 := map[string]string{}
	RefreshDiskAvail(caps2, "")
	if _, ok := caps2["disk.avail"]; ok {
		t.Error("disk.avail should not be set for empty dataDir")
	}
}
