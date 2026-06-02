package node

import (
	"testing"

	"github.com/syzygyhack/ziggurat/internal/util"
)

func TestParseCUDAVersion(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "standard nvcc output",
			input: `nvcc: NVIDIA (R) Cuda compiler driver
Copyright (c) 2005-2024 NVIDIA Corporation
Built on Thu_Mar_28_02:18:24_PDT_2024
Cuda compilation tools, release 12.4, V12.4.131
Build cuda_12.4.r12.4/compiler.34097967_0`,
			want: "12.4",
		},
		{
			name:  "release 11.8",
			input: "Cuda compilation tools, release 11.8, V11.8.89",
			want:  "11.8",
		},
		{
			name:  "no release line",
			input: "some random output\nno version here",
			want:  "",
		},
		{
			name:  "empty",
			input: "",
			want:  "",
		},
		{
			name:  "release at end of line",
			input: "Cuda compilation tools, release 12.0",
			want:  "12.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := util.ParseCUDAVersion(tt.input); got != tt.want {
				t.Errorf("util.ParseCUDAVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

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
