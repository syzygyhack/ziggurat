package worker

import (
	"testing"
)

func TestGPUAllocator_Basic(t *testing.T) {
	a := NewGPUAllocator(map[string]string{"gpu.count": "4"})
	if a == nil {
		t.Fatal("expected non-nil allocator")
	}

	// Allocate 2 of 4.
	devs, err := a.Allocate(2)
	if err != nil {
		t.Fatalf("allocate 2: %v", err)
	}
	if len(devs) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(devs))
	}

	// Allocate 2 more — should succeed.
	devs2, err := a.Allocate(2)
	if err != nil {
		t.Fatalf("allocate 2 more: %v", err)
	}
	if len(devs2) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(devs2))
	}

	// No overlap.
	all := append(devs, devs2...)
	seen := map[int]bool{}
	for _, d := range all {
		if seen[d] {
			t.Fatalf("duplicate device %d", d)
		}
		seen[d] = true
	}

	// 0 free — allocating 1 should fail.
	_, err = a.Allocate(1)
	if err == nil {
		t.Fatal("expected error when no GPUs free")
	}

	// Release first batch, allocate again.
	a.Release(devs)
	devs3, err := a.Allocate(2)
	if err != nil {
		t.Fatalf("allocate after release: %v", err)
	}
	if len(devs3) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(devs3))
	}
}

func TestGPUAllocator_NilForNoGPU(t *testing.T) {
	a := NewGPUAllocator(map[string]string{})
	if a != nil {
		t.Fatal("expected nil allocator for no GPUs")
	}

	a = NewGPUAllocator(map[string]string{"gpu.count": "0"})
	if a != nil {
		t.Fatal("expected nil allocator for 0 GPUs")
	}
}

func TestGPUAllocator_ZeroRequest(t *testing.T) {
	a := NewGPUAllocator(map[string]string{"gpu.count": "2"})
	devs, err := a.Allocate(0)
	if err != nil {
		t.Fatalf("allocate 0: %v", err)
	}
	if devs != nil {
		t.Fatalf("expected nil for 0 GPUs, got %v", devs)
	}
}

func TestFormatDevices(t *testing.T) {
	tests := []struct {
		in   []int
		want string
	}{
		{[]int{0}, "0"},
		{[]int{0, 1}, "0,1"},
		{[]int{2, 5, 7}, "2,5,7"},
	}
	for _, tt := range tests {
		got := FormatDevices(tt.in)
		if got != tt.want {
			t.Errorf("FormatDevices(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
