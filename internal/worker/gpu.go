package worker

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// GPUAllocator tracks per-device GPU assignment so concurrent tasks each
// get exclusive devices via CUDA_VISIBLE_DEVICES. Thread-safe.
type GPUAllocator struct {
	mu    sync.Mutex
	total int
	inUse map[int]bool // device index -> in use
}

// NewGPUAllocator creates an allocator from the node's capabilities.
// Returns nil if the node has no GPUs (gpu.count absent or 0).
func NewGPUAllocator(caps map[string]string) *GPUAllocator {
	v, ok := caps["gpu.count"]
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return nil
	}
	return &GPUAllocator{
		total: n,
		inUse: make(map[int]bool, n),
	}
}

// Allocate reserves n GPU device indices. Returns the indices or an error
// if not enough devices are free. Passing n=0 is a no-op (returns nil, nil).
func (a *GPUAllocator) Allocate(n int) ([]int, error) {
	if n <= 0 {
		return nil, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// Collect free devices.
	var free []int
	for i := 0; i < a.total; i++ {
		if !a.inUse[i] {
			free = append(free, i)
		}
	}
	if len(free) < n {
		return nil, fmt.Errorf("need %d GPUs but only %d of %d free", n, len(free), a.total)
	}

	devices := free[:n]
	for _, d := range devices {
		a.inUse[d] = true
	}
	return devices, nil
}

// Release returns GPU device indices to the free pool.
func (a *GPUAllocator) Release(devices []int) {
	if len(devices) == 0 {
		return
	}
	a.mu.Lock()
	for _, d := range devices {
		delete(a.inUse, d)
	}
	a.mu.Unlock()
}

// FormatDevices returns a CUDA_VISIBLE_DEVICES value string (e.g. "0,2,3").
func FormatDevices(devices []int) string {
	parts := make([]string, len(devices))
	for i, d := range devices {
		parts[i] = strconv.Itoa(d)
	}
	return strings.Join(parts, ",")
}
