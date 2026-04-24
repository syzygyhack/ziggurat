package coord

import (
	"runtime"
	"sync"
)

// WorkerLoad tracks per-worker running task counts and concurrency limits.
// Implements scheduler.NodeLoad.
type WorkerLoad struct {
	mu      sync.RWMutex
	running map[string]int // nodeID -> running task count
	limits  map[string]int // nodeID -> concurrency limit
}

// NewWorkerLoad creates a WorkerLoad tracker.
func NewWorkerLoad() *WorkerLoad {
	return &WorkerLoad{
		running: make(map[string]int),
		limits:  make(map[string]int),
	}
}

// Load returns the running count and concurrency limit for a node.
func (wl *WorkerLoad) Load(nodeID string) (running, limit int) {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	r := wl.running[nodeID]
	l := wl.limits[nodeID]
	if l == 0 {
		l = runtime.GOMAXPROCS(0) // default to local CPU count
	}
	return r, l
}

// TaskStarted increments the running count for a worker.
func (wl *WorkerLoad) TaskStarted(nodeID string) {
	wl.mu.Lock()
	wl.running[nodeID]++
	wl.mu.Unlock()
}

// TaskFinished decrements the running count for a worker.
func (wl *WorkerLoad) TaskFinished(nodeID string) {
	wl.mu.Lock()
	if wl.running[nodeID] > 0 {
		wl.running[nodeID]--
	}
	wl.mu.Unlock()
}

// SetLimit sets the concurrency limit for a worker.
func (wl *WorkerLoad) SetLimit(nodeID string, limit int) {
	wl.mu.Lock()
	wl.limits[nodeID] = limit
	wl.mu.Unlock()
}

// Snapshot returns the current load state for all workers.
func (wl *WorkerLoad) Snapshot() map[string][2]int {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	snap := make(map[string][2]int, len(wl.running))
	for id, r := range wl.running {
		l := wl.limits[id]
		if l == 0 {
			l = runtime.GOMAXPROCS(0)
		}
		snap[id] = [2]int{r, l}
	}
	return snap
}

// OverloadedWorkers returns node IDs with queue depth > 2x the median running count.
// These are candidates for having their queued tasks redistributed.
func (wl *WorkerLoad) OverloadedWorkers() []string {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	if len(wl.running) < 2 {
		return nil
	}

	// Calculate median load.
	var loads []int
	for _, r := range wl.running {
		loads = append(loads, r)
	}
	// Simple median: sort and take middle.
	for i := 0; i < len(loads); i++ {
		for j := i + 1; j < len(loads); j++ {
			if loads[j] < loads[i] {
				loads[i], loads[j] = loads[j], loads[i]
			}
		}
	}
	median := loads[len(loads)/2]

	threshold := 2 * median
	if threshold < 2 {
		threshold = 2 // minimum threshold to avoid thrashing
	}

	var overloaded []string
	for id, r := range wl.running {
		if r > threshold {
			overloaded = append(overloaded, id)
		}
	}
	return overloaded
}
