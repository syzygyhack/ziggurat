package coord

import (
	"runtime"
	"sort"
	"sync"

	"github.com/syzygyhack/ziggurat/internal/scheduler"
)

// WorkerLoad tracks per-worker running task counts, concurrency limits,
// and allocated resources. Implements scheduler.NodeLoad and
// scheduler.ResourceTracker.
type WorkerLoad struct {
	mu      sync.RWMutex
	running map[string]int // nodeID -> running task count
	limits  map[string]int // nodeID -> concurrency limit
	alloc   map[string]*allocState // nodeID -> allocated resources
}

// allocState tracks resources allocated to running tasks on a single worker.
type allocState struct {
	memory   int64
	cpuCores int
	gpus     int
}

// NewWorkerLoad creates a WorkerLoad tracker.
func NewWorkerLoad() *WorkerLoad {
	return &WorkerLoad{
		running: make(map[string]int),
		limits:  make(map[string]int),
		alloc:   make(map[string]*allocState),
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

// AllocResources adds the task's resource requests to the worker's allocated total.
func (wl *WorkerLoad) AllocResources(nodeID string, memory int64, cpuCores, gpus int) {
	wl.mu.Lock()
	a := wl.alloc[nodeID]
	if a == nil {
		a = &allocState{}
		wl.alloc[nodeID] = a
	}
	a.memory += memory
	a.cpuCores += cpuCores
	a.gpus += gpus
	wl.mu.Unlock()
}

// ReleaseResources removes the task's resource requests from the worker's allocated total.
func (wl *WorkerLoad) ReleaseResources(nodeID string, memory int64, cpuCores, gpus int) {
	wl.mu.Lock()
	if a, ok := wl.alloc[nodeID]; ok {
		a.memory -= memory
		if a.memory < 0 {
			a.memory = 0
		}
		a.cpuCores -= cpuCores
		if a.cpuCores < 0 {
			a.cpuCores = 0
		}
		a.gpus -= gpus
		if a.gpus < 0 {
			a.gpus = 0
		}
	}
	wl.mu.Unlock()
}

// Allocated returns the resources currently allocated to running tasks on a node.
// Implements scheduler.ResourceTracker.
func (wl *WorkerLoad) Allocated(nodeID string) scheduler.AllocatedResources {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	a := wl.alloc[nodeID]
	if a == nil {
		return scheduler.AllocatedResources{}
	}
	return scheduler.AllocatedResources{
		Memory:   a.memory,
		CPUCores: a.cpuCores,
		GPUs:     a.gpus,
	}
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
	sort.Ints(loads)
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
