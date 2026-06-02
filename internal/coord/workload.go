package coord

import (
	"runtime"
	"strconv"
	"sync"

	"github.com/syzygyhack/ziggurat/internal/scheduler"
)

// WorkerLoad tracks per-worker running task counts, concurrency limits,
// and allocated resources. Implements scheduler.NodeLoad and
// scheduler.ResourceTracker.
type WorkerLoad struct {
	mu      sync.RWMutex
	running map[string]int         // nodeID -> running task count
	limits  map[string]int         // nodeID -> concurrency limit
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
	return wl.running[nodeID], wl.limitLocked(nodeID)
}

// limitLocked returns the effective concurrency limit for a node, falling back
// to the local CPU count when unset. The caller must hold wl.mu (read or write).
func (wl *WorkerLoad) limitLocked(nodeID string) int {
	l := wl.limits[nodeID]
	if l == 0 {
		l = runtime.GOMAXPROCS(0) // default to local CPU count
	}
	return l
}

// HasCapacity reports whether a known node is running fewer tasks than its
// concurrency limit. Returns false for unknown nodes (no limit recorded — e.g.
// departed or never seen), so callers treat them as unavailable. Used to decide
// whether to honor node affinity to a remote node.
func (wl *WorkerLoad) HasCapacity(nodeID string) bool {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	limit, known := wl.limits[nodeID]
	if !known || limit <= 0 {
		return false
	}
	return wl.running[nodeID] < limit
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

// ClearWorker removes all load, limit, and allocation tracking for a node.
// Called when a node departs the cluster so its stale entries don't linger.
func (wl *WorkerLoad) ClearWorker(nodeID string) {
	wl.mu.Lock()
	delete(wl.running, nodeID)
	delete(wl.limits, nodeID)
	delete(wl.alloc, nodeID)
	wl.mu.Unlock()
}

// concurrencyLimitFromCaps derives a worker's concurrency limit from its
// advertised capabilities: compute.concurrency if present, else cpu.cores.
// Returns 0 if neither is available or parseable (caller leaves the limit
// unset, falling back to the local CPU count).
func concurrencyLimitFromCaps(caps map[string]string) int {
	for _, key := range []string{"compute.concurrency", "cpu.cores"} {
		if v, ok := caps[key]; ok {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

// Snapshot returns the current load state for all workers.
func (wl *WorkerLoad) Snapshot() map[string][2]int {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	snap := make(map[string][2]int, len(wl.running))
	for id, r := range wl.running {
		snap[id] = [2]int{r, wl.limitLocked(id)}
	}
	return snap
}

// OverloadedWorkers returns nodes running more tasks than their own
// concurrency limit — an oversubscribed backlog whose excess is necessarily in
// SCHEDULED (not-yet-running) state and thus safe to redistribute.
//
// This is capacity-aware: it compares each node's running count against that
// node's own limit, so a high-capacity node is not flagged for merely running
// many tasks (the previous raw-count-vs-median heuristic penalised powerful
// nodes and could never fire in a two-node cluster). If no node is
// oversubscribed, or fewer than two workers are known, returns nil. When work
// is stolen but no node has spare capacity, the dispatcher's existing
// no-candidate path simply re-queues it — no duplicate execution.
func (wl *WorkerLoad) OverloadedWorkers() []string {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	if len(wl.limits) < 2 {
		return nil // need at least two known workers to redistribute
	}

	var overloaded []string
	for id, r := range wl.running {
		if r > wl.limitLocked(id) {
			overloaded = append(overloaded, id)
		}
	}
	return overloaded
}
