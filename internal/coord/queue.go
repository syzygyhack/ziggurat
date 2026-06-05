package coord

import (
	"container/heap"
	"strconv"
	"sync"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// Queue is a thread-safe priority queue for tasks. Higher priority tasks
// are dequeued first; within the same priority, FIFO order is preserved.
type Queue struct {
	mu   sync.Mutex
	heap taskHeap
	seq  int64 // monotonic counter for FIFO ordering
}

// NewQueue creates an empty task queue.
func NewQueue() *Queue {
	q := &Queue{}
	heap.Init(&q.heap)
	return q
}

// Push adds a task to the queue. Constraint expressions are parsed once
// here so that Pop never re-parses them.
func (q *Queue) Push(t *model.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pushLocked(t)
}

// TryPush atomically checks that the queue length is below maxLen and
// pushes the task. Returns false if the queue is already at or above
// the limit. A maxLen of 0 means unlimited (always succeeds).
func (q *Queue) TryPush(t *model.Task, maxLen int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if maxLen > 0 && q.heap.Len() >= maxLen {
		return false
	}
	q.pushLocked(t)
	return true
}

func (q *Queue) pushLocked(t *model.Task) {
	q.seq++
	entry := &taskEntry{task: t, seq: q.seq}
	entry.constraints = parseConstraints(t.Constraints)
	heap.Push(&q.heap, entry)
}

// Pop removes and returns the highest-priority task whose Requires are
// a subset of the provided tags, whose Constraints are satisfied by the
// given capabilities, and whose resource requests fit within the node's
// advertised capacity.
//
// Optional filter functions are applied after the static checks. If any
// filter returns false, the entry is skipped. This is used for dynamic
// resource fitness checks (e.g. ensuring remaining capacity after
// accounting for already-allocated resources).
//
// The scan is O(n) over the heap slice; only the selected entry triggers
// a heap.Remove (O(log n)).
func (q *Queue) Pop(tags []string, caps map[string]string, filters ...func(*model.Task) bool) *model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	tagSet := makeTagSet(tags)
	return q.popBest(func(e *taskEntry) bool {
		return matchesTags(e.task.Requires, tagSet) &&
			evalCachedConstraints(e.constraints, caps) &&
			matchesResources(e.task.Resources, caps) &&
			matchesRuntime(e.task, caps) &&
			applyFilters(e.task, filters)
	})
}

// popBest scans the heap for the best (highest-priority, lowest-seq) entry
// satisfying match, removes it, and returns its task — or nil if none match.
// The caller must hold q.mu.
func (q *Queue) popBest(match func(*taskEntry) bool) *model.Task {
	bestIdx := -1
	for i, entry := range q.heap {
		if !match(entry) {
			continue
		}
		if bestIdx < 0 || q.heap.Less(i, bestIdx) {
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		return nil
	}
	return heap.Remove(&q.heap, bestIdx).(*taskEntry).task
}

// makeTagSet builds a set from a tag slice for O(1) membership tests.
func makeTagSet(tags []string) map[string]bool {
	set := make(map[string]bool, len(tags))
	for _, t := range tags {
		set[t] = true
	}
	return set
}

// applyFilters runs all filter functions against a task. Returns true
// if all filters pass (or if there are no filters).
func applyFilters(t *model.Task, filters []func(*model.Task) bool) bool {
	for _, fn := range filters {
		if fn != nil && !fn(t) {
			return false
		}
	}
	return true
}

// PopForRemote removes and returns the highest-priority task that the local
// worker should not run: either it does NOT match the local worker's tags,
// constraints, or resources, or the optional prefersRemote predicate reports
// that it is pinned (by affinity) to another node that can take it. Returns
// nil if every queued task should run locally. prefersRemote may be nil.
func (q *Queue) PopForRemote(localTags []string, localCaps map[string]string, prefersRemote func(*model.Task) bool) *model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	tagSet := makeTagSet(localTags)
	return q.popBest(func(e *taskEntry) bool {
		localMatch := matchesTags(e.task.Requires, tagSet) &&
			evalCachedConstraints(e.constraints, localCaps) &&
			matchesResources(e.task.Resources, localCaps) &&
			matchesRuntime(e.task, localCaps)
		// Take tasks the local worker can't run, plus any affinity-pinned to a
		// remote node that currently has capacity.
		return !localMatch || (prefersRemote != nil && prefersRemote(e.task))
	})
}

// Remove removes a task by ID from the queue. Used for cancellation.
func (q *Queue) Remove(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, entry := range q.heap {
		if entry.task.ID == id {
			heap.Remove(&q.heap, i)
			return
		}
	}
}

// PopAny removes and returns the highest-priority task regardless of
// tag/constraint matching. Used by coordinator-only nodes that have no
// local worker and must dispatch everything to remote nodes.
func (q *Queue) PopAny() *model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() == 0 {
		return nil
	}
	entry := heap.Pop(&q.heap).(*taskEntry)
	return entry.task
}

// Len returns the number of tasks in the queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.heap.Len()
}

// matchesRuntime returns false when a task requires a container runtime (it
// specifies an OCI Image) but the node advertises none — so image tasks are
// never routed to nodes that can't execute them.
func matchesRuntime(t *model.Task, caps map[string]string) bool {
	if t.Image == "" {
		return true
	}
	return caps["container.runtime"] != ""
}

func matchesTags(requires []string, tags map[string]bool) bool {
	for _, req := range requires {
		if !tags[req] {
			return false
		}
	}
	return true
}

// matchesResources checks that the node has enough CPU cores, memory, and
// GPUs for the task's resource requests. A zero request means no requirement.
func matchesResources(req model.ResourceReq, caps map[string]string) bool {
	return capAtLeast(caps, "cpu.cores", int64(req.CPUCores)) &&
		capAtLeast(caps, "mem.total", req.Memory) &&
		capAtLeast(caps, "gpu.count", int64(req.GPUs))
}

// capAtLeast reports whether the integer capability named key is present and at
// least min. A non-positive min is always satisfied (no requirement). A missing
// or unparseable capability fails.
func capAtLeast(caps map[string]string, key string, min int64) bool {
	if min <= 0 {
		return true
	}
	v, ok := caps[key]
	if !ok {
		return false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return err == nil && n >= min
}

// parseConstraints pre-parses constraint expression strings into
// Constraint structs so Pop never re-parses. Malformed constraints are
// silently skipped — Submit validates them before enqueuing, and
// recovery should not re-introduce sentinel constraints that would
// create infinite dequeue/repush cycles.
func parseConstraints(exprs []string) []Constraint {
	if len(exprs) == 0 {
		return nil
	}
	out := make([]Constraint, 0, len(exprs))
	for _, expr := range exprs {
		c, err := ParseConstraint(expr)
		if err != nil {
			continue // malformed — validated at submission time
		}
		out = append(out, c)
	}
	return out
}

// evalCachedConstraints evaluates pre-parsed constraints against caps.
func evalCachedConstraints(constraints []Constraint, caps map[string]string) bool {
	for _, c := range constraints {
		if !EvalConstraint(c, caps) {
			return false
		}
	}
	return true
}

// taskEntry wraps a task with sequence number for heap ordering and
// pre-parsed constraints for efficient dequeue matching.
type taskEntry struct {
	task        *model.Task
	seq         int64
	constraints []Constraint // parsed once at Push time
}

// taskHeap implements heap.Interface. Higher priority first, then lower seq (FIFO).
type taskHeap []*taskEntry

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	if h[i].task.Config.Priority != h[j].task.Config.Priority {
		return h[i].task.Config.Priority > h[j].task.Config.Priority
	}
	return h[i].seq < h[j].seq
}

func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x any) {
	*h = append(*h, x.(*taskEntry))
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return entry
}
