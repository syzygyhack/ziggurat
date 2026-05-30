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

	tagSet := make(map[string]bool, len(tags))
	for _, t := range tags {
		tagSet[t] = true
	}

	// Linear scan: find the best (highest priority, lowest seq) match.
	bestIdx := -1
	for i, entry := range q.heap {
		if !matchesTags(entry.task.Requires, tagSet) {
			continue
		}
		if !evalCachedConstraints(entry.constraints, caps) {
			continue
		}
		if !matchesResources(entry.task.Resources, caps) {
			continue
		}
		if !applyFilters(entry.task, filters) {
			continue
		}
		if bestIdx < 0 || q.heap.Less(i, bestIdx) {
			bestIdx = i
		}
	}

	if bestIdx < 0 {
		return nil
	}

	entry := heap.Remove(&q.heap, bestIdx).(*taskEntry)
	return entry.task
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

// PopForRemote removes and returns the highest-priority task that does
// NOT match the local worker's tags, constraints, or resources. These
// are tasks that can only be executed by a remote node. Returns nil if
// every queued task already matches the local capabilities.
func (q *Queue) PopForRemote(localTags []string, localCaps map[string]string) *model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	tagSet := make(map[string]bool, len(localTags))
	for _, t := range localTags {
		tagSet[t] = true
	}

	bestIdx := -1
	for i, entry := range q.heap {
		localMatch := matchesTags(entry.task.Requires, tagSet) &&
			evalCachedConstraints(entry.constraints, localCaps) &&
			matchesResources(entry.task.Resources, localCaps)
		if localMatch {
			continue // local worker can handle this one
		}
		if bestIdx < 0 || q.heap.Less(i, bestIdx) {
			bestIdx = i
		}
	}

	if bestIdx < 0 {
		return nil
	}

	entry := heap.Remove(&q.heap, bestIdx).(*taskEntry)
	return entry.task
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
	if req.CPUCores > 0 {
		v, ok := caps["cpu.cores"]
		if !ok {
			return false
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < req.CPUCores {
			return false
		}
	}
	if req.Memory > 0 {
		v, ok := caps["mem.total"]
		if !ok {
			return false
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < req.Memory {
			return false
		}
	}
	if req.GPUs > 0 {
		v, ok := caps["gpu.count"]
		if !ok {
			return false
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < req.GPUs {
			return false
		}
	}
	return true
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
		if c.Op == "!" {
			return false // malformed sentinel
		}
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
