package coord

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/scheduler"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// NodeRegistry provides cluster node information for dispatch decisions.
type NodeRegistry interface {
	List() []*model.Node
}

// DispatchResult holds the outcome of a task executed on a remote node.
type DispatchResult struct {
	ExitCode    int
	Stdout      string
	Stderr      string
	Error       string
	OutputRef   string
	OutputBytes int64
}

// TaskDispatcher sends tasks to remote nodes and fetches their results.
type TaskDispatcher interface {
	DispatchTask(ctx context.Context, addr string, task *model.Task) (string, error)
	// FetchResult retrieves the result of a completed task from a remote node.
	// Returns an error if the task is not yet terminal.
	FetchResult(ctx context.Context, addr string, taskID string) (*DispatchResult, error)
	// PullObject downloads an object by content hash from a remote node.
	PullObject(ctx context.Context, addr string, hash string) (io.ReadCloser, error)
}

// Dispatcher moves tasks from the local queue to remote nodes. On a hybrid
// node it only dispatches tasks that cannot be executed locally. On a
// coordinator-only node (dispatchAll=true) it dispatches everything since
// there is no local worker.
type Dispatcher struct {
	coord       *Coordinator
	registry    NodeRegistry
	transport   TaskDispatcher
	locator     scheduler.ObjectLocator
	store       *store.Store // local store for output replication
	localID     string
	localTags   []string
	localCaps   map[string]string
	dispatchAll bool // true for coordinator-only nodes
	log         *slog.Logger

	// Track dispatched tasks for result polling.
	dispatchedMu sync.Mutex
	dispatched   map[string]string // taskID -> grpcAddr
}

// NewDispatcher creates a Dispatcher. locator may be nil if object
// location data is unavailable (scoring falls back to load balancing).
// Set dispatchAll=true for coordinator-only nodes that have no local worker.
func NewDispatcher(
	c *Coordinator,
	registry NodeRegistry,
	transport TaskDispatcher,
	locator scheduler.ObjectLocator,
	s *store.Store,
	localID string,
	localTags []string,
	localCaps map[string]string,
	dispatchAll bool,
	log *slog.Logger,
) *Dispatcher {
	return &Dispatcher{
		coord:       c,
		registry:    registry,
		transport:   transport,
		locator:     locator,
		store:       s,
		localID:     localID,
		localTags:   localTags,
		localCaps:   localCaps,
		dispatchAll: dispatchAll,
		dispatched:  make(map[string]string),
		log:         log,
	}
}

// Run starts the dispatch loop. It periodically dequeues tasks for remote
// execution and collects results from previously dispatched tasks.
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatchBatch(ctx)
			d.collectResults(ctx)
		}
	}
}

// dispatchBatch tries to dispatch queued tasks to remote nodes until the
// queue is empty or no candidates can accept work.
func (d *Dispatcher) dispatchBatch(ctx context.Context) {
	// Respect drain: don't dispatch new work while draining.
	if d.coord.IsDraining() {
		return
	}

	for {
		var task *model.Task
		if d.dispatchAll {
			// Coordinator-only: dispatch everything (no local worker).
			task = d.coord.queue.PopAny()
		} else {
			// Hybrid: only dispatch tasks that can't run locally.
			task = d.coord.queue.PopForRemote(d.localTags, d.localCaps)
		}
		if task == nil {
			return
		}

		candidates := d.buildCandidates(task)
		if len(candidates) == 0 {
			// No remote node can handle it; put it back.
			d.coord.queue.Push(task)
			return
		}

		idx := scheduler.Select(task, candidates, d.locator, d.coord.workerLoad)
		if idx < 0 {
			d.coord.queue.Push(task)
			return
		}

		target := candidates[idx]
		node := d.nodeByID(target.NodeID)
		if node == nil || node.GRPCAddress == "" {
			d.coord.queue.Push(task)
			return
		}

		_, err := d.transport.DispatchTask(ctx, node.GRPCAddress, task)
		if err != nil {
			d.log.Warn("dispatch failed, re-queuing locally",
				"task", task.ID, "target", target.NodeID, "err", err)
			d.coord.queue.Push(task)
			return // back off on transport error
		}

		// Mark the coordinator's copy as dispatched and track for result polling.
		// If MarkDispatched returns false, the task was cancelled between pop
		// and dispatch. The remote worker is already executing it (can't recall),
		// but we still track it so collectResults can clean up the entry.
		if !d.coord.MarkDispatched(task.ID, target.NodeID) {
			d.log.Warn("task cancelled before dispatch recorded",
				"task", task.ID, "target", target.NodeID)
		}

		d.dispatchedMu.Lock()
		d.dispatched[task.ID] = node.GRPCAddress
		d.dispatchedMu.Unlock()

		d.log.Info("task dispatched to remote node",
			"task", task.ID, "target", target.NodeID, "addr", node.GRPCAddress)
	}
}

// collectResults polls remote workers for results of dispatched tasks.
// When a result is available, it propagates it to the coordinator.
// Also cleans up stale entries for tasks that are no longer in-flight
// (e.g. requeued after a worker departure, or cancelled).
func (d *Dispatcher) collectResults(ctx context.Context) {
	d.dispatchedMu.Lock()
	snapshot := make(map[string]string, len(d.dispatched))
	for id, addr := range d.dispatched {
		snapshot[id] = addr
	}
	d.dispatchedMu.Unlock()

	for id, addr := range snapshot {
		// Check if the coordinator's task is still in-flight. If it was
		// requeued (worker departed) or cancelled, remove from tracking
		// rather than polling a potentially dead address.
		t, err := d.coord.Get(id)
		if err != nil || isTerminal(t.Status) || t.Status == model.TaskQueued {
			d.dispatchedMu.Lock()
			delete(d.dispatched, id)
			d.dispatchedMu.Unlock()
			continue
		}

		result, fetchErr := d.transport.FetchResult(ctx, addr, id)
		if fetchErr != nil {
			continue // task not yet terminal, or transient error
		}

		// Pull task output from the remote worker to the local store so
		// it is accessible via the coordinator's API. If replication fails,
		// skip completion — the task stays in the dispatched map and will
		// be retried on the next collectResults cycle.
		if result.OutputRef != "" && d.store != nil {
			if err := d.replicateOutput(ctx, addr, result.OutputRef, id); err != nil {
				d.log.Warn("output replication failed, will retry",
					"task", id, "err", err)
				continue
			}
		}

		if err := d.coord.CompleteRemote(
			id,
			result.ExitCode,
			result.Stdout,
			result.Stderr,
			result.Error,
			result.OutputRef,
			result.OutputBytes,
		); err != nil {
			d.log.Error("failed to record remote result", "id", id, "err", err)
		}

		d.dispatchedMu.Lock()
		delete(d.dispatched, id)
		d.dispatchedMu.Unlock()

		d.log.Info("remote task result collected", "id", id)
	}
}

// buildCandidates returns scheduler candidates from cluster nodes that
// can run the given task (matching tags, constraints, resources, and
// role != coordinator). The local node is excluded.
func (d *Dispatcher) buildCandidates(task *model.Task) []scheduler.Candidate {
	nodes := d.registry.List()
	tagSet := func(tags []string) map[string]bool {
		m := make(map[string]bool, len(tags))
		for _, t := range tags {
			m[t] = true
		}
		return m
	}

	var candidates []scheduler.Candidate
	for _, n := range nodes {
		if n.ID == d.localID {
			continue // skip self
		}
		if n.Role == model.RoleCoordinator {
			continue // coordinators don't execute tasks
		}
		if !matchesTags(task.Requires, tagSet(n.Tags)) {
			continue
		}
		if !MatchesConstraints(task.Constraints, n.Capabilities) {
			continue
		}
		if !matchesResources(task.Resources, n.Capabilities) {
			continue
		}
		candidates = append(candidates, scheduler.Candidate{
			NodeID: n.ID,
			Tags:   n.Tags,
			Caps:   n.Capabilities,
		})
	}
	return candidates
}

func (d *Dispatcher) nodeByID(id string) *model.Node {
	for _, n := range d.registry.List() {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// replicateOutput pulls a task's output object from the remote worker to the
// coordinator's local store so it is accessible via the coordinator's API
// (e.g. "ziggurat get output/<taskID>"). Returns an error if replication
// fails — the caller should NOT finalize the task in that case.
func (d *Dispatcher) replicateOutput(ctx context.Context, remoteAddr, outputRef, taskID string) error {
	rc, err := d.transport.PullObject(ctx, remoteAddr, outputRef)
	if err != nil {
		return fmt.Errorf("pull from %s: %w", remoteAddr, err)
	}

	nsKey := fmt.Sprintf("output/%s", taskID)
	hash, err := d.store.Put(ctx, nsKey, rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("store put: %w", err)
	}

	d.log.Info("task output replicated to coordinator",
		"task", taskID, "hash", shortHash(hash))
	return nil
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
