package coord

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/syzygyhack/ziggurat/internal/metrics"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// TaskDefaults holds cluster-wide defaults applied when a task doesn't
// specify its own values.
type TaskDefaults struct {
	MaxRetries int
	Timeout    time.Duration
	DeadLetter bool
}

// Coordinator manages task lifecycle: submission, scheduling, state tracking.
type Coordinator struct {
	store    *store.Store
	persist  *Persist
	queue    *Queue
	log      *slog.Logger
	defaults TaskDefaults

	mu       sync.RWMutex
	tasks    map[string]*model.Task // id -> task
	draining bool                   // when true, Dequeue returns nil

	// Waiters blocked on task completion.
	waitMu  sync.Mutex
	waiters map[string][]chan struct{}

	// Cancel functions for running tasks, keyed by task ID.
	cancelMu sync.Mutex
	cancels  map[string]context.CancelFunc

	// Optional callback fired when a task reaches a terminal state.
	onComplete func(ctx context.Context, taskID string, status model.TaskStatus)

	// callbackWg tracks in-flight onComplete goroutines so shutdown can
	// wait for them to finish before closing databases.
	callbackWg sync.WaitGroup

	// Per-worker load tracking for scheduling decisions and work stealing.
	workerLoad *WorkerLoad
}

// New creates a Coordinator backed by the given store and persistence layer.
func New(s *store.Store, p *Persist, defaults TaskDefaults, log *slog.Logger) *Coordinator {
	return &Coordinator{
		store:    s,
		persist:  p,
		queue:    NewQueue(),
		log:      log,
		defaults: defaults,
		tasks:      make(map[string]*model.Task),
		waiters:    make(map[string][]chan struct{}),
		cancels:    make(map[string]context.CancelFunc),
		workerLoad: NewWorkerLoad(),
	}
}

// SetOnComplete registers a callback fired when a task reaches a terminal state.
// Used by PipelineManager to advance pipeline stages.
func (c *Coordinator) SetOnComplete(fn func(ctx context.Context, taskID string, status model.TaskStatus)) {
	c.onComplete = fn
}

// Recover loads persisted tasks from a previous run. Tasks that were
// in-progress (queued, scheduled, running, cancelling) are re-enqueued.
func (c *Coordinator) Recover() error {
	tasks, err := c.persist.LoadAll()
	if err != nil {
		return fmt.Errorf("load tasks: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range tasks {
		c.tasks[t.ID] = t
		switch t.Status {
		case model.TaskQueued, model.TaskScheduled, model.TaskRunning, model.TaskCancelling:
			// Reset to queued so the worker picks them up again.
			t.Status = model.TaskQueued
			clearExecState(t)
			c.queue.Push(t)
			if err := c.persist.Save(t); err != nil {
				c.log.Error("failed to persist recovery reset", "id", t.ID, "err", err)
			}
		}
	}

	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))

	if len(tasks) > 0 {
		c.log.Info("recovered tasks", "count", len(tasks))
	}
	return nil
}

// Submit accepts a new task, resolves its references, and enqueues it.
func (c *Coordinator) Submit(ctx context.Context, task *model.Task) (*model.Task, error) {
	task.ID = uuid.New().String()
	task.Status = model.TaskQueued
	task.CreatedAt = time.Now()
	task.Metrics.QueuedAt = task.CreatedAt

	// Apply cluster defaults for unset per-task config.
	if task.Config.MaxRetries == 0 && c.defaults.MaxRetries > 0 {
		task.Config.MaxRetries = c.defaults.MaxRetries
	}
	if task.Config.Timeout == 0 && c.defaults.Timeout > 0 {
		task.Config.Timeout = model.Duration(c.defaults.Timeout)
	}

	// Resolve namespace keys to content hashes at submission time.
	if err := ResolveRefs(task, c.store); err != nil {
		return nil, fmt.Errorf("resolve refs: %w", err)
	}

	// Snapshot the task before it becomes visible to other goroutines.
	// Once stored in c.tasks or pushed to the queue, concurrent access
	// (Dequeue, Cancel, etc.) can mutate the live pointer.
	cp := *task

	// Persist before making the task visible. If persistence fails, roll
	// back the refcount increments from ResolveRefs and return an error
	// instead of silently accepting a task that would be lost on restart.
	if err := c.persist.Save(&cp); err != nil {
		ReleaseRefs(task, c.store)
		return nil, fmt.Errorf("persist task: %w", err)
	}

	c.mu.Lock()
	c.tasks[task.ID] = task
	c.mu.Unlock()

	c.queue.Push(task)
	metrics.TasksSubmitted.Inc()
	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))
	c.log.Info("task submitted", "id", cp.ID, "command", cp.Command)

	return &cp, nil
}

// Get returns a shallow copy of a task by ID or unambiguous ID prefix.
// The copy is safe to read outside the coordinator lock (e.g. for JSON
// serialization). Prefix matching requires at least 4 characters.
func (c *Coordinator) Get(id string) (*model.Task, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	resolved, err := c.resolveID(id)
	if err != nil {
		return nil, err
	}
	cp := *c.tasks[resolved]
	return &cp, nil
}

// List returns all tasks, optionally filtered by status.
// Returns shallow copies so callers cannot mutate coordinator state.
func (c *Coordinator) List(status *model.TaskStatus) []*model.Task {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []*model.Task
	for _, t := range c.tasks {
		if status == nil || t.Status == *status {
			cp := *t
			result = append(result, &cp)
		}
	}
	return result
}

// Cancel cancels a task. See Task Cancellation in the spec for state machine.
func (c *Coordinator) Cancel(id string) (*model.Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resolved, err := c.resolveID(id)
	if err != nil {
		return nil, err
	}
	t := c.tasks[resolved]
	id = resolved

	switch t.Status {
	case model.TaskQueued, model.TaskScheduled:
		t.Status = model.TaskCancelled
		t.Error = "cancelled by user"
		c.queue.Remove(id)
		metrics.TaskQueueDepth.Set(float64(c.queue.Len()))
		if t.Worker != "" {
			c.workerLoad.TaskFinished(t.Worker)
			c.workerLoad.ReleaseResources(t.Worker, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
		}
		if !t.RemoteOrigin {
			ReleaseRefs(t, c.store)
		}
		c.notifyWaiters(id)
	case model.TaskRunning:
		t.Status = model.TaskCancelling
		// Signal the worker to initiate graceful shutdown.
		c.cancelMu.Lock()
		if fn, ok := c.cancels[id]; ok {
			fn()
		}
		c.cancelMu.Unlock()
	case model.TaskCompleted, model.TaskFailed, model.TaskCancelled:
		// No-op.
	}

	if err := c.persist.Save(t); err != nil {
		c.log.Error("failed to persist cancel", "id", id, "err", err)
	}

	cp := *t
	return &cp, nil
}

// Wait blocks until the task reaches a terminal state or ctx is cancelled.
func (c *Coordinator) Wait(ctx context.Context, id string) (*model.Task, error) {
	// Hold mu.RLock while checking status AND registering the waiter to
	// prevent a race where Complete/Cancel fires between the two steps.
	// Lock ordering: mu → waitMu (same as Complete/Cancel path).
	c.mu.RLock()
	resolved, err := c.resolveID(id)
	if err != nil {
		c.mu.RUnlock()
		return nil, err
	}
	id = resolved
	t := c.tasks[id]
	if isTerminal(t.Status) {
		cp := *t
		c.mu.RUnlock()
		return &cp, nil
	}

	ch := make(chan struct{}, 1)
	c.waitMu.Lock()
	c.waiters[id] = append(c.waiters[id], ch)
	c.waitMu.Unlock()
	c.mu.RUnlock()

	select {
	case <-ch:
		c.mu.RLock()
		t = c.tasks[id]
		cp := *t
		c.mu.RUnlock()
		return &cp, nil
	case <-ctx.Done():
		// Clean up the abandoned waiter so it doesn't leak.
		c.removeWaiter(id, ch)
		return nil, ctx.Err()
	}
}

// removeWaiter removes a specific waiter channel from the waiters map.
func (c *Coordinator) removeWaiter(id string, ch chan struct{}) {
	c.waitMu.Lock()
	defer c.waitMu.Unlock()
	waiters := c.waiters[id]
	for i, w := range waiters {
		if w == ch {
			c.waiters[id] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(c.waiters[id]) == 0 {
		delete(c.waiters, id)
	}
}

// Dequeue returns the next schedulable task whose requirements are met by tags
// and whose constraints are satisfied by caps. Returns nil if the queue is
// empty, no task matches, or the node is draining.
func (c *Coordinator) Dequeue(tags []string, caps map[string]string) *model.Task {
	c.mu.RLock()
	if c.draining {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	t := c.queue.Pop(tags, caps)
	if t == nil {
		return nil
	}

	c.mu.Lock()
	t.Status = model.TaskScheduled
	c.mu.Unlock()

	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))
	return t
}

// Drain puts the coordinator into draining mode. New dequeues are blocked
// but in-flight tasks run to completion. Submissions still succeed (tasks
// queue but won't be picked up locally until drain is lifted).
func (c *Coordinator) Drain() {
	c.mu.Lock()
	c.draining = true
	c.mu.Unlock()
	c.log.Info("coordinator draining: no new tasks will be dequeued")
}

// Undrain takes the coordinator out of draining mode.
func (c *Coordinator) Undrain() {
	c.mu.Lock()
	c.draining = false
	c.mu.Unlock()
	c.log.Info("coordinator undrained: dequeue resumed")
}

// IsDraining returns whether the coordinator is in drain mode.
func (c *Coordinator) IsDraining() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.draining
}

// RunningCount returns the number of currently running tasks.
func (c *Coordinator) RunningCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	count := 0
	for _, t := range c.tasks {
		if t.Status == model.TaskRunning {
			count++
		}
	}
	return count
}

// QueueLen returns the number of tasks waiting in the queue.
func (c *Coordinator) QueueLen() int {
	return c.queue.Len()
}

// Complete records a task result from the worker.
func (c *Coordinator) Complete(id string, exitCode int, stdout, stderr, errMsg, outputRef string, outputBytes int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.tasks[id]
	if !ok {
		return fmt.Errorf("task not found: %s", id)
	}

	// If the task already reached a terminal state (e.g. cancelled while
	// dispatched to a remote worker), don't process the result again.
	// This prevents double ReleaseRefs and double waiter notification.
	if isTerminal(t.Status) {
		return nil
	}

	t.ExitCode = exitCode
	t.Stdout = stdout
	t.Stderr = stderr
	t.OutputRef = outputRef
	t.Metrics.CompletedAt = time.Now()
	t.Metrics.WallTime = model.Duration(t.Metrics.CompletedAt.Sub(t.Metrics.StartedAt))

	if t.Worker != "" {
		c.workerLoad.TaskFinished(t.Worker)
		c.workerLoad.ReleaseResources(t.Worker, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
	}
	t.Metrics.OutputBytes = outputBytes

	if t.Status == model.TaskCancelling || t.Status == model.TaskCancelled {
		// Task was being cancelled — don't retry, go straight to terminal.
		t.Status = model.TaskCancelled
		if t.Error == "" {
			t.Error = "cancelled by user"
		}
	} else if exitCode == 0 {
		t.Status = model.TaskCompleted
	} else {
		t.Attempt++
		if t.Attempt <= t.Config.MaxRetries {
			t.Status = model.TaskQueued
			clearExecState(t)
			c.queue.Push(t)
		} else {
			if c.defaults.DeadLetter {
				t.Status = model.TaskDeadLetter
			} else {
				t.Status = model.TaskFailed
			}
			if errMsg != "" {
				t.Error = errMsg
			} else {
				t.Error = fmt.Sprintf("exit code %d after %d attempts", exitCode, t.Attempt)
			}
		}
	}

	if err := c.persist.Save(t); err != nil {
		c.log.Error("failed to persist completion", "id", id, "err", err)
	}

	terminal := isTerminal(t.Status)
	finalStatus := t.Status
	if terminal {
		metrics.TasksCompleted.WithLabelValues(t.Status.String()).Inc()
		if t.Metrics.WallTime > 0 {
			metrics.TaskDuration.Observe(time.Duration(t.Metrics.WallTime).Seconds())
		}
		// Only release refs if this node incremented them (via Submit/ResolveRefs).
		// Tasks accepted via AcceptDispatch have RemoteOrigin=true and never
		// incremented local refcounts, so releasing would undercount.
		if !t.RemoteOrigin {
			ReleaseRefs(t, c.store)
		}
		c.notifyWaiters(id)
	}
	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))

	// Fire pipeline callback outside the lock to avoid deadlock.
	// Tracked by callbackWg so shutdown can wait for completion.
	if terminal && c.onComplete != nil {
		c.callbackWg.Add(1)
		go func() {
			defer c.callbackWg.Done()
			c.onComplete(context.Background(), id, finalStatus)
		}()
	}

	return nil
}

// CompleteRemote records the final result of a task that was executed on a
// remote worker. Unlike Complete, it does NOT apply retry logic — the remote
// worker already exhausted retries locally. The coordinator adopts the
// terminal status directly.
func (c *Coordinator) CompleteRemote(id string, exitCode int, stdout, stderr, errMsg, outputRef string, outputBytes int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.tasks[id]
	if !ok {
		return fmt.Errorf("task not found: %s", id)
	}

	if isTerminal(t.Status) {
		return nil
	}

	t.ExitCode = exitCode
	t.Stdout = stdout
	t.Stderr = stderr
	t.OutputRef = outputRef
	t.Metrics.CompletedAt = time.Now()
	t.Metrics.WallTime = model.Duration(t.Metrics.CompletedAt.Sub(t.Metrics.StartedAt))
	t.Metrics.OutputBytes = outputBytes

	if t.Worker != "" {
		c.workerLoad.TaskFinished(t.Worker)
		c.workerLoad.ReleaseResources(t.Worker, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
	}

	// Adopt terminal status directly — no retry logic.
	if t.Status == model.TaskCancelling || t.Status == model.TaskCancelled {
		t.Status = model.TaskCancelled
		if t.Error == "" {
			t.Error = "cancelled by user"
		}
	} else if exitCode == 0 {
		t.Status = model.TaskCompleted
	} else {
		if c.defaults.DeadLetter {
			t.Status = model.TaskDeadLetter
		} else {
			t.Status = model.TaskFailed
		}
		if errMsg != "" {
			t.Error = errMsg
		} else {
			t.Error = fmt.Sprintf("exit code %d (remote)", exitCode)
		}
	}

	if err := c.persist.Save(t); err != nil {
		c.log.Error("failed to persist remote completion", "id", id, "err", err)
	}

	finalStatus := t.Status
	metrics.TasksCompleted.WithLabelValues(t.Status.String()).Inc()
	if t.Metrics.WallTime > 0 {
		metrics.TaskDuration.Observe(time.Duration(t.Metrics.WallTime).Seconds())
	}
	ReleaseRefs(t, c.store)
	c.notifyWaiters(id)
	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))

	if c.onComplete != nil {
		c.callbackWg.Add(1)
		go func() {
			defer c.callbackWg.Done()
			c.onComplete(context.Background(), id, finalStatus)
		}()
	}

	return nil
}

// RegisterCancel stores a cancel function for a running task. The worker
// calls this before execution so the coordinator can cancel the context
// if a user requests task cancellation.
func (c *Coordinator) RegisterCancel(id string, cancel context.CancelFunc) {
	c.cancelMu.Lock()
	c.cancels[id] = cancel
	c.cancelMu.Unlock()
}

// UnregisterCancel removes a task's cancel function after execution completes.
func (c *Coordinator) UnregisterCancel(id string) {
	c.cancelMu.Lock()
	delete(c.cancels, id)
	c.cancelMu.Unlock()
}

// MarkRunning transitions a task to RUNNING state. Returns false if the task
// has already been cancelled (e.g. between Dequeue and MarkRunning), in which
// case the worker should skip execution.
func (c *Coordinator) MarkRunning(id, workerID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return false
	}
	// If the task was cancelled between dequeue and now, don't start it.
	if t.Status == model.TaskCancelled || t.Status == model.TaskCancelling {
		return false
	}
	t.Status = model.TaskRunning
	t.Worker = workerID
	t.Metrics.StartedAt = time.Now()
	c.workerLoad.TaskStarted(workerID)
	c.workerLoad.AllocResources(workerID, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
	if err := c.persist.Save(t); err != nil {
		c.log.Error("failed to persist running state", "id", id, "err", err)
	}
	return true
}

func (c *Coordinator) notifyWaiters(id string) {
	c.waitMu.Lock()
	defer c.waitMu.Unlock()
	for _, ch := range c.waiters[id] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	delete(c.waiters, id)
}

// RequeueByWorker finds all tasks assigned to the given worker that are still
// in-progress (RUNNING or SCHEDULED) and re-enqueues them. This is called when
// a node departs the cluster unexpectedly so its tasks can be retried elsewhere.
//
// Only tasks whose Worker field matches the departed node are requeued.
// Tasks in SCHEDULED state with Worker=="" (dequeued but not yet MarkRunning)
// are NOT requeued here — they are handled by Recover on restart — because
// we cannot attribute them to a specific departed node and requeuing them on
// any departure event would cause duplicate execution.
func (c *Coordinator) RequeueByWorker(workerID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	for _, t := range c.tasks {
		if t.Worker != workerID {
			continue
		}
		if t.Status != model.TaskRunning && t.Status != model.TaskScheduled {
			continue
		}
		c.log.Info("requeuing task from departed node", "task", t.ID, "worker", workerID)
		c.workerLoad.TaskFinished(workerID)
		c.workerLoad.ReleaseResources(workerID, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
		t.Status = model.TaskQueued
		clearExecState(t)
		c.queue.Push(t)
		metrics.TaskQueueDepth.Set(float64(c.queue.Len()))
		if err := c.persist.Save(t); err != nil {
			c.log.Error("failed to persist requeue", "id", t.ID, "err", err)
		}
		count++
	}
	return count
}

// RequeueTask requeues a single task by ID. Only tasks in SCHEDULED state
// are eligible — RUNNING tasks are left alone to avoid duplicate execution.
// Used by work stealing to surgically requeue one task at a time.
func (c *Coordinator) RequeueTask(taskID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.tasks[taskID]
	if !ok {
		return false
	}
	if t.Status != model.TaskScheduled {
		return false
	}

	c.log.Info("requeuing stolen task", "task", t.ID, "worker", t.Worker)
	if t.Worker != "" {
		c.workerLoad.TaskFinished(t.Worker)
		c.workerLoad.ReleaseResources(t.Worker, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
	}
	t.Status = model.TaskQueued
	clearExecState(t)
	c.queue.Push(t)
	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))
	if err := c.persist.Save(t); err != nil {
		c.log.Error("failed to persist requeue", "id", t.ID, "err", err)
	}
	return true
}

// AcceptDispatch accepts a task dispatched from a remote coordinator.
// Unlike Submit, it preserves the existing task ID and skips ResolveRefs
// (InputRefs/Artifacts are already content hashes resolved by the origin).
func (c *Coordinator) AcceptDispatch(ctx context.Context, task *model.Task) error {
	task.Status = model.TaskQueued
	task.CreatedAt = time.Now()
	task.Metrics.QueuedAt = task.CreatedAt

	// Apply cluster defaults for unset per-task config.
	if task.Config.MaxRetries == 0 && c.defaults.MaxRetries > 0 {
		task.Config.MaxRetries = c.defaults.MaxRetries
	}
	if task.Config.Timeout == 0 && c.defaults.Timeout > 0 {
		task.Config.Timeout = model.Duration(c.defaults.Timeout)
	}

	// Skip ResolveRefs: refs are already content hashes from the origin coordinator.
	// Skip refcount increments: the origin coordinator owns the refcounts.
	// Mark as remote so Complete skips ReleaseRefs on this node's store.
	task.RemoteOrigin = true

	cp := *task
	if err := c.persist.Save(&cp); err != nil {
		return fmt.Errorf("persist dispatched task: %w", err)
	}

	c.mu.Lock()
	c.tasks[task.ID] = task
	c.mu.Unlock()

	c.queue.Push(task)
	metrics.TasksSubmitted.Inc()
	metrics.TaskQueueDepth.Set(float64(c.queue.Len()))
	c.log.Info("dispatched task accepted", "id", task.ID, "command", task.Command)

	return nil
}

// MarkDispatched transitions a task to SCHEDULED state on the coordinator
// after it has been dispatched to a remote worker node. Records the worker
// ID so the task is attributed correctly. Returns false if the task was
// cancelled (or otherwise reached a terminal state) between queue pop and
// dispatch, in which case the status is NOT overwritten.
func (c *Coordinator) MarkDispatched(id, workerID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return false
	}
	if isTerminal(t.Status) || t.Status == model.TaskCancelling {
		return false
	}
	t.Status = model.TaskScheduled
	t.Worker = workerID
	t.Metrics.StartedAt = time.Now()
	c.workerLoad.TaskStarted(workerID)
	c.workerLoad.AllocResources(workerID, t.Resources.Memory, t.Resources.CPUCores, t.Resources.GPUs)
	if err := c.persist.Save(t); err != nil {
		c.log.Error("failed to persist dispatch state", "id", id, "err", err)
	}
	return true
}

// WaitForCallbacks blocks until all in-flight onComplete callback goroutines
// finish. Call this during shutdown before closing databases to prevent
// pipeline advancement from racing with database closure.
func (c *Coordinator) WaitForCallbacks() {
	c.callbackWg.Wait()
}

// WorkerLoad returns the load tracker for scheduling and observability.
func (c *Coordinator) WorkerLoad() *WorkerLoad {
	return c.workerLoad
}

// resolveID finds the full task ID from an exact match or unambiguous prefix.
// Caller must hold at least c.mu.RLock.
func (c *Coordinator) resolveID(id string) (string, error) {
	if _, ok := c.tasks[id]; ok {
		return id, nil
	}
	if len(id) >= 4 {
		found := ""
		for tid := range c.tasks {
			if len(tid) > len(id) && tid[:len(id)] == id {
				if found != "" {
					return "", fmt.Errorf("ambiguous task ID prefix %q", id)
				}
				found = tid
			}
		}
		if found != "" {
			return found, nil
		}
	}
	return "", fmt.Errorf("task not found: %s", id)
}

func isTerminal(s model.TaskStatus) bool {
	return s == model.TaskCompleted || s == model.TaskFailed || s == model.TaskCancelled || s == model.TaskDeadLetter
}

// clearExecState resets execution-related fields before re-enqueuing a task.
// This prevents stale worker, timing, output, and error data from a previous
// attempt from leaking into the next execution.
func clearExecState(t *model.Task) {
	t.Worker = ""
	t.ExitCode = 0
	t.Stdout = ""
	t.Stderr = ""
	t.Error = ""
	t.OutputRef = ""
	t.Metrics.StartedAt = time.Time{}
	t.Metrics.CompletedAt = time.Time{}
	t.Metrics.WallTime = 0
	t.Metrics.OutputBytes = 0
}
