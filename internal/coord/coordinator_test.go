package coord

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"go.etcd.io/bbolt"
)

// testSetup creates a coordinator backed by real store and persistence for
// integration testing. Temp directories are cleaned up automatically.
func testSetup(t *testing.T) (*Coordinator, *store.Store) {
	t.Helper()
	tmpDir := t.TempDir()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := store.DefaultTestConfig()
	s, err := store.New(storeCfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	tasksDB, err := bbolt.Open(filepath.Join(tmpDir, "tasks.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tasksDB.Close() })

	persist, err := NewPersist(tasksDB)
	if err != nil {
		t.Fatal(err)
	}

	defaults := TaskDefaults{MaxRetries: 2, Timeout: 5 * time.Minute}
	c := New(s, persist, defaults, log)
	return c, s
}

func TestCoordinator_SubmitAndGet(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{
		Command: []string{"echo", "hello"},
	}
	result, err := c.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if result.ID == "" {
		t.Fatal("expected task ID to be set")
	}
	if result.Status != model.TaskQueued {
		t.Fatalf("expected queued status, got %s", result.Status)
	}

	got, err := c.Get(result.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != result.ID {
		t.Fatalf("got ID %s, want %s", got.ID, result.ID)
	}
}

func TestCoordinator_ListReturnsCopies(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{Command: []string{"echo"}}
	submitted, _ := c.Submit(context.Background(), task)

	// List should return tasks whose mutation doesn't affect coordinator state.
	tasks := c.List(nil)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	// Mutate the returned task.
	tasks[0].Status = model.TaskFailed
	tasks[0].Error = "mutated"

	// Original in coordinator should be unaffected.
	original, _ := c.Get(submitted.ID)
	if original.Status == model.TaskFailed {
		t.Fatal("mutation of List result affected coordinator internal state")
	}
}

func TestCoordinator_CancelQueued(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{Command: []string{"echo"}}
	submitted, _ := c.Submit(context.Background(), task)

	cancelled, err := c.Cancel(submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != model.TaskCancelled {
		t.Fatalf("expected cancelled, got %s", cancelled.Status)
	}
}

func TestCoordinator_CompleteSuccess(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{Command: []string{"echo"}}
	submitted, _ := c.Submit(context.Background(), task)

	// Dequeue and mark running.
	dequeued := c.Dequeue(nil, nil, "")
	if dequeued == nil {
		t.Fatal("expected task from dequeue")
	}
	c.MarkRunning(submitted.ID, "worker-1")

	// Complete successfully.
	err := c.Complete(submitted.ID, 0, "output", "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := c.Get(submitted.ID)
	if got.Status != model.TaskCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
}

func TestCoordinator_CompleteFailureRetries(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{
		Command: []string{"false"},
		Config:  model.TaskConfig{MaxRetries: 1},
	}
	submitted, _ := c.Submit(context.Background(), task)

	// First attempt: dequeue, run, fail.
	c.Dequeue(nil, nil, "")
	c.MarkRunning(submitted.ID, "worker-1")
	c.Complete(submitted.ID, 1, "", "error", "", "", 0)

	// Task should be re-queued.
	got, _ := c.Get(submitted.ID)
	if got.Status != model.TaskQueued {
		t.Fatalf("expected re-queued, got %s", got.Status)
	}

	// Second attempt: dequeue, run, fail again.
	c.Dequeue(nil, nil, "")
	c.MarkRunning(submitted.ID, "worker-1")
	c.Complete(submitted.ID, 1, "", "error again", "", "", 0)

	// Should be failed now (retries exhausted).
	got, _ = c.Get(submitted.ID)
	if got.Status != model.TaskFailed {
		t.Fatalf("expected failed after retry exhaustion, got %s", got.Status)
	}
}

func TestCoordinator_DefaultsApplied(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{Command: []string{"echo"}}
	submitted, _ := c.Submit(context.Background(), task)

	if submitted.Config.MaxRetries != 2 {
		t.Fatalf("expected default max_retries=2, got %d", submitted.Config.MaxRetries)
	}
	if submitted.Config.Timeout.Duration() != 5*time.Minute {
		t.Fatalf("expected default timeout=5m, got %s", submitted.Config.Timeout.Duration())
	}
}

func TestCoordinator_RecoverReEnqueuesInProgress(t *testing.T) {
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := store.DefaultTestConfig()
	s, err := store.New(storeCfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}

	tasksDB, err := bbolt.Open(filepath.Join(tmpDir, "tasks.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	persist, err := NewPersist(tasksDB)
	if err != nil {
		t.Fatal(err)
	}
	defaults := TaskDefaults{MaxRetries: 2, Timeout: 5 * time.Minute}

	// First coordinator: submit tasks and put some in various states.
	c1 := New(s, persist, defaults, log)

	queued, _ := c1.Submit(context.Background(), &model.Task{Command: []string{"echo", "queued"}})
	running, _ := c1.Submit(context.Background(), &model.Task{Command: []string{"echo", "running"}})
	completed, _ := c1.Submit(context.Background(), &model.Task{Command: []string{"echo", "completed"}})

	// Move running task through dequeue → running.
	c1.Dequeue(nil, nil, "") // pops highest-seq first (both same priority), gets queued or running
	c1.Dequeue(nil, nil, "")
	c1.MarkRunning(running.ID, "worker-1")

	// Complete one task.
	c1.MarkRunning(completed.ID, "worker-1")
	c1.Complete(completed.ID, 0, "done", "", "", "", 0)

	// Verify states before "crash".
	qTask, _ := c1.Get(queued.ID)
	rTask, _ := c1.Get(running.ID)
	cTask, _ := c1.Get(completed.ID)

	// queued was dequeued so it's scheduled; running is running; completed is completed.
	if rTask.Status != model.TaskRunning {
		t.Fatalf("expected running, got %s", rTask.Status)
	}
	if cTask.Status != model.TaskCompleted {
		t.Fatalf("expected completed, got %s", cTask.Status)
	}
	_ = qTask // scheduled

	// Simulate crash: close the first coordinator (no cleanup), create new one.
	// The tasksDB is still open — in real crash, it would be reopened.
	c2 := New(s, persist, defaults, log)
	if err := c2.Recover(); err != nil {
		t.Fatal(err)
	}

	// After recovery:
	// - queued/scheduled/running tasks should be re-enqueued (status = queued)
	// - completed task should remain completed
	recovered, _ := c2.Get(queued.ID)
	if recovered.Status != model.TaskQueued {
		t.Fatalf("expected scheduled task re-enqueued as queued, got %s", recovered.Status)
	}

	recoveredRunning, _ := c2.Get(running.ID)
	if recoveredRunning.Status != model.TaskQueued {
		t.Fatalf("expected running task re-enqueued as queued, got %s", recoveredRunning.Status)
	}

	recoveredCompleted, _ := c2.Get(completed.ID)
	if recoveredCompleted.Status != model.TaskCompleted {
		t.Fatalf("expected completed task to stay completed, got %s", recoveredCompleted.Status)
	}

	// The re-enqueued tasks should be dequeueable.
	d1 := c2.Dequeue(nil, nil, "")
	d2 := c2.Dequeue(nil, nil, "")
	if d1 == nil || d2 == nil {
		t.Fatal("expected 2 dequeueable tasks after recovery")
	}
	d3 := c2.Dequeue(nil, nil, "")
	if d3 != nil {
		t.Fatal("expected no more tasks after dequeueing recovered ones")
	}

	// Cleanup.
	tasksDB.Close()
	s.Close()
}

func TestCoordinator_WaitReturnsImmediatelyForTerminal(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{Command: []string{"echo"}}
	submitted, _ := c.Submit(context.Background(), task)

	// Cancel it so it's terminal.
	c.Cancel(submitted.ID)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	got, err := c.Wait(ctx, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskCancelled {
		t.Fatalf("expected cancelled, got %s", got.Status)
	}
}

func TestCoordinator_RequeueByWorker(t *testing.T) {
	c, _ := testSetup(t)

	// Submit three tasks.
	t1, _ := c.Submit(context.Background(), &model.Task{Command: []string{"echo", "1"}})
	t2, _ := c.Submit(context.Background(), &model.Task{Command: []string{"echo", "2"}})
	t3, _ := c.Submit(context.Background(), &model.Task{Command: []string{"echo", "3"}})

	// Dequeue and mark all running on different workers.
	c.Dequeue(nil, nil, "")
	c.Dequeue(nil, nil, "")
	c.Dequeue(nil, nil, "")
	c.MarkRunning(t1.ID, "node-A")
	c.MarkRunning(t2.ID, "node-B")
	c.MarkRunning(t3.ID, "node-A")

	// node-A departs — its tasks should be requeued.
	count := c.RequeueByWorker("node-A")
	if count != 2 {
		t.Fatalf("expected 2 requeued tasks, got %d", count)
	}

	// t1 and t3 should be queued again.
	got1, _ := c.Get(t1.ID)
	if got1.Status != model.TaskQueued {
		t.Fatalf("t1: expected queued, got %s", got1.Status)
	}
	if got1.Worker != "" {
		t.Fatalf("t1: expected worker cleared, got %q", got1.Worker)
	}

	got3, _ := c.Get(t3.ID)
	if got3.Status != model.TaskQueued {
		t.Fatalf("t3: expected queued, got %s", got3.Status)
	}

	// t2 should still be running (different worker).
	got2, _ := c.Get(t2.ID)
	if got2.Status != model.TaskRunning {
		t.Fatalf("t2: expected running, got %s", got2.Status)
	}

	// The requeued tasks should be dequeueable.
	d1 := c.Dequeue(nil, nil, "")
	d2 := c.Dequeue(nil, nil, "")
	if d1 == nil || d2 == nil {
		t.Fatal("expected 2 dequeueable tasks after requeue")
	}
	d3 := c.Dequeue(nil, nil, "")
	if d3 != nil {
		t.Fatal("expected no more tasks (node-B's task is still running)")
	}
}

func TestCoordinator_RequeueByWorker_ScheduledNoWorker_NotRequeued(t *testing.T) {
	c, _ := testSetup(t)

	// Submit a task and dequeue it (becomes SCHEDULED) but don't call MarkRunning.
	// This simulates a node failing between Dequeue and MarkRunning.
	t1, _ := c.Submit(context.Background(), &model.Task{Command: []string{"echo", "orphan"}})
	dequeued := c.Dequeue(nil, nil, "")
	if dequeued == nil {
		t.Fatal("expected task from dequeue")
	}

	// At this point t1 is SCHEDULED with Worker == "".
	got, _ := c.Get(t1.ID)
	if got.Status != model.TaskScheduled {
		t.Fatalf("expected scheduled, got %s", got.Status)
	}
	if got.Worker != "" {
		t.Fatalf("expected empty worker, got %q", got.Worker)
	}

	// An unrelated node departure must NOT requeue orphaned scheduled tasks.
	// Doing so would cause duplicate execution when the original worker
	// (which is still alive) calls MarkRunning.
	count := c.RequeueByWorker("any-departed-node")
	if count != 0 {
		t.Fatalf("expected 0 requeued (orphan must not be attributed to random node), got %d", count)
	}

	got, _ = c.Get(t1.ID)
	if got.Status != model.TaskScheduled {
		t.Fatalf("expected still scheduled, got %s", got.Status)
	}
}

func TestCoordinator_Recover_RequeuesOrphanedScheduled(t *testing.T) {
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := store.DefaultTestConfig()
	s, err := store.New(storeCfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	tasksDB, err := bbolt.Open(filepath.Join(tmpDir, "tasks.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	persist, err := NewPersist(tasksDB)
	if err != nil {
		t.Fatal(err)
	}
	defaults := TaskDefaults{MaxRetries: 0, Timeout: 5 * time.Minute}

	// Submit and dequeue (SCHEDULED, Worker=="") — simulates node crash
	// between Dequeue and MarkRunning.
	c1 := New(s, persist, defaults, log)
	t1, _ := c1.Submit(context.Background(), &model.Task{Command: []string{"echo", "orphan"}})
	c1.Dequeue(nil, nil, "")

	got, _ := c1.Get(t1.ID)
	if got.Status != model.TaskScheduled {
		t.Fatalf("expected scheduled, got %s", got.Status)
	}

	// Simulate crash: create new coordinator and recover from DB.
	c2 := New(s, persist, defaults, log)
	if err := c2.Recover(); err != nil {
		t.Fatal(err)
	}

	// Recover should re-enqueue the orphaned scheduled task.
	recovered, _ := c2.Get(t1.ID)
	if recovered.Status != model.TaskQueued {
		t.Fatalf("expected orphaned scheduled task recovered as queued, got %s", recovered.Status)
	}

	d := c2.Dequeue(nil, nil, "")
	if d == nil {
		t.Fatal("expected orphaned task to be dequeueable after recovery")
	}

	tasksDB.Close()
	s.Close()
}

func TestCoordinator_Drain(t *testing.T) {
	c, _ := testSetup(t)

	// Submit a task.
	task := &model.Task{Command: []string{"echo"}}
	c.Submit(context.Background(), task)

	if c.IsDraining() {
		t.Fatal("should not be draining initially")
	}

	// Drain — dequeue should return nil even though queue has tasks.
	c.Drain()
	if !c.IsDraining() {
		t.Fatal("should be draining after Drain()")
	}

	got := c.Dequeue(nil, nil, "")
	if got != nil {
		t.Fatal("dequeue should return nil while draining")
	}

	// Queue should still have the task.
	if c.QueueLen() != 1 {
		t.Fatalf("expected 1 queued task, got %d", c.QueueLen())
	}

	// Undrain — dequeue should work again.
	c.Undrain()
	if c.IsDraining() {
		t.Fatal("should not be draining after Undrain()")
	}

	got = c.Dequeue(nil, nil, "")
	if got == nil {
		t.Fatal("dequeue should return task after undrain")
	}
}
