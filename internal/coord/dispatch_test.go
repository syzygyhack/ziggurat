package coord

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"go.etcd.io/bbolt"
)

// mockTransport records dispatch and cancel calls.
type mockTransport struct {
	dispatched []string
	cancelled  []string
}

func (m *mockTransport) DispatchTask(ctx context.Context, addr string, task *model.Task) (string, error) {
	m.dispatched = append(m.dispatched, task.ID)
	return task.ID, nil
}

func (m *mockTransport) FetchResult(ctx context.Context, addr string, taskID string) (*DispatchResult, error) {
	return nil, io.EOF
}

func (m *mockTransport) PullObject(ctx context.Context, addr string, hash string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockTransport) CancelTask(ctx context.Context, addr string, taskID string) error {
	m.cancelled = append(m.cancelled, taskID)
	return nil
}

// mockNodeRegistry returns a static list of nodes.
type mockNodeRegistry struct {
	nodes []*model.Node
}

func (m *mockNodeRegistry) List() []*model.Node {
	return m.nodes
}

func (m *mockNodeRegistry) Get(id string) (*model.Node, bool) {
	for _, n := range m.nodes {
		if n.ID == id {
			return n, true
		}
	}
	return nil, false
}

func testDispatcher(t *testing.T) (*Dispatcher, *Coordinator, *mockTransport) {
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

	defaults := TaskDefaults{MaxRetries: 0, Timeout: 5 * time.Minute}
	c := New(s, persist, defaults, log)

	transport := &mockTransport{}
	registry := &mockNodeRegistry{}

	d := NewDispatcher(c, registry, transport, nil, s,
		"local-node", nil, nil, true, log)

	return d, c, transport
}

func TestDispatcher_StealWork_RequeuesFromOverloaded(t *testing.T) {
	d, c, transport := testDispatcher(t)

	// Submit and manually dispatch 2 tasks to "worker-1".
	for i := 0; i < 2; i++ {
		task := &model.Task{
			Command: []string{"sleep", "60"},
		}
		submitted, err := c.Submit(context.Background(), task)
		if err != nil {
			t.Fatal(err)
		}
		// Pop from queue and mark dispatched.
		c.queue.PopAny()
		c.MarkDispatched(submitted.ID, "worker-1")

		d.dispatchedMu.Lock()
		d.dispatched[submitted.ID] = "worker-1:7101"
		d.dispatchedMu.Unlock()
	}

	// Submit and manually dispatch 1 task to "worker-2".
	task := &model.Task{
		Command: []string{"echo", "hi"},
	}
	submitted, err := c.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	c.queue.PopAny()
	c.MarkDispatched(submitted.ID, "worker-2")

	d.dispatchedMu.Lock()
	d.dispatched[submitted.ID] = "worker-2:7101"
	d.dispatchedMu.Unlock()

	// Capacity-aware overload: a worker is overloaded when its running count
	// exceeds its own concurrency limit, while another worker has spare room.
	// Give worker-1 a small limit and oversubscribe it; worker-2 keeps headroom.
	// After MarkDispatched: worker-1=2 (limit 1 → oversubscribed), worker-2=1.
	c.workerLoad.SetLimit("worker-1", 1)
	c.workerLoad.SetLimit("worker-2", 8)

	// Verify worker-1 is overloaded.
	overloaded := c.workerLoad.OverloadedWorkers()
	if len(overloaded) == 0 {
		t.Fatal("expected worker-1 to be overloaded")
	}

	// Run stealWork.
	d.stealWork(context.Background())

	// Cancel should have been sent for tasks on worker-1.
	if len(transport.cancelled) == 0 {
		t.Fatal("expected at least one cancel call for stolen tasks")
	}

	// Tasks should be requeued — queue should have tasks now.
	if c.QueueLen() == 0 {
		t.Fatal("expected requeued tasks in queue after work stealing")
	}
}

func TestDispatcher_StealWork_NoopWhenBalanced(t *testing.T) {
	d, c, transport := testDispatcher(t)

	// Submit and dispatch 1 task to each worker — balanced.
	for _, worker := range []string{"worker-1", "worker-2"} {
		task := &model.Task{Command: []string{"echo"}}
		submitted, _ := c.Submit(context.Background(), task)
		c.queue.PopAny()
		c.MarkDispatched(submitted.ID, worker)

		d.dispatchedMu.Lock()
		d.dispatched[submitted.ID] = worker + ":7101"
		d.dispatchedMu.Unlock()
	}

	d.stealWork(context.Background())

	// No cancels should fire — workers are balanced.
	if len(transport.cancelled) != 0 {
		t.Fatalf("expected 0 cancels when balanced, got %d", len(transport.cancelled))
	}
	if c.QueueLen() != 0 {
		t.Fatalf("expected 0 requeued when balanced, got %d", c.QueueLen())
	}
}
