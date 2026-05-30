package coord

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"go.etcd.io/bbolt"
)

func setupPipelineTest(t *testing.T) (*Coordinator, *PipelineManager, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.New(config.StorageConfig{}, dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}

	db, err := bbolt.Open(filepath.Join(dir, "tasks.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); s.Close() })

	persist, err := NewPersist(db)
	if err != nil {
		t.Fatal(err)
	}

	defaults := TaskDefaults{MaxRetries: 0, Timeout: 5 * time.Minute}
	c := New(s, persist, defaults, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	pm, err := NewPipelineManager(c, db, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}

	return c, pm, s
}

// waitForStage polls until the given stage reaches the expected condition or times out.
func waitForStage(t *testing.T, pm *PipelineManager, pipelineID string, stageIdx int, check func(*model.Stage) bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			got, _ := pm.GetPipeline(pipelineID)
			t.Fatalf("timed out waiting for stage[%d] condition; current status=%s taskID=%s",
				stageIdx, got.Stages[stageIdx].Status, got.Stages[stageIdx].TaskID)
		default:
			got, _ := pm.GetPipeline(pipelineID)
			if check(&got.Stages[stageIdx]) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// waitForPipelineStatus polls until the pipeline reaches the expected status or times out.
func waitForPipelineStatus(t *testing.T, pm *PipelineManager, pipelineID string, status model.PipelineStatus) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			got, _ := pm.GetPipeline(pipelineID)
			t.Fatalf("timed out waiting for pipeline status %s; current=%s", status, got.Status)
		default:
			got, _ := pm.GetPipeline(pipelineID)
			if got.Status == status {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestPipeline_LinearChain(t *testing.T) {
	c, pm, _ := setupPipelineTest(t)

	// Wire callback.
	c.SetOnComplete(func(ctx context.Context, taskID string, status model.TaskStatus) {
		pm.OnTaskComplete(ctx, taskID, status)
	})

	p := &model.Pipeline{
		Name: "test-linear",
		Stages: []model.Stage{
			{ID: "a", Command: []string{"echo", "hello"}},
			{ID: "b", Command: []string{"echo", "world"}, DependsOn: []string{"a"}},
		},
	}

	result, err := pm.SubmitPipeline(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != model.PipelineRunning {
		t.Fatalf("expected running, got %s", result.Status)
	}

	// Stage A should be scheduled, stage B should still be queued.
	got, _ := pm.GetPipeline(result.ID)
	if got.Stages[0].TaskID == "" {
		t.Fatal("stage A should have a task ID")
	}
	if got.Stages[1].TaskID != "" {
		t.Fatal("stage B should not be scheduled yet")
	}

	// Complete stage A's task.
	c.MarkRunning(got.Stages[0].TaskID, "worker-1")
	c.Complete(got.Stages[0].TaskID, 0, "hello\n", "", "", "", 0)

	// Wait for stage B to be scheduled by the async callback.
	waitForStage(t, pm, result.ID, 1, func(s *model.Stage) bool {
		return s.TaskID != ""
	})

	// Complete stage B.
	got, _ = pm.GetPipeline(result.ID)
	c.MarkRunning(got.Stages[1].TaskID, "worker-1")
	c.Complete(got.Stages[1].TaskID, 0, "world\n", "", "", "", 0)

	waitForPipelineStatus(t, pm, result.ID, model.PipelineCompleted)
}

func TestPipeline_FailureCancelsDownstream(t *testing.T) {
	c, pm, _ := setupPipelineTest(t)
	c.SetOnComplete(func(ctx context.Context, taskID string, status model.TaskStatus) {
		pm.OnTaskComplete(ctx, taskID, status)
	})

	p := &model.Pipeline{
		Name: "test-failure",
		Stages: []model.Stage{
			{ID: "a", Command: []string{"false"}},
			{ID: "b", Command: []string{"echo", "never"}, DependsOn: []string{"a"}},
		},
	}

	result, err := pm.SubmitPipeline(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := pm.GetPipeline(result.ID)
	taskA := got.Stages[0].TaskID

	// Fail stage A.
	c.MarkRunning(taskA, "worker-1")
	c.Complete(taskA, 1, "", "error\n", "exit 1", "", 0)

	waitForPipelineStatus(t, pm, result.ID, model.PipelineFailed)

	got, _ = pm.GetPipeline(result.ID)
	if got.Stages[1].Status != model.TaskCancelled {
		t.Fatalf("expected stage B cancelled, got %s", got.Stages[1].Status)
	}
}

func TestPipeline_Retry(t *testing.T) {
	c, pm, _ := setupPipelineTest(t)
	c.SetOnComplete(func(ctx context.Context, taskID string, status model.TaskStatus) {
		pm.OnTaskComplete(ctx, taskID, status)
	})

	p := &model.Pipeline{
		Name: "test-retry",
		Stages: []model.Stage{
			{ID: "a", Command: []string{"echo", "ok"}},
			{ID: "b", Command: []string{"false"}, DependsOn: []string{"a"}},
		},
	}

	result, err := pm.SubmitPipeline(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := pm.GetPipeline(result.ID)

	// Complete A, then wait for B to be scheduled.
	c.MarkRunning(got.Stages[0].TaskID, "w1")
	c.Complete(got.Stages[0].TaskID, 0, "ok\n", "", "", "", 0)

	waitForStage(t, pm, result.ID, 1, func(s *model.Stage) bool {
		return s.TaskID != ""
	})

	// Fail B.
	got, _ = pm.GetPipeline(result.ID)
	c.MarkRunning(got.Stages[1].TaskID, "w1")
	c.Complete(got.Stages[1].TaskID, 1, "", "", "exit 1", "", 0)

	waitForPipelineStatus(t, pm, result.ID, model.PipelineFailed)

	// Retry: A should be skipped (completed), B should be re-scheduled.
	retried, err := pm.RetryPipeline(context.Background(), result.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != model.PipelineRunning {
		t.Fatalf("expected running after retry, got %s", retried.Status)
	}
	if retried.Stages[0].Status != model.TaskCompleted {
		t.Fatalf("stage A should remain completed, got %s", retried.Stages[0].Status)
	}
	if retried.Stages[1].TaskID == "" {
		t.Fatal("stage B should have a new task ID after retry")
	}
}

func TestPipeline_ConstraintsAndImagePropagation(t *testing.T) {
	c, pm, _ := setupPipelineTest(t)
	c.SetOnComplete(func(ctx context.Context, taskID string, status model.TaskStatus) {
		pm.OnTaskComplete(ctx, taskID, status)
	})

	p := &model.Pipeline{
		Name: "test-constraints",
		Stages: []model.Stage{
			{
				ID:          "a",
				Command:     []string{"echo", "gpu"},
				Constraints: []string{"gpu == nvidia"},
				Image:       "cuda:12.0",
			},
		},
	}

	result, err := pm.SubmitPipeline(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := pm.GetPipeline(result.ID)
	taskID := got.Stages[0].TaskID
	if taskID == "" {
		t.Fatal("stage A should have a task ID")
	}

	// Verify the submitted task has the constraints and image from the stage.
	task, err := c.Get(taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(task.Constraints) != 1 || task.Constraints[0] != "gpu == nvidia" {
		t.Fatalf("expected constraints [gpu == nvidia], got %v", task.Constraints)
	}
	if task.Image != "cuda:12.0" {
		t.Fatalf("expected image cuda:12.0, got %s", task.Image)
	}
}

func TestPipeline_PrefixResolution(t *testing.T) {
	_, pm, _ := setupPipelineTest(t)

	p := &model.Pipeline{
		Name:   "test-prefix",
		Stages: []model.Stage{{ID: "a", Command: []string{"echo"}}},
	}

	result, err := pm.SubmitPipeline(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}

	// Full ID works.
	got, err := pm.GetPipeline(result.ID)
	if err != nil {
		t.Fatalf("full ID lookup failed: %v", err)
	}
	if got.ID != result.ID {
		t.Fatalf("expected %s, got %s", result.ID, got.ID)
	}

	// 8-char prefix works (standard shortID length).
	prefix := result.ID[:8]
	got, err = pm.GetPipeline(prefix)
	if err != nil {
		t.Fatalf("prefix lookup failed: %v", err)
	}
	if got.ID != result.ID {
		t.Fatalf("expected %s, got %s", result.ID, got.ID)
	}

	// Cancel and retry also support prefix.
	_, err = pm.CancelPipeline(prefix)
	if err != nil {
		t.Fatalf("cancel by prefix failed: %v", err)
	}

	// Too short prefix fails.
	_, err = pm.GetPipeline("ab")
	if err == nil {
		t.Fatal("expected error for short prefix")
	}
}

func TestPipeline_CycleDetection(t *testing.T) {
	_, pm, _ := setupPipelineTest(t)

	p := &model.Pipeline{
		Name: "cycle",
		Stages: []model.Stage{
			{ID: "a", Command: []string{"echo"}, DependsOn: []string{"b"}},
			{ID: "b", Command: []string{"echo"}, DependsOn: []string{"a"}},
		},
	}

	_, err := pm.SubmitPipeline(context.Background(), p)
	if err == nil {
		t.Fatal("expected error for cyclic pipeline")
	}
}
