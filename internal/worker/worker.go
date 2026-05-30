package worker

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/metrics"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// Worker executes tasks in isolated workspaces.
type Worker struct {
	nodeID         string
	tags           []string
	caps           map[string]string
	store          *store.Store
	coord          *coord.Coordinator
	cfg            config.ComputeConfig
	dataDir        string // node data directory (for persistent envs)
	gpuAlloc       *GPUAllocator
	logBroadcaster *LogBroadcaster
	log            *slog.Logger
}

// New creates a Worker.
func New(nodeID string, tags []string, caps map[string]string, s *store.Store, c *coord.Coordinator, cfg config.ComputeConfig, dataDir string, log *slog.Logger) *Worker {
	return &Worker{
		nodeID:   nodeID,
		tags:     tags,
		caps:     caps,
		store:    s,
		coord:    c,
		cfg:      cfg,
		dataDir:  dataDir,
		gpuAlloc: NewGPUAllocator(caps),
		log:      log,
	}
}

// SetLogBroadcaster attaches a broadcaster for live log streaming.
// When set, task stdout/stderr are tee'd to subscribers in real time.
func (w *Worker) SetLogBroadcaster(lb *LogBroadcaster) {
	w.logBroadcaster = lb
}

// Run starts the worker loop, waiting for tasks from the coordinator.
// Respects compute.concurrency: launches up to cfg.Concurrency goroutines.
func (w *Worker) Run(ctx context.Context) {
	concurrency := w.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	ready := w.coord.Ready()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		default:
		}

		task := w.coord.Dequeue(w.tags, w.caps, w.nodeID)
		if task == nil {
			// No work available — wait for a signal or poll fallback.
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case <-ready:
				continue
			case <-time.After(2 * time.Second):
				// Fallback poll in case a signal was missed.
				continue
			}
		}

		// Acquire concurrency slot.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}

		wg.Add(1)
		go func(t *model.Task) {
			defer wg.Done()
			defer func() { <-sem }()
			w.execute(ctx, t)
		}(task)
	}
}

func (w *Worker) execute(ctx context.Context, task *model.Task) {
	timeout := task.Config.Timeout.Duration()
	if timeout == 0 {
		timeout = w.cfg.TaskTimeout
	}

	var execCtx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		execCtx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		execCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Register cancel BEFORE MarkRunning so a concurrent Cancel() that
	// sees RUNNING can always find the cancel function.
	w.coord.RegisterCancel(task.ID, cancel)
	defer w.coord.UnregisterCancel(task.ID)

	// MarkRunning checks if the task was cancelled between Dequeue and now.
	// If cancelled, skip execution.
	if !w.coord.MarkRunning(task.ID, w.nodeID) {
		w.log.Info("task cancelled before execution", "id", task.ID)
		return
	}

	// Allocate GPU devices if requested.
	var gpuDevices []int
	if task.Resources.GPUs > 0 && w.gpuAlloc != nil {
		var err error
		gpuDevices, err = w.gpuAlloc.Allocate(task.Resources.GPUs)
		if err != nil {
			w.log.Error("gpu allocation failed", "id", task.ID, "err", err)
			w.coord.Complete(task.ID, -1, "", "", err.Error(), "", 0)
			return
		}
		defer w.gpuAlloc.Release(gpuDevices)
		w.log.Info("gpu allocated", "id", task.ID, "devices", gpuDevices)
	}

	metrics.WorkersActive.Inc()
	result := Execute(execCtx, task, w.store, w.cfg, w.dataDir, gpuDevices, w.logBroadcaster, w.log)
	metrics.WorkersActive.Dec()

	if err := w.coord.Complete(
		task.ID,
		result.ExitCode,
		result.Stdout,
		result.Stderr,
		result.Error,
		result.OutputRef,
		result.OutputBytes,
	); err != nil {
		w.log.Error("failed to report task completion", "id", task.ID, "err", err)
	}

	if result.ExitCode == 0 {
		w.log.Info("task completed", "id", task.ID, "wall", result.WallTime)
	} else {
		w.log.Warn("task failed", "id", task.ID, "exit", result.ExitCode, "err", result.Error)
	}
}
