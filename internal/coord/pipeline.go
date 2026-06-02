package coord

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

var bucketPipelines = []byte("pipelines")

// PipelineManager manages pipeline lifecycle alongside the Coordinator.
type PipelineManager struct {
	coord *Coordinator
	db    *bbolt.DB
	log   interface{ Info(string, ...any) }

	mu        sync.RWMutex
	pipelines map[string]*model.Pipeline
}

// NewPipelineManager creates a PipelineManager.
func NewPipelineManager(c *Coordinator, db *bbolt.DB, log interface{ Info(string, ...any) }) (*PipelineManager, error) {
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketPipelines)
		return err
	})
	if err != nil {
		return nil, err
	}

	pm := &PipelineManager{
		coord:     c,
		db:        db,
		log:       log,
		pipelines: make(map[string]*model.Pipeline),
	}

	// Recover persisted pipelines.
	db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPipelines)
		return b.ForEach(func(k, v []byte) error {
			var p model.Pipeline
			if err := json.Unmarshal(v, &p); err != nil {
				return nil // skip corrupt entries
			}
			pm.pipelines[p.ID] = &p
			return nil
		})
	})

	return pm, nil
}

// RecoverPipelines reconciles recovered pipelines with coordinator task state
// and reschedules ready stages. Must be called after Coordinator.Recover() so
// task states are up-to-date.
func (pm *PipelineManager) RecoverPipelines(ctx context.Context) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, p := range pm.pipelines {
		if p.Status != model.PipelineRunning {
			continue
		}

		// Reconcile each stage's status with the coordinator's task state.
		for i := range p.Stages {
			s := &p.Stages[i]
			if s.TaskID == "" || s.IsTerminal() {
				continue
			}
			t, err := pm.coord.Get(s.TaskID)
			if err != nil {
				// Task was lost (not recovered). Mark stage failed so
				// downstream cancellation and retry logic can run.
				s.Status = model.TaskFailed
				s.Error = "task lost during restart"
				pm.cancelDownstream(p, s.ID)
				continue
			}
			// Sync stage status with the recovered task.
			s.Status = t.Status
		}

		// Schedule any stages that are now ready (deps complete, not
		// yet scheduled). This handles the crash window between
		// SubmitPipeline persisting and scheduleReady executing.
		pm.scheduleReadyLocked(ctx, p)
		pm.updatePipelineStatus(p)
		pm.persist(p)
	}
}

// SubmitPipeline validates and starts a pipeline.
func (pm *PipelineManager) SubmitPipeline(ctx context.Context, p *model.Pipeline) (*model.Pipeline, error) {
	p.ID = uuid.New().String()
	p.Status = model.PipelineRunning
	p.CreatedAt = time.Now()

	// Validate: check for duplicate stage IDs and missing depends_on references.
	stageIDs := make(map[string]bool)
	for i := range p.Stages {
		s := &p.Stages[i]
		if s.ID == "" {
			return nil, fmt.Errorf("stage[%d]: id is required", i)
		}
		if stageIDs[s.ID] {
			return nil, fmt.Errorf("duplicate stage id: %s", s.ID)
		}
		stageIDs[s.ID] = true
		if len(s.Command) == 0 {
			return nil, fmt.Errorf("stage[%d] (%s): command is required", i, s.ID)
		}
		s.Status = model.TaskQueued
	}
	for _, s := range p.Stages {
		for _, dep := range s.DependsOn {
			if !stageIDs[dep] {
				return nil, fmt.Errorf("stage %s: depends_on references unknown stage %s", s.ID, dep)
			}
		}
	}

	// Detect cycles via topological sort.
	if err := validateDAG(p.Stages); err != nil {
		return nil, err
	}

	// Persist before inserting into the in-memory map so a failed write
	// doesn't leave a phantom pipeline in memory.
	if err := pm.persist(p); err != nil {
		return nil, fmt.Errorf("persist pipeline: %w", err)
	}

	// Hold the lock across insert, scheduling, and persist so that an
	// OnTaskComplete callback (which also holds pm.mu) cannot modify and
	// persist the pipeline between scheduleReady and our persist call.
	pm.mu.Lock()
	pm.pipelines[p.ID] = p
	pm.scheduleReadyLocked(ctx, p)
	pm.persist(p)
	pm.mu.Unlock()

	return p, nil
}

// GetPipeline returns a pipeline by ID or unambiguous prefix (min 4 chars).
func (pm *PipelineManager) GetPipeline(id string) (*model.Pipeline, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	resolved, err := pm.resolveID(id)
	if err != nil {
		return nil, err
	}
	return deepCopyPipeline(pm.pipelines[resolved]), nil
}

// ListPipelines returns all pipelines.
func (pm *PipelineManager) ListPipelines() []*model.Pipeline {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	var result []*model.Pipeline
	for _, p := range pm.pipelines {
		result = append(result, deepCopyPipeline(p))
	}
	return result
}

// OnTaskComplete is called by the coordinator when a task completes.
// It advances the pipeline if the task belongs to one.
func (pm *PipelineManager) OnTaskComplete(ctx context.Context, taskID string, status model.TaskStatus) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	p, stage := pm.findStageByTask(taskID)
	if p == nil {
		return // task doesn't belong to a pipeline
	}

	stage.Status = status
	if status == model.TaskFailed || status == model.TaskDeadLetter {
		// Look up error from the task.
		if t, err := pm.coord.Get(taskID); err == nil {
			stage.Error = t.Error
		}
		// Cancel downstream stages.
		pm.cancelDownstream(p, stage.ID)
		pm.updatePipelineStatus(p)
	} else if status == model.TaskCompleted {
		// Resolve output refs and schedule dependent stages.
		pm.scheduleReadyLocked(ctx, p)
		pm.updatePipelineStatus(p)
	} else if status == model.TaskCancelled {
		pm.updatePipelineStatus(p)
	}

	pm.persist(p)
}

// CancelPipeline cancels all non-terminal stages.
func (pm *PipelineManager) CancelPipeline(id string) (*model.Pipeline, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	resolved, err := pm.resolveID(id)
	if err != nil {
		return nil, err
	}
	p := pm.pipelines[resolved]

	for i := range p.Stages {
		s := &p.Stages[i]
		if s.IsTerminal() {
			continue
		}
		if s.TaskID != "" {
			pm.coord.Cancel(s.TaskID)
		}
		s.Status = model.TaskCancelled
	}
	p.Status = model.PipelineCancelled
	pm.persist(p)

	return deepCopyPipeline(p), nil
}

// RetryPipeline retries a failed pipeline from the first failed stage.
// Completed stages are skipped — their outputs are already in storage.
func (pm *PipelineManager) RetryPipeline(ctx context.Context, id string) (*model.Pipeline, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	resolved, err := pm.resolveID(id)
	if err != nil {
		return nil, err
	}
	p := pm.pipelines[resolved]
	if p.Status != model.PipelineFailed {
		return nil, fmt.Errorf("can only retry failed pipelines, current status: %s", p.Status)
	}

	// Reset failed and cancelled stages to queued.
	for i := range p.Stages {
		s := &p.Stages[i]
		if s.Status == model.TaskFailed || s.Status == model.TaskDeadLetter || s.Status == model.TaskCancelled {
			s.Status = model.TaskQueued
			s.TaskID = ""
			s.Error = ""
		}
	}
	p.Status = model.PipelineRunning
	p.Error = ""

	pm.scheduleReadyLocked(ctx, p)
	pm.persist(p)

	return deepCopyPipeline(p), nil
}

// scheduleReadyLocked schedules ready stages. Caller must hold pm.mu.
func (pm *PipelineManager) scheduleReadyLocked(ctx context.Context, p *model.Pipeline) {
	hadSubmitFailure := false
	for i := range p.Stages {
		s := &p.Stages[i]
		if s.Status != model.TaskQueued || s.TaskID != "" {
			continue
		}
		if !pm.depsComplete(p, s) {
			continue
		}

		// Resolve $stage.output references in input_refs.
		resolved := pm.resolveOutputRefs(p, s)

		task := &model.Task{
			Command:     s.Command,
			Artifacts:   s.Artifacts,
			InputRefs:   resolved,
			Params:      s.Params,
			Requires:    s.Requires,
			Constraints: s.Constraints,
			Image:       s.Image,
			Config:      s.Config,
		}

		submitted, err := pm.coord.Submit(ctx, task)
		if err != nil {
			s.Status = model.TaskFailed
			s.Error = fmt.Sprintf("submit failed: %v", err)
			pm.cancelDownstream(p, s.ID)
			hadSubmitFailure = true
			continue
		}
		s.TaskID = submitted.ID
		s.Status = model.TaskScheduled
		pm.log.Info("pipeline stage scheduled", "pipeline", p.ID, "stage", s.ID, "task", submitted.ID)
	}

	// If any stage failed during submission, recompute pipeline status.
	// This catches the case where a root stage fails synchronously, which
	// would otherwise leave the pipeline stuck in "running" forever.
	if hadSubmitFailure {
		pm.updatePipelineStatus(p)
	}
}

// depsComplete returns true if all stages this stage depends on are completed.
func (pm *PipelineManager) depsComplete(p *model.Pipeline, s *model.Stage) bool {
	for _, dep := range s.DependsOn {
		for _, ds := range p.Stages {
			if ds.ID == dep && ds.Status != model.TaskCompleted {
				return false
			}
		}
	}
	return true
}

// resolveOutputRefs replaces "$stage_id.output" references with actual output refs.
func (pm *PipelineManager) resolveOutputRefs(p *model.Pipeline, s *model.Stage) map[string]string {
	if len(s.InputRefs) == 0 {
		return s.InputRefs
	}

	resolved := make(map[string]string, len(s.InputRefs))
	for k, v := range s.InputRefs {
		if strings.HasPrefix(v, "$") && strings.HasSuffix(v, ".output") {
			stageID := v[1 : len(v)-7] // strip "$" and ".output"
			for _, ds := range p.Stages {
				if ds.ID == stageID && ds.TaskID != "" {
					if t, err := pm.coord.Get(ds.TaskID); err == nil && t.OutputRef != "" {
						// Forward the dependency's output by its namespace key
						// ("output/<taskID>", where task outputs are stored), NOT
						// the raw content hash: the dependent stage is submitted
						// through Submit→ResolveRefs, which resolves namespace
						// keys. Passing a hash here fails ("namespace key not
						// found").
						resolved[k] = fmt.Sprintf("output/%s", ds.TaskID)
						continue
					}
				}
			}
			// If we couldn't resolve, keep original (will fail at task execution).
			if _, ok := resolved[k]; !ok {
				resolved[k] = v
			}
		} else {
			resolved[k] = v
		}
	}
	return resolved
}

// cancelDownstream cancels all stages that transitively depend on the given stage.
func (pm *PipelineManager) cancelDownstream(p *model.Pipeline, failedStageID string) {
	// Find all transitive dependents.
	cancelled := map[string]bool{failedStageID: true}
	changed := true
	for changed {
		changed = false
		for i := range p.Stages {
			s := &p.Stages[i]
			if cancelled[s.ID] && s.ID != failedStageID {
				continue
			}
			for _, dep := range s.DependsOn {
				if cancelled[dep] && !cancelled[s.ID] {
					if !s.IsTerminal() {
						s.Status = model.TaskCancelled
						s.Error = fmt.Sprintf("upstream stage %s failed", failedStageID)
						if s.TaskID != "" {
							pm.coord.Cancel(s.TaskID)
						}
					}
					cancelled[s.ID] = true
					changed = true
				}
			}
		}
	}
}

// updatePipelineStatus derives pipeline status from stage states.
func (pm *PipelineManager) updatePipelineStatus(p *model.Pipeline) {
	allTerminal := true
	anyFailed := false
	anyCancelled := false

	for _, s := range p.Stages {
		if !s.IsTerminal() {
			allTerminal = false
		}
		if s.Status == model.TaskFailed || s.Status == model.TaskDeadLetter {
			anyFailed = true
		}
		if s.Status == model.TaskCancelled {
			anyCancelled = true
		}
	}

	if !allTerminal {
		return // still running
	}

	if anyFailed {
		p.Status = model.PipelineFailed
	} else if anyCancelled {
		p.Status = model.PipelineCancelled
	} else {
		p.Status = model.PipelineCompleted
	}
}

func (pm *PipelineManager) findStageByTask(taskID string) (*model.Pipeline, *model.Stage) {
	for _, p := range pm.pipelines {
		for i := range p.Stages {
			if p.Stages[i].TaskID == taskID {
				return p, &p.Stages[i]
			}
		}
	}
	return nil, nil
}

// resolveID finds the full pipeline ID from an exact match or unambiguous
// prefix (minimum 4 characters). Caller must hold at least pm.mu.RLock.
func (pm *PipelineManager) resolveID(id string) (string, error) {
	if _, ok := pm.pipelines[id]; ok {
		return id, nil
	}
	if len(id) >= 4 {
		found := ""
		for pid := range pm.pipelines {
			if len(pid) > len(id) && pid[:len(id)] == id {
				if found != "" {
					return "", fmt.Errorf("ambiguous pipeline ID prefix %q", id)
				}
				found = pid
			}
		}
		if found != "" {
			return found, nil
		}
	}
	return "", fmt.Errorf("pipeline not found: %s", id)
}

func (pm *PipelineManager) persist(p *model.Pipeline) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return pm.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketPipelines).Put([]byte(p.ID), data)
	})
}

// deepCopyPipeline returns an independent copy of a pipeline, including
// all mutable slice and map fields within each stage.
func deepCopyPipeline(p *model.Pipeline) *model.Pipeline {
	cp := *p
	cp.Stages = make([]model.Stage, len(p.Stages))
	for i, s := range p.Stages {
		cp.Stages[i] = s
		if s.Command != nil {
			cp.Stages[i].Command = make([]string, len(s.Command))
			copy(cp.Stages[i].Command, s.Command)
		}
		if s.Artifacts != nil {
			cp.Stages[i].Artifacts = make([]string, len(s.Artifacts))
			copy(cp.Stages[i].Artifacts, s.Artifacts)
		}
		if s.InputRefs != nil {
			cp.Stages[i].InputRefs = make(map[string]string, len(s.InputRefs))
			for k, v := range s.InputRefs {
				cp.Stages[i].InputRefs[k] = v
			}
		}
		if s.Params != nil {
			cp.Stages[i].Params = make(map[string]string, len(s.Params))
			for k, v := range s.Params {
				cp.Stages[i].Params[k] = v
			}
		}
		if s.Requires != nil {
			cp.Stages[i].Requires = make([]string, len(s.Requires))
			copy(cp.Stages[i].Requires, s.Requires)
		}
		if s.Constraints != nil {
			cp.Stages[i].Constraints = make([]string, len(s.Constraints))
			copy(cp.Stages[i].Constraints, s.Constraints)
		}
		if s.DependsOn != nil {
			cp.Stages[i].DependsOn = make([]string, len(s.DependsOn))
			copy(cp.Stages[i].DependsOn, s.DependsOn)
		}
	}
	return &cp
}

// validateDAG checks for cycles using Kahn's algorithm.
func validateDAG(stages []model.Stage) error {
	inDeg := make(map[string]int)
	adj := make(map[string][]string)
	for _, s := range stages {
		if _, ok := inDeg[s.ID]; !ok {
			inDeg[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.ID)
			inDeg[s.ID]++
		}
	}

	var queue []string
	for id, deg := range inDeg {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[node] {
			inDeg[next]--
			if inDeg[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if visited != len(stages) {
		return fmt.Errorf("pipeline contains a cycle")
	}
	return nil
}
