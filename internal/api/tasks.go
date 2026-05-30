package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/syzygyhack/ziggurat/internal/model"
)

// submitTaskRequest is the JSON body for POST /api/v1/tasks.
type submitTaskRequest struct {
	Command   []string          `json:"command"`
	Env       map[string]string `json:"env,omitempty"`
	InputRefs map[string]string `json:"input_refs,omitempty"`
	Artifacts []string          `json:"artifacts,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
	Requires    []string               `json:"requires,omitempty"`
	Constraints []string               `json:"constraints,omitempty"`
	Resources   model.ResourceReq      `json:"resources,omitempty"`
	Image       string                 `json:"image,omitempty"`
	Environment *model.TaskEnvironment `json:"environment,omitempty"`
	Config      model.TaskConfig       `json:"config,omitempty"`
}

func (s *Server) submitTask(w http.ResponseWriter, r *http.Request) {
	if !s.requireCoordinator(w) {
		return
	}

	var req submitTaskRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlPlaneBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	if len(req.Command) == 0 {
		writeError(w, http.StatusBadRequest, "command is required")
		return
	}
	if req.Image != "" {
		writeError(w, http.StatusNotImplemented, "OCI image execution is not yet supported; omit the image field to run on the host OS")
		return
	}

	task := &model.Task{
		Command:     req.Command,
		Env:         req.Env,
		InputRefs:   req.InputRefs,
		Artifacts:   req.Artifacts,
		Params:      req.Params,
		Requires:    req.Requires,
		Constraints: req.Constraints,
		Resources:   req.Resources,
		Environment: req.Environment,
		Config:      req.Config,
	}

	result, err := s.coord.Submit(r.Context(), task)
	if err != nil {
		// Resolution failures (missing input/artifact keys) are client errors.
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "resolve refs") {
			status = http.StatusBadRequest
		}
		writeError(w, status, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) submitBatch(w http.ResponseWriter, r *http.Request) {
	if !s.requireCoordinator(w) {
		return
	}

	var reqs []submitTaskRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlPlaneBody)).Decode(&reqs); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if len(reqs) == 0 {
		writeError(w, http.StatusBadRequest, "batch must contain at least one task")
		return
	}

	// Validate all tasks before submitting any. Submission is best-effort
	// atomic: if a later Submit fails, earlier tasks are cancelled. However,
	// workers may dequeue and start tasks between individual Submit calls, so
	// a rollback can cancel tasks that have already begun executing. True
	// transactional semantics would require a held/pending state, which is
	// deferred to Phase 2.
	tasks := make([]*model.Task, len(reqs))
	for i, req := range reqs {
		if len(req.Command) == 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d]: command is required", i))
			return
		}
		if req.Image != "" {
			writeError(w, http.StatusNotImplemented, fmt.Sprintf("task[%d]: OCI image execution is not yet supported", i))
			return
		}
		tasks[i] = &model.Task{
			Command:     req.Command,
			Env:         req.Env,
			InputRefs:   req.InputRefs,
			Artifacts:   req.Artifacts,
			Params:      req.Params,
			Requires:    req.Requires,
			Constraints: req.Constraints,
			Resources:   req.Resources,
			Environment: req.Environment,
			Config:      req.Config,
		}
	}

	// Submit all tasks. On failure, cancel any already-submitted tasks.
	results := make([]*model.Task, 0, len(tasks))
	for i, task := range tasks {
		result, err := s.coord.Submit(r.Context(), task)
		if err != nil {
			// Rollback: cancel all previously submitted tasks.
			for _, prev := range results {
				s.coord.Cancel(prev.ID)
			}
			writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d]: %v", i, err))
			return
		}
		results = append(results, result)
	}

	writeJSON(w, http.StatusCreated, results)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	// Parse optional status filter.
	var statusFilter *model.TaskStatus
	if sq := r.URL.Query().Get("status"); sq != "" {
		var ts model.TaskStatus
		if err := ts.UnmarshalJSON([]byte(`"` + sq + `"`)); err != nil {
			writeError(w, http.StatusBadRequest, "invalid status filter: "+sq)
			return
		}
		statusFilter = &ts
	}

	tasks := s.coord.List(statusFilter)

	// Sort by creation time (newest first) for deterministic pagination.
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})

	// Apply offset and limit for pagination.
	offset := 0
	if oq := r.URL.Query().Get("offset"); oq != "" {
		if v, err := strconv.Atoi(oq); err == nil && v > 0 {
			offset = v
		}
	}
	if offset > len(tasks) {
		offset = len(tasks)
	}
	tasks = tasks[offset:]

	if lq := r.URL.Query().Get("limit"); lq != "" {
		if v, err := strconv.Atoi(lq); err == nil && v > 0 && v < len(tasks) {
			tasks = tasks[:v]
		}
	}

	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	task, err := s.coord.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	task, err := s.coord.Cancel(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) waitTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	task, err := s.coord.Wait(r.Context(), id)
	if err != nil {
		// Distinguish client disconnect / timeout from task-not-found.
		if r.Context().Err() != nil {
			writeError(w, http.StatusGatewayTimeout, "wait cancelled: "+err.Error())
			return
		}
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}
