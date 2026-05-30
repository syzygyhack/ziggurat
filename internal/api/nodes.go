package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/syzygyhack/ziggurat/internal/model"
)

// localNodeInfo returns a model.Node-shaped description for single-node mode.
// Uses the same schema as cluster mode so CLI consumers get a consistent shape.
func (s *Server) localNodeInfo() *model.Node {
	stats := s.store.Stats()
	role := model.RoleHybrid
	switch s.role {
	case "coordinator":
		role = model.RoleCoordinator
	case "worker":
		role = model.RoleWorker
	}
	return &model.Node{
		ID:       "local",
		Name:     "local",
		Role:     role,
		JoinedAt: s.startTime,
		LastSeen: time.Now(),
		Load: model.LoadInfo{
			TasksRunning: s.coord.RunningCount(),
			TasksQueued:  s.coord.QueueLen(),
		},
		Storage: model.StorageInfo{
			Used:     stats.UsedBytes,
			Objects:  stats.Objects,
			Capacity: stats.Capacity,
		},
	}
}

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	if s.nodes == nil {
		writeJSON(w, http.StatusOK, []*model.Node{s.localNodeInfo()})
		return
	}
	writeJSON(w, http.StatusOK, s.nodes.List())
}

func (s *Server) getNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if s.nodes == nil {
		if id == "local" {
			writeJSON(w, http.StatusOK, s.localNodeInfo())
			return
		}
		writeError(w, http.StatusNotFound, "node not found: "+id)
		return
	}

	// Support prefix matching with ambiguity detection.
	nodes := s.nodes.List()
	for _, n := range nodes {
		if n.ID == id {
			writeJSON(w, http.StatusOK, n)
			return
		}
	}
	if len(id) >= 4 {
		var match *model.Node
		for _, n := range nodes {
			if len(n.ID) > len(id) && n.ID[:len(id)] == id {
				if match != nil {
					writeError(w, http.StatusBadRequest, "ambiguous node ID prefix: "+id)
					return
				}
				match = n
			}
		}
		if match != nil {
			writeJSON(w, http.StatusOK, match)
			return
		}
	}
	writeError(w, http.StatusNotFound, "node not found: "+id)
}

func (s *Server) drain(w http.ResponseWriter, r *http.Request) {
	s.coord.Drain()

	// Trigger shard migration in the background if configured.
	// onDrain launches its own tracked goroutine internally.
	if s.onDrain != nil {
		s.onDrain()
	}

	stats := s.store.Stats()
	resp := map[string]any{
		"status":          "draining",
		"tasks_running":   s.coord.RunningCount(),
		"tasks_queued":    s.coord.QueueLen(),
		"storage_objects": stats.Objects,
		"message":         "shard migration started in background; poll /api/v1/health to track progress",
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	s.coord.Undrain()

	resp := map[string]any{
		"status":        s.clusterStatusLabel(),
		"tasks_running": s.coord.RunningCount(),
		"tasks_queued":  s.coord.QueueLen(),
	}
	writeJSON(w, http.StatusOK, resp)
}
