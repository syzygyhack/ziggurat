package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// localNodeInfo returns a richer self-description for single-node mode.
func (s *Server) localNodeInfo() map[string]any {
	stats := s.store.Stats()
	role := s.role
	if role == "" {
		role = "hybrid"
	}
	return map[string]any{
		"id":             "local",
		"name":           "local",
		"role":           role,
		"status":         s.clusterStatusLabel(),
		"tasks_running":  s.coord.RunningCount(),
		"tasks_queued":   s.coord.QueueLen(),
		"storage_used":   stats.UsedBytes,
		"storage_objects": stats.Objects,
		"uptime_seconds": int(time.Since(s.startTime).Seconds()),
	}
}

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	if s.nodes == nil {
		writeJSON(w, http.StatusOK, []map[string]any{s.localNodeInfo()})
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

	// Support prefix matching: find the first node whose ID starts with id.
	for _, n := range s.nodes.List() {
		if n.ID == id || (len(id) >= 4 && len(n.ID) > len(id) && n.ID[:len(id)] == id) {
			writeJSON(w, http.StatusOK, n)
			return
		}
	}
	writeError(w, http.StatusNotFound, "node not found: "+id)
}

func (s *Server) drain(w http.ResponseWriter, r *http.Request) {
	s.coord.Drain()

	// Trigger shard migration in the background if configured.
	if s.onDrain != nil {
		go s.onDrain()
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
