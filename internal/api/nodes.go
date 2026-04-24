package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	if s.nodes == nil {
		// Single-node mode: return a minimal self-description.
		writeJSON(w, http.StatusOK, []map[string]any{
			{"id": "local", "name": "local", "status": "healthy"},
		})
		return
	}
	writeJSON(w, http.StatusOK, s.nodes.List())
}

func (s *Server) getNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if s.nodes == nil {
		if id == "local" {
			writeJSON(w, http.StatusOK, map[string]any{
				"id": "local", "name": "local", "status": "healthy",
			})
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

	resp := map[string]any{
		"status":        "draining",
		"tasks_running": s.coord.RunningCount(),
		"tasks_queued":  s.coord.QueueLen(),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	s.coord.Undrain()

	resp := map[string]any{
		"status":        "running",
		"tasks_running": s.coord.RunningCount(),
		"tasks_queued":  s.coord.QueueLen(),
	}
	writeJSON(w, http.StatusOK, resp)
}
