package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// taskLogs streams live stdout/stderr from a running task via Server-Sent Events.
// Requires a LogBroadcaster to be set on the server (see SetLogBroadcaster).
// If the task is not found or no broadcaster is configured, returns 404.
func (s *Server) taskLogs(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Verify task exists.
	if _, err := s.coord.Get(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if s.logBroadcaster == nil {
		writeError(w, http.StatusNotFound, "log streaming not available")
		return
	}

	// Set SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Subscribe to log events for this task.
	ch, unsub := s.logBroadcaster.Subscribe(id, 64)
	defer unsub()

	// Flush immediately so the client sees headers.
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				// Stream closed — task finished.
				fmt.Fprintf(w, "event: done\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", data)
			flusher.Flush()
		case <-ctx.Done():
			// Client disconnected.
			return
		}
	}
}
