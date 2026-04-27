package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/syzygyhack/ziggurat/internal/worker"
)

// taskLogs streams live stdout/stderr from a running task via Server-Sent Events.
// Requires a LogBroadcaster to be set on the server (see SetLogBroadcaster).
//
// If the task has already finished, the persisted stdout/stderr are sent as
// log events followed by a done event, so callers never hang on a completed task.
func (s *Server) taskLogs(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Verify task exists and get its current state.
	task, err := s.coord.Get(id)
	if err != nil {
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
		return
	}

	// If the task is already in a terminal state, send persisted output
	// so callers don't hang forever on a completed/failed task.
	if isTerminalStatus(task.Status.String()) {
		sendPersistedLogs(w, flusher, task.Stdout, task.Stderr)
		fmt.Fprintf(w, "event: done\ndata: {\"status\":%q}\n\n", task.Status.String())
		flusher.Flush()
		return
	}

	// Subscribe to live log events.
	ch, unsub := s.logBroadcaster.Subscribe(task.ID, 64)
	defer unsub()

	flusher.Flush()

	// Heartbeat keeps the connection alive and lets clients detect stalls.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

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
		case <-heartbeat.C:
			// SSE comment line — keeps connection alive through proxies.
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

// sendPersistedLogs sends stored stdout/stderr as SSE log events.
func sendPersistedLogs(w http.ResponseWriter, flusher http.Flusher, stdout, stderr string) {
	if stdout != "" {
		ev := worker.LogEvent{Stream: "stdout", Data: stdout, Time: time.Now()}
		if data, err := json.Marshal(ev); err == nil {
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", data)
		}
	}
	if stderr != "" {
		ev := worker.LogEvent{Stream: "stderr", Data: stderr, Time: time.Now()}
		if data, err := json.Marshal(ev); err == nil {
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", data)
		}
	}
	flusher.Flush()
}

func isTerminalStatus(s string) bool {
	switch s {
	case "completed", "failed", "cancelled", "dead_letter":
		return true
	}
	return false
}
