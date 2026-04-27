package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/worker"
)

func TestAPI_TaskLogs_SSE(t *testing.T) {
	ts, srv := testServerWithRef(t)

	// Create a broadcaster and register it on the server.
	lb := worker.NewLogBroadcaster()
	srv.SetLogBroadcaster(lb)

	// Submit a task.
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo", "hello"},
	})
	var task model.Task
	decodeJSON(t, resp, &task)

	// Simulate a running task by writing to the broadcaster in a goroutine.
	go func() {
		// Give the SSE client time to connect.
		time.Sleep(100 * time.Millisecond)
		w := lb.Writer(task.ID, "stdout")
		w.Write([]byte("line1\n"))
		w.Write([]byte("line2\n"))
		time.Sleep(50 * time.Millisecond)
		lb.Close(task.ID)
	}()

	// Connect to SSE endpoint.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/tasks/"+task.ID+"/logs", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "text/event-stream" {
		t.Fatalf("expected Content-Type text/event-stream, got %q", ct)
	}

	// Parse SSE events.
	var events []worker.LogEvent
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")
			var ev worker.LogEvent
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				continue // skip non-JSON lines (like "done" event)
			}
			events = append(events, ev)
		}
	}

	if len(events) < 2 {
		t.Fatalf("expected at least 2 log events, got %d", len(events))
	}
	if events[0].Stream != "stdout" || events[0].Data != "line1\n" {
		t.Fatalf("unexpected first event: %+v", events[0])
	}
	if events[1].Stream != "stdout" || events[1].Data != "line2\n" {
		t.Fatalf("unexpected second event: %+v", events[1])
	}
}

func TestAPI_TaskLogs_NotFound(t *testing.T) {
	ts := testServer(t)

	resp, err := http.Get(ts.URL + "/api/v1/tasks/nonexistent/logs")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAPI_TaskLogs_CompletedTask(t *testing.T) {
	ts, srv := testServerWithRef(t)

	lb := worker.NewLogBroadcaster()
	srv.SetLogBroadcaster(lb)

	// Submit a task and complete it so it has persisted stdout/stderr.
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo", "hello"},
	})
	var task model.Task
	decodeJSON(t, resp, &task)

	// Complete the task via coordinator (set stdout/stderr, mark completed).
	srv.coord.MarkRunning(task.ID, "worker-1")
	srv.coord.Complete(task.ID, 0, "persisted stdout\n", "persisted stderr\n", "", "", 0)

	// Request logs for the completed task — should get persisted output, not hang.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/tasks/"+task.ID+"/logs", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Parse SSE events.
	var events []worker.LogEvent
	var gotDone bool
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")
			var ev worker.LogEvent
			if err := json.Unmarshal([]byte(data), &ev); err == nil && ev.Stream != "" {
				events = append(events, ev)
			}
			// Check for done with status.
			var doneEv map[string]any
			if err := json.Unmarshal([]byte(data), &doneEv); err == nil {
				if _, ok := doneEv["status"]; ok {
					gotDone = true
				}
			}
		}
	}

	if len(events) < 2 {
		t.Fatalf("expected at least 2 events (stdout + stderr), got %d", len(events))
	}
	if events[0].Stream != "stdout" || events[0].Data != "persisted stdout\n" {
		t.Fatalf("unexpected stdout event: %+v", events[0])
	}
	if events[1].Stream != "stderr" || events[1].Data != "persisted stderr\n" {
		t.Fatalf("unexpected stderr event: %+v", events[1])
	}
	if !gotDone {
		t.Fatal("expected done event with status")
	}
}

func TestAPI_TaskLogs_NoBroadcaster(t *testing.T) {
	ts := testServer(t)

	// Submit a task so it exists.
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo"},
	})
	var task model.Task
	decodeJSON(t, resp, &task)

	// Without a broadcaster set, should return 404 (no live logs available).
	resp, err := http.Get(ts.URL + "/api/v1/tasks/" + task.ID + "/logs")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 when no broadcaster, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}
