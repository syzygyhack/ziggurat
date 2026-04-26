package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"go.etcd.io/bbolt"
)

// testServerWithRef creates a fully wired API server backed by real store and
// coordinator, returning both the httptest.Server and the API Server for
// further configuration (e.g. SetLogBroadcaster).
func testServerWithRef(t *testing.T) (*httptest.Server, *Server) {
	t.Helper()
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := store.DefaultTestConfig()
	s, err := store.New(storeCfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	tasksDB, err := bbolt.Open(filepath.Join(tmpDir, "tasks.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tasksDB.Close() })

	persist, err := coord.NewPersist(tasksDB)
	if err != nil {
		t.Fatal(err)
	}

	defaults := coord.TaskDefaults{MaxRetries: 0, Timeout: 5 * time.Minute}
	c := coord.New(s, persist, defaults, log)

	srv := New(c, s, log)
	ts := httptest.NewServer(srv.router)
	t.Cleanup(ts.Close)
	return ts, srv
}

// testServer creates a fully wired API server backed by real store and
// coordinator, returning an httptest.Server. Callers use standard HTTP
// to exercise the API.
func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	ts, _ := testServerWithRef(t)
	return ts
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

// --- Task API Tests ---

func TestAPI_SubmitTask(t *testing.T) {
	ts := testServer(t)

	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo", "hello"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var task model.Task
	decodeJSON(t, resp, &task)
	if task.ID == "" {
		t.Fatal("expected task ID")
	}
	if task.Status != model.TaskQueued {
		t.Fatalf("expected queued, got %s", task.Status)
	}
}

func TestAPI_SubmitTask_NoCommand(t *testing.T) {
	ts := testServer(t)

	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAPI_GetTask(t *testing.T) {
	ts := testServer(t)

	// Submit.
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo"},
	})
	var task model.Task
	decodeJSON(t, resp, &task)

	// Get.
	resp, err := http.Get(ts.URL + "/api/v1/tasks/" + task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var got model.Task
	decodeJSON(t, resp, &got)
	if got.ID != task.ID {
		t.Fatalf("expected ID %s, got %s", task.ID, got.ID)
	}
}

func TestAPI_GetTask_NotFound(t *testing.T) {
	ts := testServer(t)

	resp, err := http.Get(ts.URL + "/api/v1/tasks/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAPI_ListTasks(t *testing.T) {
	ts := testServer(t)

	// Submit 3 tasks.
	for i := 0; i < 3; i++ {
		resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
			"command": []string{"echo"},
		})
		resp.Body.Close()
	}

	resp, err := http.Get(ts.URL + "/api/v1/tasks")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var tasks []model.Task
	decodeJSON(t, resp, &tasks)
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
}

func TestAPI_ListTasks_StatusFilter(t *testing.T) {
	ts := testServer(t)

	// Submit a task (it will be queued).
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo"},
	})
	resp.Body.Close()

	// Filter for "completed" should return 0.
	resp, _ = http.Get(ts.URL + "/api/v1/tasks?status=completed")
	var tasks []model.Task
	decodeJSON(t, resp, &tasks)
	if len(tasks) != 0 {
		t.Fatalf("expected 0 completed tasks, got %d", len(tasks))
	}

	// Filter for "queued" should return 1.
	resp, _ = http.Get(ts.URL + "/api/v1/tasks?status=queued")
	decodeJSON(t, resp, &tasks)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 queued task, got %d", len(tasks))
	}
}

func TestAPI_ListTasks_Pagination(t *testing.T) {
	ts := testServer(t)

	for i := 0; i < 5; i++ {
		resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
			"command": []string{"echo"},
		})
		resp.Body.Close()
	}

	resp, _ := http.Get(ts.URL + "/api/v1/tasks?limit=2")
	var tasks []model.Task
	decodeJSON(t, resp, &tasks)
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks with limit, got %d", len(tasks))
	}

	resp, _ = http.Get(ts.URL + "/api/v1/tasks?offset=3")
	decodeJSON(t, resp, &tasks)
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks with offset=3, got %d", len(tasks))
	}
}

func TestAPI_CancelTask(t *testing.T) {
	ts := testServer(t)

	// Submit.
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo"},
	})
	var task model.Task
	decodeJSON(t, resp, &task)

	// Cancel.
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/tasks/"+task.ID, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var cancelled model.Task
	decodeJSON(t, resp, &cancelled)
	if cancelled.Status != model.TaskCancelled {
		t.Fatalf("expected cancelled, got %s", cancelled.Status)
	}
}

// --- Store API Tests ---

func TestAPI_StorePutGetDelete(t *testing.T) {
	ts := testServer(t)

	// PUT object.
	content := []byte("test data for store")
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/store/mykey", bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT expected 201, got %d", resp.StatusCode)
	}
	var putResult map[string]string
	decodeJSON(t, resp, &putResult)
	if putResult["hash"] == "" {
		t.Fatal("expected hash in PUT response")
	}
	if putResult["key"] != "mykey" {
		t.Fatalf("expected key=mykey, got %s", putResult["key"])
	}

	// GET object.
	resp, err = http.Get(ts.URL + "/api/v1/store/mykey")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET expected 200, got %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("GET content mismatch: got %q, want %q", got, content)
	}

	// DELETE object.
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/store/mykey", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// GET after delete should 404.
	resp, _ = http.Get(ts.URL + "/api/v1/store/mykey")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAPI_StoreList(t *testing.T) {
	ts := testServer(t)

	// PUT a few objects.
	for _, key := range []string{"data/a", "data/b", "other/c"} {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/store/"+key, bytes.NewReader([]byte(key)))
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}

	// List with prefix.
	resp, _ := http.Get(ts.URL + "/api/v1/store/?prefix=data/")
	var keys []string
	decodeJSON(t, resp, &keys)
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys with prefix data/, got %d: %v", len(keys), keys)
	}
}

// --- Health & Cluster ---

func TestAPI_Health(t *testing.T) {
	ts := testServer(t)

	resp, err := http.Get(ts.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var health map[string]any
	decodeJSON(t, resp, &health)
	if health["status"] != "healthy" {
		t.Fatalf("expected healthy, got %v", health["status"])
	}
}

func TestAPI_SubmitWithConstraints(t *testing.T) {
	ts := testServer(t)

	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command":     []string{"echo"},
		"requires":    []string{"gpu"},
		"constraints": []string{"gpu.vram >= 8GB"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var task model.Task
	decodeJSON(t, resp, &task)
	if len(task.Requires) != 1 || task.Requires[0] != "gpu" {
		t.Fatalf("expected requires [gpu], got %v", task.Requires)
	}
	if len(task.Constraints) != 1 || task.Constraints[0] != "gpu.vram >= 8GB" {
		t.Fatalf("expected constraint, got %v", task.Constraints)
	}
}

// --- Nodes & Drain ---

func TestAPI_ListNodes_SingleNode(t *testing.T) {
	ts := testServer(t)

	resp, err := http.Get(ts.URL + "/api/v1/nodes")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var nodes []map[string]any
	decodeJSON(t, resp, &nodes)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node in single-node mode, got %d", len(nodes))
	}
	if nodes[0]["id"] != "local" {
		t.Fatalf("expected id=local, got %v", nodes[0]["id"])
	}
}

func TestAPI_GetNode_SingleNode(t *testing.T) {
	ts := testServer(t)

	resp, err := http.Get(ts.URL + "/api/v1/nodes/local")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/api/v1/nodes/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAPI_Drain(t *testing.T) {
	ts := testServer(t)

	// Submit a task first.
	resp := postJSON(t, ts.URL+"/api/v1/tasks", map[string]any{
		"command": []string{"echo", "hello"},
	})
	resp.Body.Close()

	// Drain.
	resp = postJSON(t, ts.URL+"/api/v1/drain", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]any
	decodeJSON(t, resp, &result)
	if result["status"] != "draining" {
		t.Fatalf("expected draining status, got %v", result["status"])
	}
}
