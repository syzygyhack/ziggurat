package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// testNode holds a running node and its HTTP base URL.
type testNode struct {
	node    *Node
	httpURL string // e.g. "http://127.0.0.1:12345/api/v1"
}

// testCluster spins up a multi-node Ziggurat cluster for integration testing.
// Nodes join via gossip seeds. The first node is the seed; subsequent nodes
// join it. Returns cleanup function that shuts everything down.
type testCluster struct {
	nodes []*testNode
	t     *testing.T
}

func startTestCluster(t *testing.T, configs []*config.Config) *testCluster {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	tc := &testCluster{t: t}

	for i, cfg := range configs {
		n, err := Start(context.Background(), cfg, log.With("node", i))
		if err != nil {
			// Shut down any nodes already started.
			for _, tn := range tc.nodes {
				tn.node.Shutdown(context.Background())
			}
			t.Fatalf("start node %d: %v", i, err)
		}
		tn := &testNode{
			node:    n,
			httpURL: fmt.Sprintf("http://127.0.0.1:%d/api/v1", cfg.Network.HTTPPort),
		}
		tc.nodes = append(tc.nodes, tn)
	}

	t.Cleanup(func() {
		// Shut down in reverse order (workers before coordinators).
		for i := len(tc.nodes) - 1; i >= 0; i-- {
			tc.nodes[i].node.Shutdown(context.Background())
		}
	})

	return tc
}

// waitForCluster polls the first node's health endpoint until all expected
// nodes have joined, or the deadline expires.
func (tc *testCluster) waitForCluster(expected int, timeout time.Duration) {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(tc.nodes[0].httpURL + "/health")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var health map[string]any
		json.NewDecoder(resp.Body).Decode(&health)
		resp.Body.Close()

		nodes, _ := health["nodes"].(float64)
		if int(nodes) >= expected {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	tc.t.Fatalf("cluster did not converge to %d nodes within %v", expected, timeout)
}

// makeNodeConfig creates a config for a test node with ephemeral ports.
func makeNodeConfig(t *testing.T, role string, gossipSeed string) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Node.DataDir = t.TempDir()
	cfg.Node.Role = role
	cfg.Network.Bind = "127.0.0.1"
	cfg.Network.HTTPPort = freePort(t)
	cfg.Network.GRPCPort = freePort(t)
	cfg.Network.GossipPort = freePort(t)
	cfg.Compute.TaskTimeout = 30 * time.Second
	cfg.Compute.CancelGrace = 2 * time.Second
	cfg.Resilience.TaskRetries = 0
	cfg.Cluster.Discovery = "none" // no mDNS in tests
	if gossipSeed != "" {
		cfg.Cluster.Seeds = []string{gossipSeed}
	}
	return cfg
}

// TestIntegration_CrossNodeDispatch starts a coordinator-only node and a
// worker node. A task submitted to the coordinator should be dispatched to
// the worker via gRPC, executed there, and the result retrievable from the
// coordinator.
func TestIntegration_CrossNodeDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires process execution and gossip")
	}

	// Node 1: coordinator (no local worker loop — must dispatch).
	coordCfg := makeNodeConfig(t, "coordinator", "")

	// Node 2: worker (runs worker loop, receives dispatched tasks).
	workerCfg := makeNodeConfig(t, "worker",
		fmt.Sprintf("127.0.0.1:%d", coordCfg.Network.GossipPort))

	tc := startTestCluster(t, []*config.Config{coordCfg, workerCfg})
	tc.waitForCluster(2, 10*time.Second)

	coordURL := tc.nodes[0].httpURL

	// Submit a task to the coordinator.
	body := map[string]any{
		"command": []string{"echo", "dispatched-output"},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(coordURL+"/tasks", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("submit: expected 201, got %d: %s", resp.StatusCode, b)
	}

	var submitted model.Task
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	if submitted.ID == "" {
		t.Fatal("expected task ID")
	}

	// Wait for completion with a timeout. The coordinator dispatches the
	// task to the worker via gRPC; the worker executes it and reports back.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var result model.Task
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for task %s to complete", submitted.ID)
		default:
		}

		getResp, err := http.Get(coordURL + "/tasks/" + submitted.ID)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		json.NewDecoder(getResp.Body).Decode(&result)
		getResp.Body.Close()

		if result.Status == model.TaskCompleted || result.Status == model.TaskFailed ||
			result.Status == model.TaskDeadLetter || result.Status == model.TaskCancelled {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if result.Status != model.TaskCompleted {
		t.Fatalf("expected completed, got %s (error: %s)", result.Status, result.Error)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	if result.Stdout != "dispatched-output\n" {
		t.Fatalf("expected stdout 'dispatched-output\\n', got %q", result.Stdout)
	}
}

// TestIntegration_ArtifactByBasename is the golden end-to-end test for the
// spec's primary artifact mechanism: upload a single-file script to the store,
// then run it by its basename. The artifact must be staged into the task
// workspace under its original filename (e.g. run.sh), not under a
// content-hash-derived name, or the command cannot find it.
func TestIntegration_ArtifactByBasename(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires process execution")
	}

	cfg := makeNodeConfig(t, "hybrid", "")
	tc := startTestCluster(t, []*config.Config{cfg})
	tc.waitForCluster(1, 10*time.Second)
	baseURL := tc.nodes[0].httpURL

	// Upload a raw (non-tar) single-file shell script under a namespace key.
	const marker = "artifact-ran-ok"
	script := "echo " + marker + "\n"
	req, _ := http.NewRequest(http.MethodPut, baseURL+"/store/scripts/run.sh",
		strings.NewReader(script))
	req.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT script: expected 201, got %d", putResp.StatusCode)
	}

	// Submit a task that references the script by basename in its command and
	// lists the namespace key as an artifact.
	body := map[string]any{
		"command":   []string{"sh", "run.sh"},
		"artifacts": []string{"scripts/run.sh"},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/tasks", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("submit: expected 201, got %d: %s", resp.StatusCode, b)
	}
	var submitted model.Task
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	result := waitForTask(t, baseURL, submitted.ID, 30*time.Second)

	if result.Status != model.TaskCompleted {
		t.Fatalf("expected completed, got %s (exit %d, error: %s, stderr: %q)",
			result.Status, result.ExitCode, result.Error, result.Stderr)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d (stderr: %q)", result.ExitCode, result.Stderr)
	}
	if strings.TrimSpace(result.Stdout) != marker {
		t.Fatalf("expected stdout %q, got %q", marker, result.Stdout)
	}
}

// TestIntegration_ArtifactByBasename_CrossNode verifies the artifact-basename
// fix survives the gRPC wire path: a coordinator with no local worker resolves
// the artifact (preserving its basename) and dispatches the task to a separate
// worker node, which must stage the script under run.sh and execute it.
func TestIntegration_ArtifactByBasename_CrossNode(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires process execution and gossip")
	}

	coordCfg := makeNodeConfig(t, "coordinator", "")
	workerCfg := makeNodeConfig(t, "worker",
		fmt.Sprintf("127.0.0.1:%d", coordCfg.Network.GossipPort))
	tc := startTestCluster(t, []*config.Config{coordCfg, workerCfg})
	tc.waitForCluster(2, 10*time.Second)
	coordURL := tc.nodes[0].httpURL

	const marker = "cross-node-artifact-ok"
	script := "echo " + marker + "\n"
	req, _ := http.NewRequest(http.MethodPut, coordURL+"/store/scripts/run.sh",
		strings.NewReader(script))
	req.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT script: expected 201, got %d", putResp.StatusCode)
	}

	body := map[string]any{
		"command":   []string{"sh", "run.sh"},
		"artifacts": []string{"scripts/run.sh"},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(coordURL+"/tasks", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var submitted model.Task
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	result := waitForTask(t, coordURL, submitted.ID, 30*time.Second)
	if result.Status != model.TaskCompleted || result.ExitCode != 0 {
		t.Fatalf("expected completed/exit 0, got %s/%d (error: %s, stderr: %q)",
			result.Status, result.ExitCode, result.Error, result.Stderr)
	}
	if strings.TrimSpace(result.Stdout) != marker {
		t.Fatalf("expected stdout %q, got %q", marker, result.Stdout)
	}
}

// TestIntegration_DirectoryArtifact guards against the basename fix regressing
// tar (directory) artifacts: a tar-wrapped directory artifact must still
// extract into the workspace root so its files are reachable by their relative
// paths, not nested under the artifact's name.
func TestIntegration_DirectoryArtifact(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires process execution")
	}

	cfg := makeNodeConfig(t, "hybrid", "")
	tc := startTestCluster(t, []*config.Config{cfg})
	tc.waitForCluster(1, 10*time.Second)
	baseURL := tc.nodes[0].httpURL

	// Build a deterministic tar of a directory containing a script.
	srcDir := t.TempDir()
	const marker = "dir-artifact-ok"
	if err := os.WriteFile(srcDir+"/run.sh", []byte("echo "+marker+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var tarBuf bytes.Buffer
	if err := store.CreateDeterministicTar(srcDir, &tarBuf); err != nil {
		t.Fatal(err)
	}

	req, _ := http.NewRequest(http.MethodPut, baseURL+"/store/bundles/scripts",
		bytes.NewReader(tarBuf.Bytes()))
	req.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT bundle: expected 201, got %d", putResp.StatusCode)
	}

	// The tar's files extract into the workspace root, so run.sh is reachable
	// by its relative path even though the artifact key basename is "scripts".
	body := map[string]any{
		"command":   []string{"sh", "run.sh"},
		"artifacts": []string{"bundles/scripts"},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/tasks", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var submitted model.Task
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	result := waitForTask(t, baseURL, submitted.ID, 30*time.Second)
	if result.Status != model.TaskCompleted || result.ExitCode != 0 {
		t.Fatalf("expected completed/exit 0, got %s/%d (error: %s, stderr: %q)",
			result.Status, result.ExitCode, result.Error, result.Stderr)
	}
	if strings.TrimSpace(result.Stdout) != marker {
		t.Fatalf("expected stdout %q, got %q", marker, result.Stdout)
	}
}

// waitForTask polls the task endpoint until the task reaches a terminal state
// or the timeout expires.
func waitForTask(t *testing.T, baseURL, id string, timeout time.Duration) model.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var result model.Task
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for task %s to complete", id)
		default:
		}
		getResp, err := http.Get(baseURL + "/tasks/" + id)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		json.NewDecoder(getResp.Body).Decode(&result)
		getResp.Body.Close()
		if result.Status == model.TaskCompleted || result.Status == model.TaskFailed ||
			result.Status == model.TaskDeadLetter || result.Status == model.TaskCancelled {
			return result
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestIntegration_StoreReplication starts two hybrid nodes, stores an object
// on node 1 via HTTP PUT, and verifies the object is replicated to node 2.
func TestIntegration_StoreReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires gossip and replication")
	}

	// Node 1: hybrid (coordinator + worker).
	n1Cfg := makeNodeConfig(t, "hybrid", "")
	n1Cfg.Storage.ReplicationFactor = 2

	// Node 2: hybrid, joins node 1.
	n2Cfg := makeNodeConfig(t, "hybrid",
		fmt.Sprintf("127.0.0.1:%d", n1Cfg.Network.GossipPort))
	n2Cfg.Storage.ReplicationFactor = 2

	tc := startTestCluster(t, []*config.Config{n1Cfg, n2Cfg})
	tc.waitForCluster(2, 10*time.Second)

	n1URL := tc.nodes[0].httpURL
	n2URL := tc.nodes[1].httpURL

	// PUT an object on node 1.
	payload := "replication-test-payload-12345"
	req, _ := http.NewRequest(http.MethodPut, n1URL+"/store/test/replobj",
		strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var putResult map[string]string
	json.NewDecoder(putResp.Body).Decode(&putResult)
	putResp.Body.Close()

	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT: expected 201, got %d", putResp.StatusCode)
	}

	hash := putResult["hash"]
	if hash == "" {
		t.Fatal("expected hash in PUT response")
	}

	// Wait for replication to node 2. Try fetching by content hash.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var got string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for replication of %s to node 2", hash[:12])
		default:
		}

		getResp, err := http.Get(n2URL + "/store/@hash/" + hash)
		if err != nil || getResp.StatusCode != http.StatusOK {
			if getResp != nil {
				getResp.Body.Close()
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		b, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		got = string(b)
		break
	}

	if got != payload {
		t.Fatalf("replication mismatch: got %q, want %q", got, payload)
	}
}
