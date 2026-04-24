package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/model"
)

// freePort finds an available TCP port by binding to :0.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// TestNode_EndToEnd starts a real node, submits a task via HTTP, waits for
// it to complete, and verifies the stdout captured.
func TestNode_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e test requires process execution")
	}

	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	port := freePort(t)
	grpcPort := freePort(t)
	gossipPort := freePort(t)
	cfg := config.DefaultConfig()
	cfg.Node.DataDir = tmpDir
	cfg.Network.Bind = "127.0.0.1"
	cfg.Network.HTTPPort = port
	cfg.Network.GRPCPort = grpcPort
	cfg.Network.GossipPort = gossipPort
	cfg.Compute.TaskTimeout = 30 * time.Second
	cfg.Compute.CancelGrace = 2 * time.Second
	cfg.Resilience.TaskRetries = 0

	ctx := context.Background()
	n, err := Start(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown(context.Background())

	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	// Submit a task.
	body := map[string]any{
		"command": []string{"echo", "hello from ziggurat"},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(base+"/tasks", "application/json", bytes.NewReader(data))
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

	// Wait for task to complete (long-poll).
	waitResp, err := http.Post(base+"/tasks/"+submitted.ID+"/wait", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if waitResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(waitResp.Body)
		waitResp.Body.Close()
		t.Fatalf("wait: expected 200, got %d: %s", waitResp.StatusCode, b)
	}

	var completed model.Task
	json.NewDecoder(waitResp.Body).Decode(&completed)
	waitResp.Body.Close()

	if completed.Status != model.TaskCompleted {
		t.Fatalf("expected completed, got %s (error: %s)", completed.Status, completed.Error)
	}
	if completed.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", completed.ExitCode)
	}
	if completed.Stdout != "hello from ziggurat\n" {
		t.Fatalf("expected stdout 'hello from ziggurat\\n', got %q", completed.Stdout)
	}

	// Verify health endpoint reports the completed task.
	healthResp, _ := http.Get(base + "/health")
	var health map[string]any
	json.NewDecoder(healthResp.Body).Decode(&health)
	healthResp.Body.Close()

	if health["status"] != "healthy" {
		t.Fatalf("expected healthy, got %v", health["status"])
	}
}

// TestNode_EndToEnd_TaskFailure submits a task that exits non-zero and
// verifies the failure is captured.
func TestNode_EndToEnd_TaskFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e test requires process execution")
	}

	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	port := freePort(t)
	grpcPort := freePort(t)
	gossipPort := freePort(t)
	cfg := config.DefaultConfig()
	cfg.Node.DataDir = tmpDir
	cfg.Network.Bind = "127.0.0.1"
	cfg.Network.HTTPPort = port
	cfg.Network.GRPCPort = grpcPort
	cfg.Network.GossipPort = gossipPort
	cfg.Compute.TaskTimeout = 30 * time.Second
	cfg.Resilience.TaskRetries = 0

	ctx := context.Background()
	n, err := Start(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown(context.Background())

	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	body := map[string]any{
		"command": []string{"sh", "-c", "echo 'oops' >&2; exit 1"},
	}
	data, _ := json.Marshal(body)
	resp, _ := http.Post(base+"/tasks", "application/json", bytes.NewReader(data))
	var submitted model.Task
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	waitResp, _ := http.Post(base+"/tasks/"+submitted.ID+"/wait", "application/json", nil)
	var result model.Task
	json.NewDecoder(waitResp.Body).Decode(&result)
	waitResp.Body.Close()

	if result.Status != model.TaskDeadLetter {
		t.Fatalf("expected dead_letter, got %s", result.Status)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", result.ExitCode)
	}
	if result.Stderr != "oops\n" {
		t.Fatalf("expected stderr 'oops\\n', got %q", result.Stderr)
	}
}

// TestNode_EndToEnd_Cancel submits a long-running task and cancels it.
func TestNode_EndToEnd_Cancel(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e test requires process execution")
	}

	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	port := freePort(t)
	grpcPort := freePort(t)
	gossipPort := freePort(t)
	cfg := config.DefaultConfig()
	cfg.Node.DataDir = tmpDir
	cfg.Network.Bind = "127.0.0.1"
	cfg.Network.HTTPPort = port
	cfg.Network.GRPCPort = grpcPort
	cfg.Network.GossipPort = gossipPort
	cfg.Compute.TaskTimeout = 60 * time.Second
	cfg.Compute.CancelGrace = 1 * time.Second
	cfg.Resilience.TaskRetries = 0

	ctx := context.Background()
	n, err := Start(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown(context.Background())

	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	body := map[string]any{
		"command": []string{"sleep", "60"},
	}
	data, _ := json.Marshal(body)
	resp, _ := http.Post(base+"/tasks", "application/json", bytes.NewReader(data))
	var submitted model.Task
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	// Give the worker time to pick up and start the task.
	time.Sleep(500 * time.Millisecond)

	// Cancel it.
	req, _ := http.NewRequest(http.MethodDelete, base+"/tasks/"+submitted.ID, nil)
	cancelResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cancelResp.Body.Close()

	// Wait for it to reach terminal state.
	waitResp, _ := http.Post(base+"/tasks/"+submitted.ID+"/wait", "application/json", nil)
	var result model.Task
	json.NewDecoder(waitResp.Body).Decode(&result)
	waitResp.Body.Close()

	if result.Status != model.TaskCancelled {
		t.Fatalf("expected cancelled, got %s (error: %s)", result.Status, result.Error)
	}
}

// TestNode_TwoNodeCluster starts two nodes, verifies they discover each other
// via gossip, and confirms the /nodes API returns both members.
func TestNode_TwoNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster test requires gossip")
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Node 1.
	dir1 := t.TempDir()
	port1 := freePort(t)
	grpc1 := freePort(t)
	gossip1 := freePort(t)
	cfg1 := config.DefaultConfig()
	cfg1.Node.DataDir = dir1
	cfg1.Node.Name = "node-one"
	cfg1.Network.Bind = "127.0.0.1"
	cfg1.Network.HTTPPort = port1
	cfg1.Network.GRPCPort = grpc1
	cfg1.Network.GossipPort = gossip1
	cfg1.Compute.TaskTimeout = 30 * time.Second
	cfg1.Resilience.TaskRetries = 0

	n1, err := Start(context.Background(), cfg1, log.With("node", "1"))
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Shutdown(context.Background())

	// Node 2 — joins node 1 via gossip.
	dir2 := t.TempDir()
	port2 := freePort(t)
	grpc2 := freePort(t)
	gossip2 := freePort(t)
	cfg2 := config.DefaultConfig()
	cfg2.Node.DataDir = dir2
	cfg2.Node.Name = "node-two"
	cfg2.Node.Tags = []string{"gpu"}
	cfg2.Network.Bind = "127.0.0.1"
	cfg2.Network.HTTPPort = port2
	cfg2.Network.GRPCPort = grpc2
	cfg2.Network.GossipPort = gossip2
	cfg2.Cluster.Seeds = []string{fmt.Sprintf("127.0.0.1:%d", gossip1)}
	cfg2.Compute.TaskTimeout = 30 * time.Second
	cfg2.Resilience.TaskRetries = 0

	n2, err := Start(context.Background(), cfg2, log.With("node", "2"))
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Shutdown(context.Background())

	// Wait for gossip convergence — both nodes should see 2 members.
	base1 := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port1)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base1 + "/health")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var health map[string]any
		json.NewDecoder(resp.Body).Decode(&health)
		resp.Body.Close()

		nodes, _ := health["nodes"].(float64)
		if int(nodes) == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify /nodes on node-1 returns both nodes.
	resp, err := http.Get(base1 + "/nodes")
	if err != nil {
		t.Fatal(err)
	}
	var nodes []map[string]any
	json.NewDecoder(resp.Body).Decode(&nodes)
	resp.Body.Close()

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes from /nodes API, got %d", len(nodes))
	}

	// Also verify from node-2's perspective.
	base2 := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port2)
	resp, err = http.Get(base2 + "/nodes")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&nodes)
	resp.Body.Close()

	if len(nodes) != 2 {
		t.Fatalf("node-2: expected 2 nodes, got %d", len(nodes))
	}

	// Verify node-2 can see node-1's name in the node detail.
	found := false
	for _, n := range nodes {
		name, _ := n["name"].(string)
		if name == "node-one" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("node-2 does not see node-one in its node list")
	}
}
