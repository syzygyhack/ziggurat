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
