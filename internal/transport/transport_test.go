package transport

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"github.com/syzygyhack/ziggurat/internal/transport/pb"
	"github.com/syzygyhack/ziggurat/internal/worker"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testEnv sets up a full coordinator + store + gRPC server for testing.
type testEnv struct {
	coord  *coord.Coordinator
	store  *store.Store
	addr   string
	server *grpc.Server
}

func setupTestEnv(t *testing.T) *testEnv {
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

	defaults := coord.TaskDefaults{MaxRetries: 2, Timeout: 5 * time.Minute}
	c := coord.New(s, persist, defaults, log)

	w := worker.New("test-worker", nil, nil, s, c, config.ComputeConfig{
		TaskTimeout: 5 * time.Minute,
	}, t.TempDir(), log)

	srv := grpc.NewServer()
	tSrv := NewServer(c, s, w, log)
	pb.RegisterZigguratNodeServer(srv, tSrv)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go srv.Serve(ln)
	t.Cleanup(func() { srv.GracefulStop() })

	return &testEnv{
		coord:  c,
		store:  s,
		addr:   ln.Addr().String(),
		server: srv,
	}
}

func TestTransport_DispatchTask(t *testing.T) {
	env := setupTestEnv(t)

	client := NewClient()
	defer client.Close()

	task := &model.Task{
		ID:      "test-task-1",
		Command: []string{"echo", "hello"},
	}

	assignedID, err := client.DispatchTask(context.Background(), env.addr, task)
	if err != nil {
		t.Fatal(err)
	}

	if assignedID == "" {
		t.Fatal("expected non-empty assigned ID from DispatchTask")
	}

	// AcceptDispatch preserves the original task ID. Verify we can look
	// up the task by the returned ID.
	got, err := env.coord.Get(assignedID)
	if err != nil {
		t.Fatalf("failed to get task by assigned ID %q: %v", assignedID, err)
	}
	if got.Command[0] != "echo" || got.Command[1] != "hello" {
		t.Fatalf("unexpected command: %v", got.Command)
	}
}

func TestTransport_PullShard(t *testing.T) {
	env := setupTestEnv(t)

	// Store an object.
	data := []byte("hello world shard data for testing")
	hash, err := env.store.Put(context.Background(), "test/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Pull it via gRPC.
	client := NewClient()
	defer client.Close()

	rc, err := client.PullShard(context.Background(), env.addr, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}
}

func TestTransport_PushShard(t *testing.T) {
	// Create two separate stores: source and destination.
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := store.DefaultTestConfig()

	srcStore, err := store.New(storeCfg, srcDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer srcStore.Close()

	dstStore, err := store.New(storeCfg, dstDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer dstStore.Close()

	// Set up gRPC server backed by the destination store.
	tasksDB, err := bbolt.Open(filepath.Join(dstDir, "tasks.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tasksDB.Close()

	persist, err := coord.NewPersist(tasksDB)
	if err != nil {
		t.Fatal(err)
	}
	c := coord.New(dstStore, persist, coord.TaskDefaults{}, log)

	w := worker.New("dst-worker", nil, nil, dstStore, c, config.ComputeConfig{
		TaskTimeout: 5 * time.Minute,
	}, t.TempDir(), log)

	srv := grpc.NewServer()
	tSrv := NewServer(c, dstStore, w, log)
	pb.RegisterZigguratNodeServer(srv, tSrv)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	defer srv.GracefulStop()

	// Store data in source.
	data := []byte("replicated shard content — testing push")
	hash, err := srcStore.Put(context.Background(), "src/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Push from source to destination via gRPC.
	client := NewClient()
	defer client.Close()

	err = client.PushShard(context.Background(), ln.Addr().String(), hash, int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify the blob exists on the destination store.
	rc, err := dstStore.GetByHash(context.Background(), hash)
	if err != nil {
		t.Fatalf("blob not found on destination: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch on destination: got %q, want %q", got, data)
	}

	// Verify receiver-side ObjectMeta was created (Fix #3).
	stats := dstStore.Stats()
	if stats.Objects != 1 {
		t.Fatalf("expected 1 object in destination metadata, got %d", stats.Objects)
	}
}

func TestTransport_PullShard_NotFound(t *testing.T) {
	env := setupTestEnv(t)

	cc, err := grpc.NewClient(env.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	client := pb.NewZigguratNodeClient(cc)
	stream, err := client.PullShard(context.Background(), &pb.PullShardRequest{Hash: "nonexistent_hash"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for non-existent shard")
	}
}
