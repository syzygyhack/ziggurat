package transport

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
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
	"github.com/zeebo/blake3"
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

func TestTransport_PushECShard_CreatesMetadata(t *testing.T) {
	// Set up a destination store with EC enabled so it can reconstruct.
	dstDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storeCfg := store.DefaultTestConfig()
	storeCfg.Erasure.Enabled = true
	storeCfg.Erasure.DataShards = 4
	storeCfg.Erasure.ParityShards = 2

	dstStore, err := store.New(storeCfg, dstDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer dstStore.Close()

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

	// Encode test data into EC shards on a source store.
	srcDir := t.TempDir()
	srcStore, err := store.New(storeCfg, srcDir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer srcStore.Close()

	data := bytes.Repeat([]byte("x"), 1024)
	hash, err := srcStore.Put(context.Background(), "src/obj", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	codec := srcStore.ErasureCodec()
	if codec == nil {
		t.Skip("erasure codec not enabled on source store")
	}

	shards, err := codec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Compute BLAKE3 hashes for each shard so the receiver can verify integrity.
	shardHashes := make([]string, len(shards))
	for i, s := range shards {
		h := blake3.New()
		h.Write(s)
		var digest [32]byte
		h.Sum(digest[:0])
		shardHashes[i] = hex.EncodeToString(digest[:])
	}

	shardSize := int64(len(shards[0]))
	ecParams := model.ErasureParams{
		DataShards:   4,
		ParityShards: 2,
		OriginalSize: 1024,
		ShardSize:    shardSize,
		ShardHashes:  shardHashes,
	}
	ecMeta, err := json.Marshal(&ecParams)
	if err != nil {
		t.Fatal(err)
	}

	// Push enough shards (4 data shards) to the destination so it can reconstruct.
	client := NewClient()
	defer client.Close()

	for i := 0; i < 4; i++ {
		err = client.PushECShard(context.Background(), ln.Addr().String(), hash, i, shards[i], ecMeta)
		if err != nil {
			t.Fatalf("push shard %d: %v", i, err)
		}
	}

	// Verify receiver-side ObjectMeta was created with ErasureParams.
	stats := dstStore.Stats()
	if stats.Objects != 1 {
		t.Fatalf("expected 1 object in destination metadata, got %d", stats.Objects)
	}

	// UsedBytes should reflect shard size, not original object size.
	if stats.UsedBytes != shardSize {
		t.Fatalf("expected UsedBytes=%d (shard size), got %d", shardSize, stats.UsedBytes)
	}

	// Verify the shards are on disk.
	indices, err := store.ListLocalShardIndices(dstStore.Dir(), hash)
	if err != nil {
		t.Fatalf("list shards on destination: %v", err)
	}
	if len(indices) != 4 {
		t.Fatalf("expected 4 shards on destination, got %d", len(indices))
	}
}

func TestTransport_PushECShard_RejectsOutOfRangeIndex(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	ecMeta, _ := json.Marshal(model.ErasureParams{
		DataShards:   4,
		ParityShards: 2,
		OriginalSize: 1024,
		ShardSize:    256,
	})

	// Index 6 is out of range for RS(4,2) = 6 total shards [0..5].
	err := client.PushECShard(context.Background(), env.addr, "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234", 6, []byte("data"), ecMeta)
	if err == nil {
		t.Fatal("expected error for out-of-range shard_index")
	}

	// Negative index.
	err = client.PushECShard(context.Background(), env.addr, "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234", -1, []byte("data"), ecMeta)
	if err == nil {
		t.Fatal("expected error for negative shard_index")
	}
}

func TestTransport_PushECShard_RejectsMissingErasureMeta(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	// Push with nil ecMeta.
	err := client.PushECShard(context.Background(), env.addr, "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234", 0, []byte("data"), nil)
	if err == nil {
		t.Fatal("expected error for missing erasure_meta")
	}
}

func TestTransport_PushECShard_RejectsHashMismatch(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	ecMeta, _ := json.Marshal(model.ErasureParams{
		DataShards:   4,
		ParityShards: 2,
		OriginalSize: 1024,
		ShardSize:    256,
		ShardHashes:  []string{"0000000000000000000000000000000000000000000000000000000000000000", "", "", "", "", ""},
	})

	// Shard 0 data won't match the all-zeros expected hash.
	err := client.PushECShard(context.Background(), env.addr, "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234", 0, []byte("this data does not match"), ecMeta)
	if err == nil {
		t.Fatal("expected error for shard hash mismatch")
	}
}

func TestTransport_PushShard_RejectsShortHash(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	// EC path: short hash should be rejected before reaching WriteSingleShard.
	ecMeta, _ := json.Marshal(model.ErasureParams{
		DataShards:   4,
		ParityShards: 2,
		OriginalSize: 1024,
		ShardSize:    256,
	})
	err := client.PushECShard(context.Background(), env.addr, "tooshort", 0, []byte("data"), ecMeta)
	if err == nil {
		t.Fatal("expected error for short hash on EC push")
	}

	// Blob path: short hash should be rejected before reaching WriteBlob.
	err = client.PushShard(context.Background(), env.addr, "ab", 4, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error for short hash on blob push")
	}
}

func TestTransport_PushECShard_RejectsZeroShardCounts(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	ecMeta, _ := json.Marshal(model.ErasureParams{
		DataShards:   0,
		ParityShards: 0,
		OriginalSize: 1024,
		ShardSize:    256,
	})
	err := client.PushECShard(context.Background(), env.addr, "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234", 0, []byte("data"), ecMeta)
	if err == nil {
		t.Fatal("expected error for zero shard counts")
	}
}

func TestTransport_PushECShard_RejectsOversizedData(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	ecMeta, _ := json.Marshal(model.ErasureParams{
		DataShards:   4,
		ParityShards: 2,
		OriginalSize: 1024,
		ShardSize:    256, // limit will be 512 (2x claimed)
	})

	// Send 1024 bytes, which exceeds 2x the claimed 256-byte ShardSize.
	bigData := bytes.Repeat([]byte("x"), 1024)
	err := client.PushECShard(context.Background(), env.addr, "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234", 0, bigData, ecMeta)
	if err == nil {
		t.Fatal("expected error for oversized shard data")
	}
}

func TestTransport_PushFullBlob_CleansUpOnHashMismatch(t *testing.T) {
	env := setupTestEnv(t)
	client := NewClient()
	defer client.Close()

	data := []byte("hello blob orphan test")
	// Push with a fake hash that won't match the actual BLAKE3.
	fakeHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	err := client.PushShard(context.Background(), env.addr, fakeHash, int64(len(data)), bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for hash mismatch")
	}

	// The blob was written under its real content hash, then deleted on mismatch.
	// Verify the destination store has no objects (no orphan).
	stats := env.store.Stats()
	if stats.Objects != 0 {
		t.Fatalf("expected 0 objects after mismatch cleanup, got %d", stats.Objects)
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

func TestTransport_CancelTask(t *testing.T) {
	env := setupTestEnv(t)

	// Submit a task and manually move it to running state so cancel has
	// something to work with.
	task := &model.Task{
		ID:      "cancel-test-1",
		Command: []string{"sleep", "60"},
	}
	if err := env.coord.AcceptDispatch(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	// Dequeue and mark running to simulate a worker picking it up.
	dequeued := env.coord.Dequeue(nil, nil)
	if dequeued == nil {
		t.Fatal("expected to dequeue task")
	}
	// Register a cancel function so the coordinator has something to call.
	var cancelled bool
	env.coord.RegisterCancel(dequeued.ID, func() { cancelled = true })
	env.coord.MarkRunning(dequeued.ID, "test-worker")

	// Cancel via gRPC.
	client := NewClient()
	defer client.Close()

	err := client.CancelTask(context.Background(), env.addr, "cancel-test-1")
	if err != nil {
		t.Fatalf("CancelTask failed: %v", err)
	}

	// Verify the task is now in cancelling state and the cancel func was called.
	got, err := env.coord.Get("cancel-test-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.TaskCancelling {
		t.Fatalf("expected TaskCancelling, got %s", got.Status)
	}
	if !cancelled {
		t.Fatal("expected cancel function to be called")
	}
}

func TestTransport_CancelTask_NotFound(t *testing.T) {
	env := setupTestEnv(t)

	client := NewClient()
	defer client.Close()

	err := client.CancelTask(context.Background(), env.addr, "nonexistent-task")
	if err != nil {
		t.Fatalf("CancelTask should not error for not-found task, got: %v", err)
	}
}
