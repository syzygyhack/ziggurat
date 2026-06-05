package transport

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"github.com/syzygyhack/ziggurat/internal/transport/pb"
	"github.com/syzygyhack/ziggurat/internal/worker"
	"github.com/zeebo/blake3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chunkSize = 256 * 1024     // 256 KB per streaming message
const maxECShardSize = 256 << 20 // 256 MiB hard cap on EC shard receive buffer

// Server implements the ZigguratNode gRPC service.
type Server struct {
	pb.UnimplementedZigguratNodeServer

	coord  *coord.Coordinator
	store  *store.Store
	worker *worker.Worker
	log    *slog.Logger
}

// NewServer creates a gRPC service server.
func NewServer(c *coord.Coordinator, s *store.Store, w *worker.Worker, log *slog.Logger) *Server {
	return &Server{
		coord:  c,
		store:  s,
		worker: w,
		log:    log,
	}
}

// DispatchTask receives a task from a remote coordinator and submits it
// to this node's coordinator for local execution.
func (s *Server) DispatchTask(ctx context.Context, req *pb.DispatchTaskRequest) (*pb.DispatchTaskResponse, error) {
	task := protoToTask(req)

	if err := s.coord.AcceptDispatch(ctx, task); err != nil {
		return &pb.DispatchTaskResponse{
			Accepted: false,
			Error:    err.Error(),
		}, nil
	}

	s.log.Info("dispatched task accepted", "id", task.ID, "command", task.Command)
	return &pb.DispatchTaskResponse{Accepted: true, AssignedId: task.ID}, nil
}

// TaskResult returns the result of a task running on this node. If the task
// is not yet complete, returns NOT_FOUND.
func (s *Server) TaskResult(ctx context.Context, req *pb.TaskResultRequest) (*pb.TaskResultResponse, error) {
	t, err := s.coord.Get(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
	}

	if t.Status != model.TaskCompleted && t.Status != model.TaskFailed && t.Status != model.TaskCancelled && t.Status != model.TaskDeadLetter {
		return nil, status.Errorf(codes.NotFound, "task %s not yet terminal (status: %s)", req.TaskId, t.Status)
	}

	return &pb.TaskResultResponse{
		TaskId:      t.ID,
		ExitCode:    int32(t.ExitCode),
		Stdout:      t.Stdout,
		Stderr:      t.Stderr,
		Error:       t.Error,
		OutputRef:   t.OutputRef,
		OutputBytes: t.Metrics.OutputBytes,
		WallTimeNs:  int64(t.Metrics.WallTime),
	}, nil
}

// PullShard streams an object's data from this node to the caller.
func (s *Server) PullShard(req *pb.PullShardRequest, stream pb.ZigguratNode_PullShardServer) error {
	if err := store.ValidateHashHex(req.Hash); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid hash: %v", err)
	}
	hash := store.NormalizeHashHex(req.Hash)
	rc, err := s.store.GetByHash(stream.Context(), hash)
	if err != nil {
		return status.Errorf(codes.NotFound, "object not found: %s", req.Hash)
	}

	buf := make([]byte, chunkSize)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.ShardData{Data: buf[:n]}); sendErr != nil {
				rc.Close()
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			rc.Close()
			return status.Errorf(codes.Internal, "read object: %v", err)
		}
	}

	// Close checks the BLAKE3 integrity digest. If the on-disk blob was
	// corrupted, this returns an error — surface it so the caller knows
	// the data it received may be invalid.
	if err := rc.Close(); err != nil {
		return status.Errorf(codes.DataLoss, "integrity check failed: %v", err)
	}
	return nil
}

// PullECShard streams a single erasure-coded shard from this node.
// Used by cross-node EC reconstruction when a remote node lacks enough
// local shards to decode.
func (s *Server) PullECShard(req *pb.PullECShardRequest, stream pb.ZigguratNode_PullECShardServer) error {
	if err := store.ValidateHashHex(req.Hash); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid hash: %v", err)
	}
	hash := store.NormalizeHashHex(req.Hash)

	idx := int(req.ShardIndex)
	if idx < 0 {
		return status.Errorf(codes.InvalidArgument, "negative shard index")
	}

	path := store.ShardPath(s.store.Dir(), hash, idx)
	f, err := os.Open(path)
	if err != nil {
		return status.Errorf(codes.NotFound, "shard %d not found for %s", idx, req.Hash[:12])
	}
	defer f.Close()

	buf := make([]byte, chunkSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.ShardData{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read shard: %v", err)
		}
	}
	return nil
}

// PushShard receives an object's data from a remote node and stores it locally.
// The first message must carry the header (hash + size).
// When the header has is_ec_shard=true, the data is stored as an individual
// erasure-coded shard (via WriteSingleShard) and receiver-side ObjectMeta with
// ErasureParams is created so the node can eventually reconstruct the object.
func (s *Server) PushShard(stream pb.ZigguratNode_PushShardServer) error {
	// Read header.
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected header: %v", err)
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Errorf(codes.InvalidArgument, "first message must be header")
	}

	// Validate hash is exactly 32 hex-encoded bytes. This prevents
	// directory-traversal attacks via crafted hash strings containing
	// path separators or ".." sequences.
	if err := store.ValidateHashHex(hdr.Hash); err != nil {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("invalid hash: %v", err),
		})
	}

	hdr.Hash = store.NormalizeHashHex(hdr.Hash)

	if hdr.GetIsEcShard() {
		return s.pushECShard(stream, hdr)
	}
	return s.pushFullBlob(stream, hdr)
}

// pushECShard handles receiving an individual erasure-coded shard. Buffers in
// memory (shards are small — original_size / data_shards).
//
// Validates:
//   - erasure_meta is present and valid JSON
//   - shard_index is within [0, total_shards)
//   - received bytes match the expected shard hash (if ShardHashes are provided)
//
// Returns Ok:false if any validation fails or metadata creation errors.
func (s *Server) pushECShard(stream pb.ZigguratNode_PushShardServer, hdr *pb.PushShardHeader) error {
	// Parse and validate erasure_meta up front — we need it for validation.
	ecJSON := hdr.GetErasureMeta()
	if len(ecJSON) == 0 {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: "missing erasure_meta in EC shard push",
		})
	}
	var ecParams model.ErasureParams
	if err := json.Unmarshal(ecJSON, &ecParams); err != nil {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("invalid erasure_meta: %v", err),
		})
	}

	// Reject nonsensical shard counts — they would corrupt metadata.
	if ecParams.DataShards <= 0 || ecParams.ParityShards <= 0 {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("invalid shard counts: data=%d parity=%d", ecParams.DataShards, ecParams.ParityShards),
		})
	}

	// Bounds check shard_index.
	totalShards := ecParams.DataShards + ecParams.ParityShards
	idx := int(hdr.ShardIndex)
	if idx < 0 || idx >= totalShards {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("shard_index %d out of range [0, %d)", idx, totalShards),
		})
	}

	// Compute receive limit: use claimed ShardSize with headroom, but never
	// exceed the hard cap. This prevents unbounded memory consumption.
	limit := int64(maxECShardSize)
	if ecParams.ShardSize > 0 && ecParams.ShardSize*2 < limit {
		limit = ecParams.ShardSize * 2
	}

	// Read shard data with size enforcement.
	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if chunk := msg.GetData(); len(chunk) > 0 {
			if int64(buf.Len())+int64(len(chunk)) > limit {
				return stream.SendAndClose(&pb.PushShardResponse{
					Ok:    false,
					Error: fmt.Sprintf("shard data exceeds size limit (%d bytes)", limit),
				})
			}
			buf.Write(chunk)
		}
	}

	data := buf.Bytes()

	// Verify shard integrity against ShardHashes if available.
	if idx < len(ecParams.ShardHashes) && ecParams.ShardHashes[idx] != "" {
		hasher := blake3.New()
		hasher.Write(data)
		var actual [32]byte
		hasher.Sum(actual[:0])
		actualHex := hex.EncodeToString(actual[:])
		if actualHex != ecParams.ShardHashes[idx] {
			expected := ecParams.ShardHashes[idx]
			if len(expected) > 12 {
				expected = expected[:12]
			}
			return stream.SendAndClose(&pb.PushShardResponse{
				Ok:    false,
				Error: fmt.Sprintf("shard %d hash mismatch: expected %s, got %s", idx, expected, actualHex[:12]),
			})
		}
	}

	if err := store.WriteSingleShard(s.store.Dir(), hdr.Hash, idx, data); err != nil {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("write EC shard: %v", err),
		})
	}

	// Record actual bytes written, not the sender's claimed size.
	ecParams.ShardSize = int64(len(data))

	// Create receiver-side ObjectMeta with ErasureParams — fail if this errors
	// so the sender knows the shard state is incomplete. Clean up the shard
	// file on failure to avoid orphaning bytes that GC can't reclaim.
	if err := s.store.PutECShardReplica(hdr.Hash, &ecParams); err != nil {
		store.DeleteSingleShard(s.store.Dir(), hdr.Hash, idx)
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("create EC shard metadata: %v", err),
		})
	}

	s.log.Info("ec shard received", "hash", hdr.Hash[:12], "index", idx, "size", len(data))
	return stream.SendAndClose(&pb.PushShardResponse{Ok: true})
}

// pushFullBlob handles receiving a full object blob. Streams directly to disk
// via a pipe to avoid buffering the entire object in memory.
func (s *Server) pushFullBlob(stream pb.ZigguratNode_PushShardServer, hdr *pb.PushShardHeader) error {
	pr, pw := io.Pipe()

	var writeErr error
	var writeHash [32]byte
	var writeSize int64
	var writeCreated bool
	done := make(chan struct{})

	go func() {
		defer close(done)
		hash, size, created, werr := store.WriteBlob(s.store.Dir(), pr)
		if werr != nil {
			writeErr = werr
			return
		}
		writeHash = hash
		writeSize = size
		writeCreated = created
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			<-done
			return err
		}
		if chunk := msg.GetData(); len(chunk) > 0 {
			if _, err := pw.Write(chunk); err != nil {
				pw.CloseWithError(err)
				<-done
				return status.Errorf(codes.Internal, "write pipe: %v", err)
			}
		}
	}
	pw.Close()
	<-done

	if writeErr != nil {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: writeErr.Error(),
		})
	}

	gotHash := hex.EncodeToString(writeHash[:])
	if gotHash != hdr.Hash {
		// Only remove the blob if we created it. If WriteBlob deduplicated
		// against an existing file, deleting it would destroy a valid blob
		// that other metadata references.
		if writeCreated {
			store.DeleteBlob(s.store.Dir(), gotHash)
		}
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("hash mismatch: expected %s, got %s", hdr.Hash, gotHash),
		})
	}

	if err := s.store.PutReplica(gotHash, writeHash, writeSize); err != nil {
		if writeCreated {
			store.DeleteBlob(s.store.Dir(), gotHash)
		}
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("store replica metadata: %v", err),
		})
	}

	s.log.Info("shard received", "hash", hdr.Hash[:12], "size", hdr.Size)
	return stream.SendAndClose(&pb.PushShardResponse{Ok: true})
}

// RetireReplica handles a request from a remote node to retire a local replica.
// Validates the hash, then unpins and decrements the refcount so local GC can
// reclaim the object after its grace period.
func (s *Server) RetireReplica(ctx context.Context, req *pb.RetireReplicaRequest) (*pb.RetireReplicaResponse, error) {
	if err := store.ValidateHashHex(req.Hash); err != nil {
		return &pb.RetireReplicaResponse{Ok: false, Error: fmt.Sprintf("invalid hash: %v", err)}, nil
	}
	hash := store.NormalizeHashHex(req.Hash)
	if err := s.store.RetireReplica(hash); err != nil {
		return &pb.RetireReplicaResponse{Ok: false, Error: err.Error()}, nil
	}
	s.log.Info("replica retired", "hash", hash[:12])
	return &pb.RetireReplicaResponse{Ok: true}, nil
}

// CancelTask handles a remote cancel request by forwarding it to the local
// coordinator. The coordinator's Cancel method handles the state machine
// (QUEUED->CANCELLED, RUNNING->CANCELLING with SIGTERM, etc.).
func (s *Server) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.CancelTaskResponse, error) {
	if req.TaskId == "" {
		return &pb.CancelTaskResponse{Ok: false, Error: "empty task_id"}, nil
	}

	_, err := s.coord.Cancel(req.TaskId)
	if err != nil {
		// Task not found is not a hard error — it may have already completed
		// or been cleaned up. Return ok=true with a note.
		return &pb.CancelTaskResponse{Ok: true, Error: err.Error()}, nil
	}

	s.log.Info("task cancel propagated", "task", req.TaskId)
	return &pb.CancelTaskResponse{Ok: true}, nil
}

// protoToTask converts a DispatchTaskRequest to a model.Task.
func protoToTask(req *pb.DispatchTaskRequest) *model.Task {
	t := &model.Task{
		ID:            req.Id,
		Command:       req.Command,
		Env:           req.Env,
		InputRefs:     req.InputRefs,
		Artifacts:     req.Artifacts,
		ArtifactNames: req.ArtifactNames,
		Params:        req.Params,
		Requires:      req.Requires,
		Constraints:   req.Constraints,
		Image:         req.Image,
		Config: model.TaskConfig{
			Priority:      int(req.Priority),
			Timeout:       model.Duration(time.Duration(req.TimeoutNs)),
			MaxRetries:    int(req.MaxRetries),
			MaxOutputSize: req.MaxOutputSize,
			Affinity:      req.Affinity,
			KeepWorkspace: req.KeepWorkspace,
		},
		Attempt: int(req.Attempt),
	}
	if e := req.Environment; e != nil {
		t.Environment = &model.TaskEnvironment{
			Name:        e.Name,
			Setup:       e.Setup,
			Fingerprint: e.Fingerprint,
		}
	}
	if r := req.Resources; r != nil {
		t.Resources = model.ResourceReq{
			Memory:   r.Memory,
			CPUCores: int(r.CpuCores),
			GPUs:     int(r.Gpus),
		}
	}
	return t
}

// taskToProto converts a model.Task to a DispatchTaskRequest.
func taskToProto(t *model.Task) *pb.DispatchTaskRequest {
	req := &pb.DispatchTaskRequest{
		Id:            t.ID,
		Command:       t.Command,
		Env:           t.Env,
		InputRefs:     t.InputRefs,
		Artifacts:     t.Artifacts,
		ArtifactNames: t.ArtifactNames,
		Params:        t.Params,
		Requires:      t.Requires,
		Constraints:   t.Constraints,
		Image:         t.Image,
		Priority:      int32(t.Config.Priority),
		TimeoutNs:     int64(t.Config.Timeout),
		MaxRetries:    int32(t.Config.MaxRetries),
		MaxOutputSize: t.Config.MaxOutputSize,
		Affinity:      t.Config.Affinity,
		KeepWorkspace: t.Config.KeepWorkspace,
		Attempt:       int32(t.Attempt),
	}
	if e := t.Environment; e != nil {
		req.Environment = &pb.TaskEnvironment{
			Name:        e.Name,
			Setup:       e.Setup,
			Fingerprint: e.Fingerprint,
		}
	}
	if r := t.Resources; r.Memory > 0 || r.CPUCores > 0 || r.GPUs > 0 {
		req.Resources = &pb.TaskResources{
			Memory:   r.Memory,
			CpuCores: int32(r.CPUCores),
			Gpus:     int32(r.GPUs),
		}
	}
	return req
}
