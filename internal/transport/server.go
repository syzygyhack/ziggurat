package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"github.com/syzygyhack/ziggurat/internal/transport/pb"
	"github.com/syzygyhack/ziggurat/internal/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chunkSize = 256 * 1024 // 256 KB per streaming message

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
	rc, err := s.store.GetByHash(stream.Context(), req.Hash)
	if err != nil {
		return status.Errorf(codes.NotFound, "object not found: %s", req.Hash)
	}
	defer rc.Close()

	buf := make([]byte, chunkSize)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.ShardData{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read object: %v", err)
		}
	}
	return nil
}

// PushShard receives an object's data from a remote node and stores it locally.
// The first message must carry the header (hash + size).
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

	// Collect data from stream into a pipe that feeds WriteBlob.
	pr, pw := io.Pipe()

	var writeErr error
	var writeHash [32]byte
	var writeSize int64
	done := make(chan struct{})

	go func() {
		defer close(done)
		hash, size, werr := store.WriteBlob(s.store.Dir(), pr)
		if werr != nil {
			writeErr = werr
			return
		}
		writeHash = hash
		writeSize = size
	}()

	// Stream data chunks into the pipe.
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
		chunk := msg.GetData()
		if len(chunk) == 0 {
			continue
		}
		if _, err := pw.Write(chunk); err != nil {
			pw.CloseWithError(err)
			<-done
			return status.Errorf(codes.Internal, "write pipe: %v", err)
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

	// Verify hash matches claimed hash.
	gotHash := fmt.Sprintf("%x", writeHash)
	if gotHash != hdr.Hash {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("hash mismatch: expected %s, got %s", hdr.Hash, gotHash),
		})
	}

	// Create receiver-side ObjectMeta so the replica is visible to GC,
	// Stats, and NodesForHash.
	if err := s.store.PutReplica(gotHash, writeHash, writeSize); err != nil {
		return stream.SendAndClose(&pb.PushShardResponse{
			Ok:    false,
			Error: fmt.Sprintf("store replica metadata: %v", err),
		})
	}

	s.log.Info("shard received", "hash", hdr.Hash[:12], "size", hdr.Size)
	return stream.SendAndClose(&pb.PushShardResponse{Ok: true})
}

// protoToTask converts a DispatchTaskRequest to a model.Task.
func protoToTask(req *pb.DispatchTaskRequest) *model.Task {
	t := &model.Task{
		ID:        req.Id,
		Command:   req.Command,
		Env:       req.Env,
		InputRefs: req.InputRefs,
		Artifacts: req.Artifacts,
		Params:    req.Params,
		Requires:  req.Requires,
		Constraints: req.Constraints,
		Image:     req.Image,
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

// taskResultToBytes is a helper that reads a PullShard response into a byte slice.
// Used internally for small objects; large objects should use streaming directly.
func readAllChunks(stream pb.ZigguratNode_PullShardClient) ([]byte, error) {
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		buf.Write(chunk.Data)
	}
}
