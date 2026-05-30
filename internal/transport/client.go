package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/transport/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Client connects to a remote ZigguratNode gRPC server.
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // addr -> conn
}

// NewClient creates a new transport client with a connection cache.
func NewClient() *Client {
	return &Client{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Close closes all cached connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		conn.Close()
		delete(c.conns, addr)
	}
	return nil
}

// conn returns a cached or new connection to the given address.
// Stale connections (SHUTDOWN state) are evicted and replaced.
func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok := c.conns[addr]; ok {
		state := cc.GetState()
		if state == connectivity.Shutdown {
			cc.Close()
			delete(c.conns, addr)
		} else {
			return cc, nil
		}
	}

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	c.conns[addr] = cc
	return cc, nil
}

// EvictAddr removes and closes the cached connection for the given address.
// Called when a node departs the cluster so stale connections don't accumulate.
func (c *Client) EvictAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[addr]; ok {
		cc.Close()
		delete(c.conns, addr)
	}
}

// DispatchTask sends a task to a remote node for execution. Returns the
// task ID assigned by the receiving node's coordinator.
func (c *Client) DispatchTask(ctx context.Context, addr string, task *model.Task) (string, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return "", err
	}

	client := pb.NewZigguratNodeClient(cc)
	resp, err := client.DispatchTask(ctx, taskToProto(task))
	if err != nil {
		return "", fmt.Errorf("dispatch task to %s: %w", addr, err)
	}
	if !resp.Accepted {
		return "", fmt.Errorf("task rejected by %s: %s", addr, resp.Error)
	}
	return resp.AssignedId, nil
}

// TaskResult fetches the result of a task from a remote node.
func (c *Client) TaskResult(ctx context.Context, addr string, taskID string) (*pb.TaskResultResponse, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewZigguratNodeClient(cc)
	return client.TaskResult(ctx, &pb.TaskResultRequest{TaskId: taskID})
}

// FetchResult retrieves the result of a completed task from a remote node.
// Returns an error if the task is not yet terminal.
func (c *Client) FetchResult(ctx context.Context, addr string, taskID string) (*coord.DispatchResult, error) {
	resp, err := c.TaskResult(ctx, addr, taskID)
	if err != nil {
		return nil, err
	}
	return &coord.DispatchResult{
		ExitCode:    int(resp.ExitCode),
		Stdout:      resp.Stdout,
		Stderr:      resp.Stderr,
		Error:       resp.Error,
		OutputRef:   resp.OutputRef,
		OutputBytes: resp.OutputBytes,
	}, nil
}

// RetireReplica tells a remote node to release its hold on a replica.
// Returns an error if the RPC fails. The GC callback uses the error to
// defer local metadata deletion and retry next sweep, preventing
// permanently orphaned replicas on peers.
func (c *Client) RetireReplica(ctx context.Context, addr string, hash string) error {
	cc, err := c.conn(addr)
	if err != nil {
		return err
	}
	client := pb.NewZigguratNodeClient(cc)
	resp, err := client.RetireReplica(ctx, &pb.RetireReplicaRequest{Hash: hash})
	if err != nil {
		return fmt.Errorf("retire replica on %s: %w", addr, err)
	}
	if !resp.Ok {
		return fmt.Errorf("retire replica rejected by %s: %s", addr, resp.Error)
	}
	return nil
}

// CancelTask sends a cancel request to a remote node for a specific task.
// The remote node's coordinator handles the state transition and process
// termination (SIGTERM -> grace -> SIGKILL).
func (c *Client) CancelTask(ctx context.Context, addr string, taskID string) error {
	cc, err := c.conn(addr)
	if err != nil {
		return err
	}
	client := pb.NewZigguratNodeClient(cc)
	resp, err := client.CancelTask(ctx, &pb.CancelTaskRequest{TaskId: taskID})
	if err != nil {
		return fmt.Errorf("cancel task on %s: %w", addr, err)
	}
	if !resp.Ok {
		return fmt.Errorf("cancel task rejected by %s: %s", addr, resp.Error)
	}
	return nil
}

// PullObject downloads an object by content hash from a remote node.
// Implements coord.TaskDispatcher.
func (c *Client) PullObject(ctx context.Context, addr string, hash string) (io.ReadCloser, error) {
	return c.PullShard(ctx, addr, hash)
}

// PullShard downloads an object from a remote node and returns a reader.
func (c *Client) PullShard(ctx context.Context, addr string, hash string) (io.ReadCloser, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}

	// Wrap context with cancel so Close() can release the gRPC stream
	// immediately instead of waiting for the parent context to expire.
	streamCtx, cancel := context.WithCancel(ctx)

	client := pb.NewZigguratNodeClient(cc)
	stream, err := client.PullShard(streamCtx, &pb.PullShardRequest{Hash: hash})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("pull shard from %s: %w", addr, err)
	}

	return &streamReader{stream: stream, cancel: cancel}, nil
}

// PullECShard downloads a single erasure-coded shard from a remote node.
// Returns the raw shard bytes. Used by cross-node EC reconstruction.
func (c *Client) PullECShard(ctx context.Context, addr string, hash string, shardIndex int) ([]byte, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewZigguratNodeClient(cc)
	stream, err := client.PullECShard(ctx, &pb.PullECShardRequest{
		Hash:       hash,
		ShardIndex: int32(shardIndex),
	})
	if err != nil {
		return nil, fmt.Errorf("pull EC shard from %s: %w", addr, err)
	}

	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("recv EC shard from %s: %w", addr, err)
		}
		if int64(buf.Len())+int64(len(msg.Data)) > maxECShardSize {
			return nil, fmt.Errorf("EC shard from %s exceeds size limit (%d bytes)", addr, maxECShardSize)
		}
		buf.Write(msg.Data)
	}
	return buf.Bytes(), nil
}

// PushShard uploads an object to a remote node.
func (c *Client) PushShard(ctx context.Context, addr string, hash string, size int64, r io.Reader) error {
	return c.pushData(ctx, addr, &pb.PushShardHeader{
		Hash: hash,
		Size: size,
	}, r)
}

// PushECShard uploads a single erasure-coded shard to a remote node.
// ecMeta is JSON-serialized ErasureParams so the receiver can create metadata.
func (c *Client) PushECShard(ctx context.Context, addr string, hash string, shardIndex int, data []byte, ecMeta []byte) error {
	return c.pushData(ctx, addr, &pb.PushShardHeader{
		Hash:        hash,
		Size:        int64(len(data)),
		ShardIndex:  int32(shardIndex),
		IsEcShard:   true,
		ErasureMeta: ecMeta,
	}, bytes.NewReader(data))
}

// pushData sends a PushShard stream with the given header and data.
func (c *Client) pushData(ctx context.Context, addr string, hdr *pb.PushShardHeader, r io.Reader) error {
	cc, err := c.conn(addr)
	if err != nil {
		return err
	}

	client := pb.NewZigguratNodeClient(cc)
	stream, err := client.PushShard(ctx)
	if err != nil {
		return fmt.Errorf("push shard to %s: %w", addr, err)
	}

	// Send header first.
	if err := stream.Send(&pb.PushShardMsg{
		Payload: &pb.PushShardMsg_Header{
			Header: hdr,
		},
	}); err != nil {
		return fmt.Errorf("send header: %w", err)
	}

	// Stream data chunks.
	buf := make([]byte, chunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.PushShardMsg{
				Payload: &pb.PushShardMsg_Data{
					Data: buf[:n],
				},
			}); sendErr != nil {
				return fmt.Errorf("send chunk: %w", sendErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read data: %w", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("close push stream: %w", err)
	}
	if !resp.Ok {
		return fmt.Errorf("push shard rejected: %s", resp.Error)
	}
	return nil
}

// streamReader wraps a PullShard gRPC stream as an io.ReadCloser.
type streamReader struct {
	stream pb.ZigguratNode_PullShardClient
	cancel context.CancelFunc
	buf    []byte
	pos    int
}

func (sr *streamReader) Read(p []byte) (int, error) {
	for sr.pos >= len(sr.buf) {
		msg, err := sr.stream.Recv()
		if err == io.EOF {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		sr.buf = msg.Data
		sr.pos = 0
	}

	n := copy(p, sr.buf[sr.pos:])
	sr.pos += n
	return n, nil
}

func (sr *streamReader) Close() error {
	// Cancel the stream context to release gRPC resources immediately
	// instead of waiting for the parent context to expire.
	if sr.cancel != nil {
		sr.cancel()
	}
	return nil
}
