package transport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/transport/pb"
	"google.golang.org/grpc"
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
func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	c.conns[addr] = cc
	return cc, nil
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

	client := pb.NewZigguratNodeClient(cc)
	stream, err := client.PullShard(ctx, &pb.PullShardRequest{Hash: hash})
	if err != nil {
		return nil, fmt.Errorf("pull shard from %s: %w", addr, err)
	}

	return &streamReader{stream: stream}, nil
}

// PushShard uploads an object to a remote node.
func (c *Client) PushShard(ctx context.Context, addr string, hash string, size int64, r io.Reader) error {
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
			Header: &pb.PushShardHeader{
				Hash: hash,
				Size: size,
			},
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
	// gRPC streams don't have a Close; they end when Recv returns EOF.
	return nil
}
