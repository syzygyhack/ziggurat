package api

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
	"github.com/syzygyhack/ziggurat/internal/worker"
)

// NodeLister provides cluster node information to the API.
// nil means single-node mode (Phase 0a).
type NodeLister interface {
	List() []*model.Node
	Count() int
}

// Server is the HTTP API server for Ziggurat.
type Server struct {
	router           *chi.Mux
	srv              *http.Server
	coord            *coord.Coordinator
	store            *store.Store
	nodes            NodeLister
	log              *slog.Logger
	startTime        time.Time
	maxUploadSize    int64      // 0 = no limit
	underReplicated  func() int // optional; returns under-replicated object count
	onDrain          func()     // optional; triggers shard migration on drain
	pipelines        *coord.PipelineManager
	role             string     // "hybrid", "coordinator", "worker"
	logBroadcaster   *worker.LogBroadcaster
}

// New creates an API server (single-node mode).
func New(c *coord.Coordinator, s *store.Store, log *slog.Logger) *Server {
	return NewWithOptions(c, s, log, 0)
}

// NewWithOptions creates an API server with configurable upload size limit.
// maxUploadSize of 0 means no limit. nodes may be nil (single-node mode).
func NewWithOptions(c *coord.Coordinator, s *store.Store, log *slog.Logger, maxUploadSize int64) *Server {
	return newServer(c, s, nil, log, maxUploadSize)
}

// NewCluster creates an API server with cluster node awareness.
func NewCluster(c *coord.Coordinator, s *store.Store, nodes NodeLister, log *slog.Logger, maxUploadSize int64) *Server {
	return newServer(c, s, nodes, log, maxUploadSize)
}

func newServer(c *coord.Coordinator, s *store.Store, nodes NodeLister, log *slog.Logger, maxUploadSize int64) *Server {
	srv := &Server{
		router:        chi.NewRouter(),
		coord:         c,
		store:         s,
		nodes:         nodes,
		log:           log,
		startTime:     time.Now(),
		maxUploadSize: maxUploadSize,
	}

	srv.router.Use(middleware.RequestID)
	srv.router.Use(middleware.RealIP)
	srv.router.Use(requestLogger(log))
	srv.router.Use(middleware.Recoverer)
	// Rate limit: 100 req/s with burst of 200 per IP.
	srv.router.Use(NewRateLimiter(100, 200).Middleware)

	RegisterRoutes(srv.router, srv)

	return srv
}

// SetRole sets the node's role for API gating. Worker-only nodes reject
// task and pipeline submissions since they have no local scheduler to
// dispatch queued work.
func (s *Server) SetRole(role string) {
	s.role = role
}

// requireCoordinator returns true if the node can accept task/pipeline
// submissions (hybrid or coordinator). Returns false and writes a 503
// for worker-only nodes.
func (s *Server) requireCoordinator(w http.ResponseWriter) bool {
	if s.role == "worker" {
		writeError(w, http.StatusServiceUnavailable,
			"this node is a worker; submit tasks to a coordinator or hybrid node")
		return false
	}
	return true
}

// SetPipelineManager registers the pipeline manager for pipeline API endpoints.
func (s *Server) SetPipelineManager(pm *coord.PipelineManager) {
	s.pipelines = pm
}

// SetUnderReplicated registers a callback that returns the count of
// under-replicated objects. Used by the health endpoint.
func (s *Server) SetUnderReplicated(fn func() int) {
	s.underReplicated = fn
}

// SetOnDrain registers a callback triggered when the /drain endpoint is
// called. Used to start shard migration to peer nodes.
func (s *Server) SetOnDrain(fn func()) {
	s.onDrain = fn
}

// SetLogBroadcaster registers the log broadcaster for SSE log streaming.
// When set, the /api/v1/tasks/:id/logs endpoint streams live stdout/stderr.
func (s *Server) SetLogBroadcaster(lb *worker.LogBroadcaster) {
	s.logBroadcaster = lb
}

// Start begins listening on the given address.
func (s *Server) Start(addr string) error {
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}
	s.log.Info("http api listening", "addr", addr)
	return s.srv.ListenAndServe()
}

// Listen binds the address and returns the listener without serving.
// Use Serve(ln) to start accepting connections.
func (s *Server) Listen(addr string) (net.Listener, error) {
	s.srv = &http.Server{
		Handler: s.router,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s.log.Info("http api listening", "addr", ln.Addr().String())
	return ln, nil
}

// Serve accepts connections on an already-bound listener.
func (s *Server) Serve(ln net.Listener) error {
	return s.srv.Serve(ln)
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"req_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}

// Addr returns the server's listen address string.
func Addr(bind string, port int) string {
	return fmt.Sprintf("%s:%d", bind, port)
}
