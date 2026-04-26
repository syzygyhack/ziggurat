package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/syzygyhack/ziggurat/internal/api"
	"github.com/syzygyhack/ziggurat/internal/cluster"
	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/coord"
	"github.com/syzygyhack/ziggurat/internal/metrics"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/scheduler"
	"github.com/syzygyhack/ziggurat/internal/store"
	"github.com/syzygyhack/ziggurat/internal/transport"
	"github.com/syzygyhack/ziggurat/internal/transport/pb"
	"github.com/syzygyhack/ziggurat/internal/worker"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
)

// Node is the top-level lifecycle object that owns all subsystems.
type Node struct {
	cfg    *config.Config
	log    *slog.Logger
	nodeID string

	store      *store.Store
	coord      *coord.Coordinator
	worker     *worker.Worker
	api        *api.Server
	gc         *store.GC
	cluster    *cluster.Cluster
	replicator *store.Replicator
	grpcSrv    *grpc.Server
	transport  *transport.Client
	tasksDB    *bbolt.DB

	wg               sync.WaitGroup
	cancelWorker     context.CancelFunc
	cancelGC         context.CancelFunc
	cancelRepair     context.CancelFunc
	cancelMetrics    context.CancelFunc
	cancelDispatch   context.CancelFunc
	cancelCapRefresh context.CancelFunc
}

// registryPeerProvider adapts the cluster registry to store.PeerProvider.
type registryPeerProvider struct {
	registry *cluster.Registry
}

func (p *registryPeerProvider) Peers(exclude string) []store.Peer {
	nodes := p.registry.List()
	var peers []store.Peer
	for _, n := range nodes {
		if n.ID == exclude || n.GRPCAddress == "" {
			continue
		}
		peers = append(peers, store.Peer{NodeID: n.ID, Addr: n.GRPCAddress})
	}
	return peers
}

// registryShardFetcher adapts transport.Client + cluster.Registry to
// store.ShardFetcher. Translates nodeIDs to gRPC addresses via the registry
// before fetching.
type registryShardFetcher struct {
	client   *transport.Client
	registry *cluster.Registry
}

func (f *registryShardFetcher) FetchShard(ctx context.Context, nodeID string, hashHex string, shardIndex int) ([]byte, error) {
	node, ok := f.registry.Get(nodeID)
	if !ok || node.GRPCAddress == "" {
		return nil, fmt.Errorf("no address for node %s", nodeID)
	}
	return f.client.PullECShard(ctx, node.GRPCAddress, hashHex, shardIndex)
}

// Start initializes all subsystems and begins serving.
func Start(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Node, error) {
	dataDir := cfg.Node.DataDir
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	nodeID, err := LoadOrCreateID(dataDir)
	if err != nil {
		return nil, fmt.Errorf("node identity: %w", err)
	}
	role := cfg.Node.Role
	if role == "" {
		role = "hybrid"
	}
	log.Info("node starting", "id", nodeID, "role", role, "data_dir", dataDir)

	// Initialize store.
	s, err := store.New(cfg.Storage, dataDir, log.With("component", "store"))
	if err != nil {
		return nil, fmt.Errorf("init store: %w", err)
	}

	// Open tasks DB for coordinator persistence.
	metaDir := filepath.Join(dataDir, "metadata")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		s.Close()
		return nil, fmt.Errorf("create metadata dir: %w", err)
	}
	tasksDB, err := bbolt.Open(filepath.Join(metaDir, "tasks.db"), 0o644, nil)
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("open tasks db: %w", err)
	}

	persist, err := coord.NewPersist(tasksDB)
	if err != nil {
		tasksDB.Close()
		s.Close()
		return nil, fmt.Errorf("init persistence: %w", err)
	}

	// Initialize coordinator and recover persisted tasks.
	defaults := coord.TaskDefaults{
		MaxRetries: cfg.Resilience.TaskRetries,
		Timeout:    cfg.Compute.TaskTimeout,
		DeadLetter: cfg.Resilience.DeadLetter,
	}
	c := coord.New(s, persist, defaults, log.With("component", "coord"))
	if err := c.Recover(); err != nil {
		log.Warn("task recovery failed", "err", err)
	}

	// Detect and merge node capabilities.
	caps := DetectCapabilities(dataDir)
	caps = MergeCapabilities(caps, cfg.Node.Capabilities)
	log.Info("node capabilities", "caps", caps)

	// Initialize worker.
	w := worker.New(nodeID, cfg.Node.Tags, caps, s, c, cfg.Compute, dataDir, log.With("component", "worker"))

	// Initialize GC.
	gc := store.NewGC(s, cfg.Storage.GCGracePeriod, log.With("component", "gc"))

	// Start cluster gossip.
	clusterCfg := cluster.ConfigFromNode(nodeID, cfg.Node, cfg.Network, cfg.Cluster, caps)
	cl, err := cluster.New(clusterCfg, log.With("component", "cluster"))
	if err != nil {
		log.Warn("cluster: gossip disabled", "err", err)
		cl = nil
	}

	// Forward-declare replicator so the OnLeave closure can reference it.
	// It is initialized after gRPC transport is set up (below).
	var repl *store.Replicator

	// Create repair context early so the OnLeave closure can use it
	// instead of the parent Start context. This ensures triggered repairs
	// are cancelled properly during shutdown.
	repairCtx, cancelRepair := context.WithCancel(ctx)

	// When a node departs, re-queue any tasks it was running and trigger
	// an immediate repair pass for under-replicated objects.
	if cl != nil {
		cl.Registry.OnLeave(func(departedID string) {
			n := c.RequeueByWorker(departedID)
			if n > 0 {
				log.Info("requeued tasks from departed node", "node", departedID, "count", n)
			}
			if repl != nil {
				// Purge stale placements BEFORE repair so the repair pass
				// doesn't count the departed node toward the replication factor.
				repl.RemoveNodePlacements(departedID)
				repl.TriggerRepair(repairCtx)
			}
		})

		// When a new node joins, rebalance local objects to it if the
		// hash ring now assigns them there.
		cl.Registry.OnJoin(func(joinedID string) {
			if joinedID == nodeID {
				return // skip self
			}
			if repl != nil {
				go func() {
					n := repl.Rebalance(repairCtx, joinedID)
					if n > 0 {
						log.Info("rebalanced objects to new node", "node", joinedID, "count", n)
					}
				}()
			}
		})
	}

	// Initialize API server. Use storage capacity as upload limit if set,
	// otherwise fall back to max_output_size (the largest single object a
	// task can produce).
	maxUpload := cfg.Storage.Capacity
	if maxUpload == 0 {
		maxUpload = cfg.Compute.MaxOutputSize
	}
	var apiSrv *api.Server
	if cl != nil {
		apiSrv = api.NewCluster(c, s, cl.Registry, log.With("component", "api"), maxUpload)
	} else {
		apiSrv = api.NewWithOptions(c, s, log.With("component", "api"), maxUpload)
	}
	apiSrv.SetRole(role)

	// Initialize gRPC transport server and client.
	grpcSrv := grpc.NewServer()
	tSrv := transport.NewServer(c, s, w, log.With("component", "grpc"))
	pb.RegisterZigguratNodeServer(grpcSrv, tSrv)
	tClient := transport.NewClient()

	// Wire storage replication. The Replicator uses the transport client to
	// push shards and the cluster registry to discover peers.
	if cl != nil {
		pp := &registryPeerProvider{registry: cl.Registry}
		repl = store.NewReplicator(s, nodeID, cfg.Storage.ReplicationFactor, tClient, pp, log.With("component", "replicator"))
		repl.SetECPusher(tClient)
		repl.SetRing(cl.Registry.Ring)
		s.SetOnPut(func(ctx context.Context, hashHex string) {
			if err := repl.AfterPut(ctx, hashHex); err != nil {
				log.Warn("replication failed", "hash", hashHex[:12], "err", err)
			}
		})
		apiSrv.SetUnderReplicated(repl.UnderReplicatedCount)

		// Wire drain callback: migrate local shards to peers before shutdown.
		replForDrain := repl
		apiSrv.SetOnDrain(func() {
			n := replForDrain.MigrateAll(repairCtx)
			if n > 0 {
				log.Info("drain: migrated objects to peers", "count", n)
			}
		})

		// Wire shard fetcher for cross-node EC reconstruction.
		s.SetShardFetcher(&registryShardFetcher{
			client:   tClient,
			registry: cl.Registry,
		})
	}

	// Wire GC retirement callback: when an object is collected locally,
	// notify peer nodes that held replicas to retire them. Returns false
	// if any reachable peer fails retirement, causing GC to defer deletion
	// and retry next sweep (prevents permanently pinned orphan replicas).
	if cl != nil {
		gcClient := tClient
		gcPeers := &registryPeerProvider{registry: cl.Registry}
		gc.SetOnCollect(func(hashHex string, shards []model.ShardPlacement) bool {
			peers := gcPeers.Peers(nodeID)
			addrMap := make(map[string]string, len(peers))
			for _, p := range peers {
				addrMap[p.NodeID] = p.Addr
			}
			allOK := true
			for _, s := range shards {
				if s.NodeID == nodeID {
					continue
				}
				addr, ok := addrMap[s.NodeID]
				if !ok || addr == "" {
					// Node is not in the cluster — it left or was removed.
					// Its placements were already purged by RemoveNodePlacements.
					continue
				}
				// Short timeout so an unreachable peer doesn't stall the
				// GC sweep. The peer's own GC handles cleanup eventually.
				retireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := gcClient.RetireReplica(retireCtx, addr, hashHex); err != nil {
					log.Warn("retire replica failed", "hash", hashHex[:12], "peer", addr, "err", err)
					allOK = false
				}
				cancel()
			}
			return allOK
		})
	}

	// Initialize pipeline manager (uses the tasks DB for persistence).
	pm, err := coord.NewPipelineManager(c, tasksDB, log.With("component", "pipeline"))
	if err != nil {
		log.Warn("pipeline manager init failed", "err", err)
	} else {
		apiSrv.SetPipelineManager(pm)
		c.SetOnComplete(pm.OnTaskComplete)
		// Reconcile recovered pipelines with coordinator task state and
		// reschedule any stages that were interrupted by the restart.
		pm.RecoverPipelines(ctx)
	}

	n := &Node{
		cfg:        cfg,
		log:        log,
		nodeID:     nodeID,
		store:      s,
		coord:      c,
		worker:     w,
		api:        apiSrv,
		gc:         gc,
		cluster:    cl,
		replicator: repl,
		grpcSrv:    grpcSrv,
		transport:  tClient,
		tasksDB:    tasksDB,
	}

	// Start worker loop (skip for coordinator-only nodes).
	if role != "coordinator" {
		workerCtx, cancelWorker := context.WithCancel(ctx)
		n.cancelWorker = cancelWorker
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			w.Run(workerCtx)
		}()
	} else {
		log.Info("coordinator role: worker loop disabled")
	}

	// Start cross-node dispatch loop (coordinator and hybrid only, requires cluster).
	if role != "worker" && cl != nil {
		var locator scheduler.ObjectLocator
		if repl != nil {
			locator = repl
		}
		dispatcher := coord.NewDispatcher(
			c, cl.Registry, tClient, locator, s,
			nodeID, cfg.Node.Tags, caps,
			role == "coordinator",
			log.With("component", "dispatch"),
		)
		dispCtx, cancelDispatch := context.WithCancel(ctx)
		n.cancelDispatch = cancelDispatch
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			dispatcher.Run(dispCtx)
		}()
	}

	// Start GC loop (scan every 5 minutes).
	gcCtx, cancelGC := context.WithCancel(ctx)
	n.cancelGC = cancelGC
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		gc.Run(gcCtx, 5*time.Minute)
	}()

	// Start storage repair loop (re-replicate under-replicated objects every 2 minutes).
	// repairCtx/cancelRepair were created earlier so the OnLeave closure can share them.
	n.cancelRepair = cancelRepair
	if repl != nil {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			repl.RunRepairLoop(repairCtx, 2*time.Minute)
		}()
	}

	// Start periodic metrics refresh (store stats, cluster node count).
	metricsCtx, cancelMetrics := context.WithCancel(ctx)
	n.cancelMetrics = cancelMetrics
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.refreshMetrics(metricsCtx, s, cl)
	}()

	// Refresh volatile capabilities (disk.avail) and re-broadcast via gossip.
	capRefreshCtx, cancelCapRefresh := context.WithCancel(ctx)
	n.cancelCapRefresh = cancelCapRefresh
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.refreshCapabilities(capRefreshCtx, caps, cl)
	}()

	// cleanupOnFail shuts down cluster gossip (if started) along with
	// background goroutines and databases. Called from error paths below.
	cleanupOnFail := func() {
		n.cancelBackground()
		if cl != nil {
			cl.Leave()
			cl.Shutdown()
		}
		tasksDB.Close()
		s.Close()
	}

	// Bind HTTP API port synchronously so startup fails if port is taken.
	listenAddr := api.Addr(cfg.Network.Bind, cfg.Network.HTTPPort)
	ln, err := apiSrv.Listen(listenAddr)
	if err != nil {
		cleanupOnFail()
		return nil, fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	// Serve HTTP in background now that we know the port is bound.
	go func() {
		if err := apiSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("api server error", "err", err)
		}
	}()

	// Bind gRPC port for inter-node transport.
	grpcAddr := fmt.Sprintf("%s:%d", cfg.Network.Bind, cfg.Network.GRPCPort)
	grpcLn, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		apiSrv.Shutdown(ctx)
		cleanupOnFail()
		return nil, fmt.Errorf("listen grpc %s: %w", grpcAddr, err)
	}
	go func() {
		if err := grpcSrv.Serve(grpcLn); err != nil {
			log.Error("grpc server error", "err", err)
		}
	}()

	// Resolve the join address other nodes should use to enter the cluster.
	// Prefer the gossip bind from memberlist (which resolves the real IP)
	// over the raw config, which may be 0.0.0.0.
	joinAddr := ""
	if cl != nil {
		joinAddr = cl.LocalAddr()
	}

	log.Info("node ready", "id", nodeID, "http", ln.Addr().String(), "grpc", grpcLn.Addr().String())
	fmt.Fprintf(os.Stderr, "\nZiggurat node ready.\n  ID:   %s\n  Role: %s\n  HTTP: %s\n  gRPC: %s\n",
		nodeID, role, ln.Addr().String(), grpcLn.Addr().String())
	if joinAddr != "" {
		fmt.Fprintf(os.Stderr, "  Join: %s\n", joinAddr)
	}
	fmt.Fprintln(os.Stderr)
	return n, nil
}

// Shutdown gracefully stops all subsystems. It stops accepting new
// requests first, then cancels background goroutines, waits for them
// to finish, and only then closes databases.
func (n *Node) Shutdown(ctx context.Context) error {
	n.log.Info("node shutting down")

	// Stop accepting new HTTP requests.
	if err := n.api.Shutdown(ctx); err != nil {
		n.log.Error("api shutdown error", "err", err)
	}

	// Stop gRPC transport.
	if n.grpcSrv != nil {
		n.grpcSrv.GracefulStop()
	}
	if n.transport != nil {
		n.transport.Close()
	}

	// Leave cluster gracefully so other nodes see a clean departure.
	if n.cluster != nil {
		if err := n.cluster.Leave(); err != nil {
			n.log.Error("cluster leave error", "err", err)
		}
		n.cluster.Shutdown()
	}

	// Cancel all background goroutines, then wait for in-flight work to drain.
	n.cancelBackground()

	// Wait for any in-flight async repair goroutines spawned by TriggerRepair
	// (e.g. from OnLeave callbacks) before closing databases.
	if n.replicator != nil {
		n.replicator.WaitForRepairs()
	}

	// Safe to close databases now that all goroutines have exited.
	n.tasksDB.Close()
	n.store.Close()

	n.log.Info("node stopped")
	return nil
}

// cancelBackground cancels all background goroutines and waits for them to finish.
// Used by both Shutdown and Start error paths.
func (n *Node) cancelBackground() {
	if n.cancelWorker != nil {
		n.cancelWorker()
	}
	if n.cancelGC != nil {
		n.cancelGC()
	}
	if n.cancelRepair != nil {
		n.cancelRepair()
	}
	if n.cancelMetrics != nil {
		n.cancelMetrics()
	}
	if n.cancelDispatch != nil {
		n.cancelDispatch()
	}
	if n.cancelCapRefresh != nil {
		n.cancelCapRefresh()
	}
	n.wg.Wait()
}

// refreshMetrics periodically updates Prometheus gauges for store and cluster stats.
func (n *Node) refreshMetrics(ctx context.Context, s *store.Store, cl *cluster.Cluster) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := s.Stats()
			metrics.StoreObjects.Set(float64(stats.Objects))
			metrics.StoreBytes.Set(float64(stats.UsedBytes))
			if cl != nil {
				nodes := cl.Registry.List()
				metrics.NodesTotal.WithLabelValues("all").Set(float64(len(nodes)))
				roleCounts := map[string]int{}
				for _, nd := range nodes {
					roleCounts[nd.Role.String()]++
				}
				for _, role := range []string{"hybrid", "coordinator", "worker"} {
					metrics.NodesTotal.WithLabelValues(role).Set(float64(roleCounts[role]))
				}
			}
		}
	}
}

// refreshCapabilities periodically updates volatile capabilities (disk.avail)
// and re-broadcasts metadata via cluster gossip so other nodes see current values.
func (n *Node) refreshCapabilities(ctx context.Context, caps map[string]string, cl *cluster.Cluster) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			RefreshDiskAvail(caps, n.cfg.Node.DataDir)
			if cl != nil {
				cl.UpdateMeta(caps, n.cfg.Node.Tags)
			}
		}
	}
}
