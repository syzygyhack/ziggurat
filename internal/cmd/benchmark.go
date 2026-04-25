package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/benchmark"
	"github.com/syzygyhack/ziggurat/internal/config"
)

var benchSkipNetwork bool

func newBenchmarkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "benchmark",
		Short: "Benchmark local machine and peer connections",
		Long: `Runs CPU, memory, and disk I/O benchmarks on the local machine.
If a running node is reachable, also measures HTTP round-trip latency
to all cluster peers.

Use --skip-network to run only local benchmarks without contacting the cluster.`,
		RunE: runBenchmark,
	}
	cmd.Flags().BoolVar(&benchSkipNetwork, "skip-network", false, "skip peer latency probes")
	return cmd
}

func runBenchmark(cmd *cobra.Command, args []string) error {
	// Determine disk benchmark directory from config.
	cfg, _ := config.LoadConfig(cfgFile)
	dataDir := ""
	if cfg != nil {
		dataDir = cfg.Node.DataDir
	}
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}
	// Use the data dir if it exists, otherwise fall back to temp.
	if _, err := os.Stat(dataDir); err != nil {
		dataDir = ""
	}

	fmt.Fprintln(os.Stderr, "Running local benchmarks...")
	local, err := benchmark.RunLocal(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}

	// Network probes.
	var network *benchmark.NetworkResult
	if !benchSkipNetwork {
		peers := discoverPeers()
		if len(peers) > 0 {
			fmt.Fprintf(os.Stderr, "Probing %d peer(s)...\n", len(peers))
			network = benchmark.ProbePeers(peers)
		}
	}

	if jsonOut {
		out := map[string]any{"local": local}
		if network != nil {
			out["network"] = network
		}
		printJSON(out)
		return nil
	}

	printLocalResults(local)
	if network != nil && len(network.Peers) > 0 {
		fmt.Println()
		printNetworkResults(network)
	}

	return nil
}

// discoverPeers fetches the node list from the local API and builds
// PeerInfo entries. Returns nil if the node is unreachable.
func discoverPeers() []benchmark.PeerInfo {
	resp, err := doGet("/nodes")
	if err != nil {
		return nil
	}

	var nodes []map[string]any
	if err := readJSON(resp, &nodes); err != nil {
		return nil
	}

	var peers []benchmark.PeerInfo
	for _, n := range nodes {
		id, _ := n["id"].(string)
		name, _ := n["name"].(string)
		address, _ := n["address"].(string)

		// The address field is the gossip address (host:gossipPort).
		// We need the HTTP address. Nodes expose an http_port in their
		// address or we derive it from the gossip address. The API
		// returns nodes as seen by the registry, which includes the
		// gossip address. We need to reconstruct the HTTP endpoint.
		//
		// The node list may also carry an "http_address" field if available.
		httpAddr, _ := n["http_address"].(string)
		if httpAddr == "" {
			// Derive from gossip address: replace port with HTTP default (7100).
			// This is a best-effort heuristic. In production, nodes would
			// advertise their HTTP port via metadata.
			httpAddr = deriveHTTPAddr(address)
		}

		if id == "" || httpAddr == "" {
			continue
		}
		// Skip "local" sentinel in single-node mode.
		if id == "local" {
			continue
		}

		peers = append(peers, benchmark.PeerInfo{
			NodeID:  id,
			Name:    name,
			Address: httpAddr,
		})
	}
	return peers
}

// deriveHTTPAddr replaces the port in a host:port gossip address with
// the default HTTP port (7100). Returns empty if the address can't be parsed.
func deriveHTTPAddr(gossipAddr string) string {
	if gossipAddr == "" {
		return ""
	}
	// Format: "host:port" where port is the gossip port.
	idx := strings.LastIndex(gossipAddr, ":")
	if idx < 0 {
		return ""
	}
	host := gossipAddr[:idx]
	return host + ":7100"
}

func printLocalResults(r *benchmark.LocalResult) {
	fmt.Printf("System: %s/%s, %d cores, %s\n",
		r.System.OS, r.System.Arch, r.System.Cores, r.System.Hostname)
	fmt.Println()

	fmt.Println("CPU (BLAKE3 hashing throughput):")
	fmt.Printf("  Single-core:   %8.0f MB/s\n", r.CPU.BLAKE3SingleMBps)
	fmt.Printf("  All-core (%dx): %7.0f MB/s\n", r.CPU.Cores, r.CPU.BLAKE3ParallelMBps)
	fmt.Printf("  Scaling:       %8.0f%%\n", r.CPU.ScalingEfficiency*100)
	fmt.Println()

	fmt.Println("Memory:")
	fmt.Printf("  Seq write:     %8.0f MB/s\n", r.Memory.SeqWriteMBps)
	fmt.Printf("  Seq read:      %8.0f MB/s\n", r.Memory.SeqReadMBps)
	fmt.Println()

	fmt.Println("Disk:")
	fmt.Printf("  Seq write:     %8.0f MB/s\n", r.Disk.WriteMBps)
	fmt.Printf("  Seq read:      %8.0f MB/s\n", r.Disk.ReadMBps)
	fmt.Printf("  Fsync latency: %8.0f us\n", r.Disk.FsyncUs)
}

func printNetworkResults(r *benchmark.NetworkResult) {
	fmt.Println("Peer Latency (HTTP RTT):")
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "  NODE\tADDRESS\tMIN\tAVG\tP50\tP99\tMAX\tLOSS")
	for _, p := range r.Peers {
		nodeLabel := shortID(p.NodeID)
		if p.Name != "" {
			nodeLabel = p.Name
		}
		if p.Error != "" {
			fmt.Fprintf(w, "  %s\t%s\t%s\t\t\t\t\t\n", nodeLabel, p.Address, p.Error)
			continue
		}
		fmt.Fprintf(w, "  %s\t%s\t%.1fms\t%.1fms\t%.1fms\t%.1fms\t%.1fms\t%.0f%%\n",
			nodeLabel, p.Address,
			p.RTTMin, p.RTTAvg, p.RTTP50, p.RTTP99, p.RTTMax, p.Loss)
	}
	w.Flush()
}
