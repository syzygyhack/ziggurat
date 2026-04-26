package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func newNodesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List cluster nodes",
		Args:  cobra.NoArgs,
		RunE:  runNodes,
	}
}

func runNodes(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/nodes")
	if err != nil {
		return err
	}

	var nodes []map[string]any
	if err := readJSON(resp, &nodes); err != nil {
		return err
	}

	if jsonOut {
		printJSON(nodes)
		return nil
	}

	if len(nodes) == 0 {
		fmt.Println("No nodes.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tHTTP\tROLE\tTAGS")
	for _, n := range nodes {
		id, _ := n["id"].(string)
		name, _ := n["name"].(string)
		addr, _ := n["http_address"].(string)
		if addr == "" {
			// Fall back to gossip address for nodes that don't advertise HTTP.
			addr, _ = n["address"].(string)
		}
		role, _ := n["role"].(string)

		if len(id) > 8 {
			id = id[:8]
		}
		if name == "" {
			name = "-"
		}
		if addr == "" {
			addr = "-"
		}
		if role == "" {
			role = "hybrid"
		}

		tags := "-"
		if tagSlice, ok := n["tags"].([]any); ok && len(tagSlice) > 0 {
			var parts []string
			for _, t := range tagSlice {
				if s, ok := t.(string); ok {
					parts = append(parts, s)
				}
			}
			tags = strings.Join(parts, ", ")
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", id, name, addr, role, tags)
	}
	w.Flush()
	return nil
}

func newNodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "node <id>",
		Short: "Show node details",
		Args:  cobra.ExactArgs(1),
		RunE:  runNode,
	}
}

func runNode(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/nodes/" + args[0])
	if err != nil {
		return err
	}

	var node map[string]any
	if err := readJSON(resp, &node); err != nil {
		return err
	}

	if jsonOut {
		printJSON(node)
		return nil
	}

	id, _ := node["id"].(string)
	name, _ := node["name"].(string)
	role, _ := node["role"].(string)
	httpAddr, _ := node["http_address"].(string)
	grpcAddr, _ := node["grpc_address"].(string)
	gossipAddr, _ := node["address"].(string)

	fmt.Printf("ID:       %s\n", id)
	fmt.Printf("Name:     %s\n", name)
	if role == "" {
		role = "hybrid"
	}
	fmt.Printf("Role:     %s\n", role)
	if httpAddr != "" {
		fmt.Printf("HTTP:     %s\n", httpAddr)
	}
	if grpcAddr != "" {
		fmt.Printf("gRPC:     %s\n", grpcAddr)
	}
	if gossipAddr != "" {
		fmt.Printf("Gossip:   %s\n", gossipAddr)
	}

	if tags, ok := node["tags"].([]any); ok && len(tags) > 0 {
		var parts []string
		for _, t := range tags {
			if s, ok := t.(string); ok {
				parts = append(parts, s)
			}
		}
		fmt.Printf("Tags:     %s\n", strings.Join(parts, ", "))
	}

	if caps, ok := node["capabilities"].(map[string]any); ok && len(caps) > 0 {
		fmt.Println("\nCapabilities:")
		for k, v := range caps {
			fmt.Printf("  %-20s %v\n", k+":", v)
		}
	}

	// Load/status fields (available from enriched node info).
	if status, ok := node["status"].(string); ok && status != "" {
		fmt.Printf("\nStatus:   %s\n", status)
	}
	if running := intFromAny(node["tasks_running"]); running > 0 {
		fmt.Printf("Running:  %d\n", running)
	}
	if queued := intFromAny(node["tasks_queued"]); queued > 0 {
		fmt.Printf("Queued:   %d\n", queued)
	}
	if uptime := intFromAny(node["uptime_seconds"]); uptime > 0 {
		fmt.Printf("Uptime:   %s\n", formatDuration(time.Duration(uptime)*time.Second))
	}

	return nil
}
