package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newNodesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List cluster nodes",
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
	fmt.Fprintln(w, "ID\tNAME\tADDRESS\tROLE\tTAGS")
	for _, n := range nodes {
		id, _ := n["id"].(string)
		name, _ := n["name"].(string)
		addr, _ := n["address"].(string)
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
	address, _ := node["address"].(string)
	role, _ := node["role"].(string)

	fmt.Printf("ID:       %s\n", id)
	fmt.Printf("Name:     %s\n", name)
	fmt.Printf("Address:  %s\n", address)
	fmt.Printf("Role:     %s\n", role)

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

	return nil
}
