package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var (
	topInterval time.Duration
	topOnce     bool
)

func newTopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "top",
		Short: "Live cluster dashboard",
		Long: `Displays a live-refreshing view of cluster status, node load, and
active tasks. Press Ctrl+C to quit.

Use --once for a single snapshot (useful for scripts).`,
		RunE: runTop,
	}
	cmd.Flags().DurationVarP(&topInterval, "interval", "n", 2*time.Second, "refresh interval")
	cmd.Flags().BoolVar(&topOnce, "once", false, "print once and exit")
	return cmd
}

type topSnapshot struct {
	cluster map[string]any
	nodes   []map[string]any
	err     error
}

func fetchTop() topSnapshot {
	var snap topSnapshot

	resp, err := doGet("/cluster")
	if err != nil {
		snap.err = err
		return snap
	}
	if err := readJSON(resp, &snap.cluster); err != nil {
		snap.err = err
		return snap
	}

	resp, err = doGet("/nodes")
	if err != nil {
		snap.err = err
		return snap
	}
	if err := readJSON(resp, &snap.nodes); err != nil {
		snap.err = err
		return snap
	}

	return snap
}

func renderTop(snap topSnapshot, interactive bool) {
	if interactive {
		fmt.Print("\033[H") // cursor home (no clear — overwrite in place)
	}

	if snap.err != nil {
		fmt.Printf("error: %v\n", snap.err)
		return
	}

	c := snap.cluster

	// Header.
	status, _ := c["status"].(string)
	nodes := intVal(c, "nodes")
	healthy := intVal(c, "nodes_healthy")
	uptimeSec := intVal(c, "uptime_seconds")

	now := time.Now().Format("15:04:05")
	fmt.Printf("ziggurat top - %s   up %s   %d/%d nodes healthy   [%s]\n",
		now,
		formatDuration(time.Duration(uptimeSec)*time.Second),
		healthy, nodes, status)
	fmt.Println()

	// Task summary.
	running := intVal(c, "tasks_running")
	queued := intVal(c, "tasks_queued")
	completed := intVal(c, "tasks_completed")
	failed := intVal(c, "tasks_failed")
	cancelled := intVal(c, "tasks_cancelled")
	deadLetter := intVal(c, "tasks_dead_letter")
	total := intVal(c, "tasks_total")

	fmt.Printf("Tasks: %d run, %d queue, %d done, %d fail",
		running, queued, completed, failed)
	if cancelled > 0 {
		fmt.Printf(", %d cancel", cancelled)
	}
	if deadLetter > 0 {
		fmt.Printf(", %d dead", deadLetter)
	}
	fmt.Printf("  (%d total)\n", total)

	// Storage.
	storeObjects := intVal(c, "storage_objects")
	storeUsed := intVal(c, "storage_used_bytes")
	storeCap := intVal(c, "storage_capacity")
	storeStr := formatBytes(int64(storeUsed))
	if storeCap > 0 {
		storeStr += "/" + formatBytes(int64(storeCap))
	}
	fmt.Printf("Store: %s (%d objects)\n", storeStr, storeObjects)

	// Build worker load map (full node ID -> [running, limit]).
	workerLoad := make(map[string][2]int)
	if wl, ok := c["worker_load"].([]any); ok {
		for _, raw := range wl {
			entry, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			nodeID, _ := entry["node_id"].(string)
			r := intFromAny(entry["running"])
			l := intFromAny(entry["limit"])
			workerLoad[nodeID] = [2]int{r, l}
		}
	}

	// Node table.
	if len(snap.nodes) > 0 {
		fmt.Println()
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NODE\tNAME\tROLE\tTASKS\tTAGS")

		sort.Slice(snap.nodes, func(i, j int) bool {
			ni, _ := snap.nodes[i]["name"].(string)
			nj, _ := snap.nodes[j]["name"].(string)
			return ni < nj
		})

		for _, n := range snap.nodes {
			fullID, _ := n["id"].(string)
			name, _ := n["name"].(string)
			role, _ := n["role"].(string)

			displayID := shortID(fullID)
			if name == "" {
				name = "-"
			}
			if role == "" {
				role = "hybrid"
			}

			taskStr := "--"
			if load, ok := workerLoad[fullID]; ok {
				taskStr = fmt.Sprintf("%d/%d", load[0], load[1])
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

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", displayID, name, role, taskStr, tags)
		}
		w.Flush()
	}

	// Active tasks.
	activeTasks, _ := c["active_tasks"].([]any)
	if len(activeTasks) > 0 {
		fmt.Println()
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "TASK\tSTATUS\tNODE\tPRI\tCOMMAND\tWALL")
		for _, raw := range activeTasks {
			t, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			id, _ := t["id"].(string)
			st, _ := t["status"].(string)
			wall, _ := t["wall"].(string)
			worker, _ := t["worker"].(string)
			pri := intFromAny(t["priority"])
			cmdParts, _ := t["command"].([]any)

			var parts []string
			for _, p := range cmdParts {
				if s, ok := p.(string); ok {
					parts = append(parts, s)
				}
			}
			cmdStr := strings.Join(parts, " ")
			if len(cmdStr) > 40 {
				cmdStr = cmdStr[:37] + "..."
			}

			nodeStr := shortID(worker)
			if nodeStr == "" {
				nodeStr = "--"
			}

			priStr := ""
			if pri != 0 {
				priStr = fmt.Sprintf("%d", pri)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				shortID(id), st, nodeStr, priStr, cmdStr, wall)
		}
		w.Flush()
	} else if queued > 0 {
		fmt.Printf("\n%d queued tasks waiting\n", queued)
	}

	if interactive {
		fmt.Printf("\nRefreshing every %s. Ctrl+C to quit.\033[J", topInterval) // \033[J clears from cursor to end of screen
	}
}

func runTop(cmd *cobra.Command, args []string) error {
	snap := fetchTop()

	if jsonOut {
		out := map[string]any{
			"cluster": snap.cluster,
			"nodes":   snap.nodes,
		}
		printJSON(out)
		return snap.err
	}

	if topOnce {
		renderTop(snap, false)
		fmt.Println()
		return snap.err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	renderTop(snap, true)

	ticker := time.NewTicker(topInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println()
			return nil
		case <-ticker.C:
			snap = fetchTop()
			renderTop(snap, true)
		}
	}
}

// intFromAny extracts an int from a JSON-decoded value (float64 or int).
func intFromAny(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}
