package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show cluster health and status",
		Args:  cobra.NoArgs,
		RunE:  runStatus,
	}
}

func runStatus(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/cluster")
	if err != nil {
		return err
	}

	var data map[string]any
	if err := readJSON(resp, &data); err != nil {
		return err
	}

	if jsonOut {
		printJSON(data)
		return nil
	}

	status, _ := data["status"].(string)
	nodes := intVal(data, "nodes")
	healthy := intVal(data, "nodes_healthy")
	uptimeSec := intVal(data, "uptime_seconds")
	running := intVal(data, "tasks_running")
	queued := intVal(data, "tasks_queued")
	completed := intVal(data, "tasks_completed")
	failed := intVal(data, "tasks_failed")
	cancelled := intVal(data, "tasks_cancelled")
	deadLetter := intVal(data, "tasks_dead_letter")
	total := intVal(data, "tasks_total")
	storeObjects := intVal(data, "storage_objects")
	storeUsed := intVal(data, "storage_used_bytes")
	storeCap := intVal(data, "storage_capacity")

	// Header line.
	fmt.Printf("Status: %s    Nodes: %d/%d healthy    Uptime: %s\n",
		status, healthy, nodes, formatDuration(time.Duration(uptimeSec)*time.Second))

	// Task summary.
	fmt.Printf("Tasks:  %d running, %d queued, %d completed, %d failed",
		running, queued, completed, failed)
	if cancelled > 0 {
		fmt.Printf(", %d cancelled", cancelled)
	}
	if deadLetter > 0 {
		fmt.Printf(", %d dead-letter", deadLetter)
	}
	fmt.Printf("  (%d total)\n", total)

	// Store summary.
	storeStr := formatBytes(int64(storeUsed))
	if storeCap > 0 {
		storeStr += " / " + formatBytes(int64(storeCap))
	}
	fmt.Printf("Store:  %s (%d objects)\n", storeStr, storeObjects)

	// Active tasks table.
	activeTasks, _ := data["active_tasks"].([]any)
	if len(activeTasks) > 0 {
		fmt.Println()
		fmt.Printf("  %-10s %-10s %-30s %s\n", "ID", "STATUS", "COMMAND", "WALL")
		for _, raw := range activeTasks {
			t, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			id, _ := t["id"].(string)
			st, _ := t["status"].(string)
			wall, _ := t["wall"].(string)
			cmdParts, _ := t["command"].([]any)

			// Short ID (first 8 chars).
			if len(id) > 8 {
				id = id[:8]
			}

			// Join command, truncate to 30 chars.
			var parts []string
			for _, p := range cmdParts {
				if s, ok := p.(string); ok {
					parts = append(parts, s)
				}
			}
			cmdStr := strings.Join(parts, " ")
			if len(cmdStr) > 30 {
				cmdStr = cmdStr[:27] + "..."
			}

			fmt.Printf("  %-10s %-10s %-30s %s\n", id, st, cmdStr, wall)
		}
	}

	if queued > 0 && len(activeTasks) == 0 {
		fmt.Printf("\nQueue: %d tasks waiting\n", queued)
	}

	return nil
}

// intVal extracts a numeric value from a JSON map as int.
func intVal(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}

// formatDuration formats a duration as human-readable (e.g. "3d 14h", "2h 5m", "45s").
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh %dm", h, m)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	if hours == 0 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd %dh", days, hours)
}

// formatBytes formats byte counts as human-readable.
func formatBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
