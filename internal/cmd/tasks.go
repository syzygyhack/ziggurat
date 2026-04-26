package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var tasksStatus string

func newTasksCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tasks",
		Short: "List all tasks",
		Args:  cobra.NoArgs,
		RunE:  runTasks,
	}
	cmd.Flags().StringVar(&tasksStatus, "status", "", "filter by status (queued, running, completed, failed, cancelled, dead_letter)")
	return cmd
}

func runTasks(cmd *cobra.Command, args []string) error {
	path := "/tasks"
	if tasksStatus != "" {
		path += "?status=" + tasksStatus
	}
	resp, err := doGet(path)
	if err != nil {
		return err
	}

	var tasks []map[string]any
	if err := readJSON(resp, &tasks); err != nil {
		return err
	}

	if jsonOut {
		printJSON(tasks)
		return nil
	}

	if len(tasks) == 0 {
		if tasksStatus != "" {
			fmt.Printf("No %s tasks.\n", tasksStatus)
		} else {
			fmt.Println("No tasks.")
		}
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATUS\tNODE\tCOMMAND\tWALL\tEXIT")
	for _, t := range tasks {
		id, _ := t["id"].(string)
		status, _ := t["status"].(string)
		exitCode, _ := t["exit_code"].(float64)
		worker, _ := t["worker"].(string)
		cmdSlice, _ := t["command"].([]any)

		cmdStr := ""
		if len(cmdSlice) > 0 {
			first, _ := cmdSlice[0].(string)
			cmdStr = first
			if len(cmdSlice) > 1 {
				cmdStr += " ..."
			}
		}

		// Compute wall time from metrics if available.
		wall := "--"
		if metrics, ok := t["metrics"].(map[string]any); ok {
			if wt, ok := metrics["wall_time"].(string); ok && wt != "" && wt != "0s" {
				wall = wt
			} else if startedStr, ok := metrics["started_at"].(string); ok && startedStr != "" {
				// Running task: compute from started_at.
				if started, err := time.Parse(time.RFC3339Nano, startedStr); err == nil {
					wall = time.Since(started).Truncate(time.Second).String()
				}
			}
		}

		if len(id) > 8 {
			id = id[:8]
		}
		node := shortID(worker)
		if node == "" {
			node = "--"
		}
		exitStr := "--"
		if status == "completed" || status == "failed" || status == "cancelled" || status == "dead_letter" {
			exitStr = fmt.Sprintf("%d", int(exitCode))
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", id, status, node, cmdStr, wall, exitStr)
	}
	w.Flush()
	return nil
}
