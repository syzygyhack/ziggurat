package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func newTaskCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "task <id>",
		Short: "Get task details",
		Args:  cobra.ExactArgs(1),
		RunE:  runTask,
	}
}

func runTask(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/tasks/" + args[0])
	if err != nil {
		return err
	}

	var task map[string]any
	if err := readJSON(resp, &task); err != nil {
		return err
	}

	if jsonOut {
		printJSON(task)
		return nil
	}

	// Human-readable detail view.
	id, _ := task["id"].(string)
	status, _ := task["status"].(string)
	exitCode, _ := task["exit_code"].(float64)
	worker, _ := task["worker"].(string)
	errMsg, _ := task["error"].(string)
	outputRef, _ := task["output_ref"].(string)
	stdout, _ := task["stdout"].(string)
	stderr, _ := task["stderr"].(string)

	fmt.Printf("ID:        %s\n", id)
	fmt.Printf("Status:    %s\n", status)

	// Command
	if cmdSlice, ok := task["command"].([]any); ok && len(cmdSlice) > 0 {
		parts := make([]string, len(cmdSlice))
		for i, v := range cmdSlice {
			parts[i], _ = v.(string)
		}
		fmt.Printf("Command:   %s\n", strings.Join(parts, " "))
	}

	fmt.Printf("Exit Code: %d\n", int(exitCode))
	if worker != "" {
		fmt.Printf("Worker:    %s\n", worker)
	}

	// Timing
	if metrics, ok := task["metrics"].(map[string]any); ok {
		if wall, ok := metrics["wall_time"].(string); ok && wall != "" && wall != "0s" {
			fmt.Printf("Wall Time: %s\n", wall)
		}
	}

	// Requires and constraints
	if requires, ok := task["requires"].([]any); ok && len(requires) > 0 {
		tags := make([]string, len(requires))
		for i, v := range requires {
			tags[i], _ = v.(string)
		}
		fmt.Printf("Requires:  %s\n", strings.Join(tags, ", "))
	}
	if constraints, ok := task["constraints"].([]any); ok && len(constraints) > 0 {
		for _, v := range constraints {
			if expr, ok := v.(string); ok {
				fmt.Printf("Constraint: %s\n", expr)
			}
		}
	}

	if outputRef != "" {
		fmt.Printf("Output:    %s\n", outputRef)
	}
	if errMsg != "" {
		fmt.Printf("Error:     %s\n", errMsg)
	}

	// Stdout/stderr — show last lines for completed/failed tasks.
	if stdout != "" {
		fmt.Println()
		fmt.Println("stdout:")
		printLastLines(stdout, 20)
	}
	if stderr != "" {
		fmt.Println()
		fmt.Println("stderr:")
		printLastLines(stderr, 20)
	}

	return nil
}

// printLastLines prints the last n lines of s, prefixed with "  ".
func printLastLines(s string, n int) {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	start := 0
	if len(lines) > n {
		start = len(lines) - n
		fmt.Printf("  ... (%d lines omitted)\n", start)
	}
	for _, line := range lines[start:] {
		fmt.Printf("  %s\n", line)
	}
}
