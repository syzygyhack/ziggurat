package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newCancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <id>",
		Short: "Cancel a task",
		Args:  cobra.ExactArgs(1),
		RunE:  runCancel,
	}
}

func runCancel(cmd *cobra.Command, args []string) error {
	resp, err := doDelete("/tasks/" + args[0])
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

	status, _ := task["status"].(string)
	id, _ := task["id"].(string)
	switch status {
	case "cancelled":
		fmt.Printf("%s: cancelled\n", shortID(id))
	case "cancelling":
		fmt.Printf("%s: cancelling (waiting for process to exit)\n", shortID(id))
	case "completed", "failed", "dead_letter":
		fmt.Printf("%s: already %s (not cancelled)\n", shortID(id), status)
	default:
		fmt.Printf("%s: %s\n", shortID(id), status)
	}
	return nil
}
