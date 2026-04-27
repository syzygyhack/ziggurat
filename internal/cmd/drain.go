package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

func newDrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drain",
		Short: "Stop accepting new tasks, finish in-flight work",
		Long: `Puts the node at --addr into drain mode. No new tasks will be dequeued,
but running tasks complete normally. Submissions still succeed (tasks queue
but won't execute locally).`,
		Args: cobra.NoArgs,
		RunE: runDrain,
	}
}

func newResumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume",
		Short: "Resume task dequeuing after drain",
		Long:  `Takes the node at --addr out of drain mode, allowing workers to dequeue and execute tasks again.`,
		Args:  cobra.NoArgs,
		RunE:  runResume,
	}
}

func runDrain(cmd *cobra.Command, args []string) error {
	return printDrainResult(doPost("/drain", nil))
}

func runResume(cmd *cobra.Command, args []string) error {
	return printDrainResult(doPost("/resume", nil))
}

func printDrainResult(resp *http.Response, err error) error {
	if err != nil {
		return err
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
		return nil
	}

	status, _ := result["status"].(string)
	runningF, _ := result["tasks_running"].(float64)
	queuedF, _ := result["tasks_queued"].(float64)
	running := int(runningF)
	queued := int(queuedF)

	fmt.Printf("Status:          %s\n", status)
	fmt.Printf("Tasks running:   %d\n", running)
	fmt.Printf("Tasks queued:    %d\n", queued)

	if objectsF, ok := result["storage_objects"].(float64); ok {
		fmt.Printf("Storage objects: %d\n", int(objectsF))
	}
	if msg, ok := result["message"].(string); ok && msg != "" {
		fmt.Printf("\n%s\n", msg)
	}

	return nil
}
