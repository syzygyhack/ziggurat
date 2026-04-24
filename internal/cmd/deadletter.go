package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newDeadLetterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dead-letter",
		Short: "List dead-lettered tasks (retries exhausted)",
		RunE:  runDeadLetter,
	}
}

func runDeadLetter(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/tasks?status=dead_letter")
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
		fmt.Println("No dead-lettered tasks.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCOMMAND\tATTEMPTS\tERROR")
	for _, t := range tasks {
		id, _ := t["id"].(string)
		attempt, _ := t["attempt"].(float64)
		errMsg, _ := t["error"].(string)
		cmdSlice, _ := t["command"].([]any)

		cmdStr := ""
		if len(cmdSlice) > 0 {
			first, _ := cmdSlice[0].(string)
			cmdStr = first
			if len(cmdSlice) > 1 {
				cmdStr += " ..."
			}
		}

		if len(id) > 8 {
			id = id[:8]
		}
		if len(errMsg) > 40 {
			errMsg = errMsg[:40] + "..."
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", id, cmdStr, int(attempt), errMsg)
	}
	w.Flush()
	return nil
}
