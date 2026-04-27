package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var waitTimeout time.Duration

func newWaitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wait <id>",
		Short: "Block until a task completes",
		Args:  cobra.ExactArgs(1),
		RunE:  runWaitCmd,
	}
	cmd.Flags().DurationVar(&waitTimeout, "timeout", 0, "maximum time to wait (e.g. 5m, 1h); 0 = forever")
	return cmd
}

func runWaitCmd(cmd *cobra.Command, args []string) error {
	var req *http.Request
	var err error
	req, err = http.NewRequest(http.MethodPost, apiURL("/tasks/"+args[0]+"/wait"), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	if !jsonOut {
		fmt.Fprintf(os.Stderr, "Waiting for task %s...\n", shortID(args[0]))
	}

	if waitTimeout > 0 {
		ctx, cancel := context.WithTimeout(cmd.Context(), waitTimeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	resp, err := httpClientLong.Do(req)
	if err != nil {
		if req.Context().Err() != nil && waitTimeout > 0 {
			return fmt.Errorf("timed out waiting for task after %s", waitTimeout)
		}
		return fmt.Errorf("wait for task: %w", wrapConnError(err))
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	status, _ := result["status"].(string)

	if jsonOut {
		printJSON(result)
		if status != "completed" {
			exitCode := 3
			if ec, ok := result["exit_code"].(float64); ok && int(ec) > 0 && int(ec) < 126 {
				exitCode = int(ec)
			}
			return &ExitError{Code: exitCode, Msg: fmt.Sprintf("task %s", status)}
		}
		return nil
	}
	stdout, _ := result["stdout"].(string)
	stderr, _ := result["stderr"].(string)

	if stdout != "" {
		fmt.Print(stdout)
	}
	if stderr != "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "%s", stderr)
	}

	id, _ := result["id"].(string)
	printCompletionSummary(cmd, id, result)

	if status != "completed" {
		errMsg, _ := result["error"].(string)
		exitCode := 3
		if ec, ok := result["exit_code"].(float64); ok && int(ec) > 0 && int(ec) < 126 {
			exitCode = int(ec)
		}
		return &ExitError{
			Code: exitCode,
			Msg:  fmt.Sprintf("task %s: %s", status, strings.TrimSpace(errMsg)),
		}
	}

	return nil
}
