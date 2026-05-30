package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <id>",
		Short: "Stream live task logs (stdout/stderr)",
		Long: `Connects to the SSE log stream for a running task and prints
stdout/stderr in real time. Exits when the task finishes.

For completed tasks, prints the persisted stdout/stderr.`,
		Args: cobra.ExactArgs(1),
		RunE: runLogs,
	}
	cmd.Flags().BoolVar(&logsFollow, "follow", false, "alias for default behavior (always follows)")
	return cmd
}

var logsFollow bool

func runLogs(cmd *cobra.Command, args []string) error {
	return streamLogs(args[0])
}

func streamLogs(taskID string) error {
	req, err := http.NewRequest(http.MethodGet, apiURL("/tasks/"+taskID+"/logs"), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := httpClientLong.Do(req)
	if err != nil {
		return wrapConnError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if decErr := json.NewDecoder(resp.Body).Decode(&errResp); decErr == nil && errResp.Error != "" {
			return fmt.Errorf("server: %s", errResp.Error)
		}
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}

	// Parse SSE stream line by line. Increase buffer for large persisted
	// logs: JSON-wrapped 64 KiB stdout/stderr can exceed the default 64 KiB
	// scanner token limit.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 256*1024), 256*1024)

	// Track the current SSE event type so we can distinguish log vs done events.
	var eventType string

	for scanner.Scan() {
		line := scanner.Text()

		// SSE event type line (e.g. "event: done", "event: log").
		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
			continue
		}

		// SSE data line.
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			if eventType == "done" || eventType == "" {
				// Parse done payload — may contain status, exit_code, error.
				var doneCheck map[string]any
				if err := json.Unmarshal([]byte(data), &doneCheck); err == nil {
					if errMsg, hasErr := doneCheck["error"]; hasErr {
						return fmt.Errorf("%v", errMsg)
					}
					if status, ok := doneCheck["status"].(string); ok {
						if status != "completed" {
							exitCode := 3
							if ec, ok := doneCheck["exit_code"].(float64); ok && int(ec) > 0 && int(ec) < 126 {
								exitCode = int(ec)
							}
							return &ExitError{Code: exitCode, Msg: fmt.Sprintf("task %s", status)}
						}
						return nil
					}
					// Empty done payload (e.g. "data: {}") — task completed.
					return nil
				}
			}

			// Log event — print stream data.
			var ev struct {
				Stream string `json:"stream"`
				Data   string `json:"data"`
			}
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				continue
			}

			if ev.Stream == "stderr" {
				fmt.Fprint(os.Stderr, ev.Data)
			} else {
				fmt.Print(ev.Data)
			}
			continue
		}

		// Empty line resets event type (SSE spec: events are delimited by blank lines).
		if line == "" {
			eventType = ""
		}
	}

	return scanner.Err()
}
