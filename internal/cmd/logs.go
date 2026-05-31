package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <id>",
		Short: "Stream live task logs (stdout/stderr)",
		Long: `Connects to the SSE log stream for a running task and prints
stdout/stderr in real time. Exits when the task finishes.

For completed tasks, prints the persisted stdout/stderr.

Use --follow/-f to wait for a task that hasn't started yet.`,
		Args: cobra.ExactArgs(1),
		RunE: runLogs,
	}
	cmd.Flags().BoolVarP(&logsFollow, "follow", "f", false, "wait for task to start if not yet running")
	return cmd
}

var logsFollow bool

func runLogs(cmd *cobra.Command, args []string) error {
	if logsFollow {
		return streamLogsFollow(args[0])
	}
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

	return streamSSE(resp.Body)
}

// streamLogsFollow polls until the task exists (HTTP 200), then streams.
func streamLogsFollow(taskID string) error {
	for {
		req, err := http.NewRequest(http.MethodGet, apiURL("/tasks/"+taskID+"/logs"), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "text/event-stream")

		resp, err := httpClientLong.Do(req)
		if err != nil {
			return wrapConnError(err)
		}
		if resp.StatusCode == 404 {
			resp.Body.Close()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if resp.StatusCode >= 400 {
			resp.Body.Close()
			return fmt.Errorf("server returned %d", resp.StatusCode)
		}
		return streamSSE(resp.Body)
	}
}

// streamSSE parses an SSE stream from r, printing log events to stdout.
// Returns an ExitError on non-zero task exit codes.
func streamSSE(r io.ReadCloser) error {
	defer r.Close()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 256*1024)
	var eventType string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			if eventType == "done" || eventType == "" {
				var doneCheck map[string]any
				if err := json.Unmarshal([]byte(data), &doneCheck); err == nil {
					if errMsg, ok := doneCheck["error"].(string); ok && errMsg != "" {
						fmt.Fprintf(os.Stderr, "error: %s\n", errMsg)
						return &ExitError{Code: 1}
					}
					if status, ok := doneCheck["status"].(string); ok {
						if status != "completed" {
							exitCode := 3
							if ec, ok := doneCheck["exit_code"].(float64); ok && int(ec) > 0 && int(ec) < 126 {
								exitCode = int(ec)
							}
							return &ExitError{Code: exitCode}
						}
						if ec, ok := doneCheck["exit_code"].(float64); ok {
							return &ExitError{Code: int(ec)}
						}
					}
				}
				return nil
			}

			var ev struct {
				Stream string `json:"stream"`
				Data   string `json:"data"`
			}
			if err := json.Unmarshal([]byte(data), &ev); err == nil && ev.Data != "" {
				fmt.Print(ev.Data)
			}
		}

		if line == "" {
			eventType = ""
		}
	}
	return scanner.Err()
}
