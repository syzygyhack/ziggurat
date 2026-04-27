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
	return &cobra.Command{
		Use:   "logs <id>",
		Short: "Stream live task logs (stdout/stderr)",
		Long: `Connects to the SSE log stream for a running task and prints
stdout/stderr in real time. Exits when the task finishes.

For completed tasks, prints the persisted stdout/stderr.`,
		Args: cobra.ExactArgs(1),
		RunE: runLogs,
	}
}

func runLogs(cmd *cobra.Command, args []string) error {
	return streamLogs(args[0])
}

func streamLogs(taskID string) error {
	req, err := http.NewRequest(http.MethodGet, apiURL("/tasks/"+taskID+"/logs"), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := httpClient.Do(req)
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

	// Parse SSE stream line by line.
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			// Check for done event.
			var doneCheck map[string]any
			if err := json.Unmarshal([]byte(data), &doneCheck); err == nil {
				if _, hasDone := doneCheck["status"]; hasDone {
					// Terminal done event — task finished.
					return nil
				}
			}

			// Parse as log event.
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
		}

		// "event: done" with empty data means live stream ended.
		if line == "event: done" {
			// Read the next data line.
			if scanner.Scan() {
				dataLine := scanner.Text()
				if dataLine == "data: {}" {
					return nil
				}
			}
		}
	}

	return scanner.Err()
}
