package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/config"
)

// apiBase resolves the server address using the documented precedence:
//  1. --addr flag
//  2. ZIGGURAT_ADDR env var
//  3. client.addr from config file (ziggurat.yaml)
//  4. 127.0.0.1:7100 (default)
func apiBase() string {
	if addr != "" {
		return "http://" + addr
	}
	if env := os.Getenv("ZIGGURAT_ADDR"); env != "" {
		return "http://" + env
	}
	// Try loading client.addr from config file.
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		// Explicit --config that fails to load is a hard error — the user
		// specified a file, so silently falling back could talk to the wrong
		// cluster. Auto-discovery failure (no file) is fine — use default.
		if cfgFile != "" {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	} else if cfg.Client.Addr != "" {
		return "http://" + cfg.Client.Addr
	}
	return "http://127.0.0.1:7100"
}

func apiURL(path string) string {
	return apiBase() + "/api/v1" + path
}

// httpClient is the shared client for all CLI requests. Uses a dial timeout
// so connections fail fast when the server is unreachable, but long-running
// requests (wait, large transfers) aren't killed prematurely.
var httpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).DialContext,
	},
}

func doGet(path string) (*http.Response, error) {
	resp, err := httpClient.Get(apiURL(path))
	return resp, wrapConnError(err)
}

func doPost(path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Post(apiURL(path), "application/json", bytes.NewReader(data))
	return resp, wrapConnError(err)
}

func doPut(path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPut, apiURL(path), body)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	return resp, wrapConnError(err)
}

func doDelete(path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, apiURL(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	return resp, wrapConnError(err)
}

// readJSON decodes the response body into v. Returns an error if the response
// indicates failure.
func readJSON(resp *http.Response, v any) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil && errResp.Error != "" {
			return fmt.Errorf("server: %s", errResp.Error)
		}
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}
	if v == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// ExitError is returned by commands that want to set a specific process exit code.
// main.go checks for this type to propagate the code to os.Exit.
type ExitError struct {
	Code int
	Msg  string
}

func (e *ExitError) Error() string { return e.Msg }

// wrapConnError converts low-level connection errors into user-friendly messages.
// Returns ExitError{Code: 2} for connection failures per the documented exit code table.
func wrapConnError(err error) error {
	if err == nil {
		return nil
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if strings.Contains(opErr.Err.Error(), "connection refused") {
			return &ExitError{
				Code: 2,
				Msg:  fmt.Sprintf("cannot connect to ziggurat at %s: connection refused\nhint: is the node running? try: ziggurat start", apiBase()),
			}
		}
		if opErr.Timeout() {
			return &ExitError{
				Code: 2,
				Msg:  fmt.Sprintf("cannot connect to ziggurat at %s: connection timed out", apiBase()),
			}
		}
	}
	return err
}

// shortID returns the first 8 characters of an ID, or the full string if shorter.
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// printJSON encodes v as indented JSON to stdout.
func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// printCompletionSummary writes a short task result summary to stderr.
// Shows status, wall time, exit code, and output_ref when present.
func printCompletionSummary(cmd *cobra.Command, id string, result map[string]any) {
	w := cmd.ErrOrStderr()
	status, _ := result["status"].(string)
	exitCode, _ := result["exit_code"].(float64)
	outputRef, _ := result["output_ref"].(string)

	wallTime := ""
	if metrics, ok := result["metrics"].(map[string]any); ok {
		wallTime, _ = metrics["wall_time"].(string)
	}

	parts := []string{fmt.Sprintf("Task %s %s", shortID(id), status)}
	if wallTime != "" && wallTime != "0s" {
		parts = append(parts, fmt.Sprintf("in %s", wallTime))
	}
	if exitCode != 0 {
		parts = append(parts, fmt.Sprintf("(exit %d)", int(exitCode)))
	}
	fmt.Fprintln(w, strings.Join(parts, " "))

	if outputRef != "" {
		fmt.Fprintf(w, "Output: %s\n", outputRef)
		fmt.Fprintf(w, "  retrieve: ziggurat get %s <dest>\n", outputRef)
	}
}

// storeKeyPath returns "/store/<escaped-key>" suitable for use in API URLs.
// Each segment of the key is path-escaped to handle spaces, ?, #, % etc.,
// but forward slashes are preserved as path separators.
func storeKeyPath(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return "/store/" + strings.Join(segments, "/")
}
