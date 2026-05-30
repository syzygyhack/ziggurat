package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// looksLikeHash returns true if key matches a 64-char hex hash pattern
// (case-insensitive). Case is normalized within the function.
func looksLikeHash(key string) bool {
	return store.ValidateHashHex(key) == nil
}

// normalizeHashIfNeeded returns the hash in lowercase if it's a valid hex
// hash, or the original string unchanged.
func normalizeHashIfNeeded(key string) string {
	if store.ValidateHashHex(key) == nil {
		return store.NormalizeHashHex(key)
	}
	return key
}

var getExtract bool

func newGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <key> [dest]",
		Short: "Retrieve an object by namespace key",
		Long:  `Downloads an object. If dest is omitted, writes to stdout. Use --extract to extract tar archives into dest directory.`,
		Args:  cobra.RangeArgs(1, 2),
		RunE:  runGet,
	}
	cmd.Flags().BoolVar(&getExtract, "extract", false, "extract tar archive into destination directory")
	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	key := args[0]

	// Normalize hash case so uppercase hex from copy/paste works.
	key = normalizeHashIfNeeded(key)

	// Auto-detect bare content hashes (e.g. output_ref values) and
	// route through @hash/ so users can copy/paste hashes directly.
	path := storeKeyPath(key)
	if looksLikeHash(key) {
		path = storeKeyPath("@hash/" + key)
	}

	resp, err := doGet(path)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errBody struct {
			Error string `json:"error"`
		}
		if decErr := json.NewDecoder(resp.Body).Decode(&errBody); decErr == nil && errBody.Error != "" {
			return fmt.Errorf("server: %s", errBody.Error)
		}
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}

	// Extract mode: stream tar directly from response into destination.
	if getExtract {
		if len(args) < 2 {
			return fmt.Errorf("--extract requires a destination directory argument")
		}
		dest := args[1]
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return err
		}
		if err := store.ExtractTar(resp.Body, dest); err != nil {
			return fmt.Errorf("extract tar: %w", err)
		}
		return nil
	}

	// Normal mode: write raw bytes to file or stdout.
	var w io.Writer = os.Stdout
	if len(args) > 1 {
		f, err := os.Create(args[1])
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	_, err = io.Copy(w, resp.Body)
	return err
}
