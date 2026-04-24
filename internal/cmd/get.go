package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/store"
)

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

	resp, err := doGet("/store/" + key)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", string(body))
	}

	// Extract mode: read body and extract tar into destination.
	if getExtract {
		if len(args) < 2 {
			return fmt.Errorf("--extract requires a destination directory argument")
		}
		dest := args[1]
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return err
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read response: %w", err)
		}
		if err := store.ExtractTar(bytes.NewReader(data), dest); err != nil {
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
