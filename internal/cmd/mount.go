package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	zigfuse "github.com/syzygyhack/ziggurat/internal/fuse"
)

func newMountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mount <mountpoint>",
		Short: "Mount cluster storage as a FUSE filesystem",
		Long: `Mounts the Ziggurat object store at the given directory. Objects are accessible
as files using their namespace keys. Subdirectories are derived from key prefixes.

Example:
  ziggurat mount /mnt/zig
  ls /mnt/zig/
  cat /mnt/zig/datasets/train.csv
  cp results.tar /mnt/zig/outputs/results.tar

Press Ctrl+C or run 'fusermount -u <mountpoint>' to unmount.`,
		Args: cobra.ExactArgs(1),
		RunE: runMount,
	}
}

func runMount(cmd *cobra.Command, args []string) error {
	mountpoint := args[0]

	// Ensure mountpoint exists.
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return fmt.Errorf("create mountpoint: %w", err)
	}

	log := slog.Default().With("component", "fuse")

	cfg := zigfuse.ZigFSConfig{
		APIBase:    apiBase(),
		MountPoint: mountpoint,
		Log:        log,
	}

	server, err := zigfuse.Mount(cfg)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}

	fmt.Printf("Mounted at %s (Ctrl+C to unmount)\n", mountpoint)

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nUnmounting...")
	if err := server.Unmount(); err != nil {
		return fmt.Errorf("unmount: %w", err)
	}
	return nil
}
