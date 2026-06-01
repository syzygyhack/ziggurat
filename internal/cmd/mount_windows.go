//go:build windows

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newMountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mount <path>",
		Short: "FUSE-mount the store (Linux/macOS only)",
		Long:  `FUSE mount is not supported on Windows.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("FUSE mount is not supported on Windows")
		},
	}
}
