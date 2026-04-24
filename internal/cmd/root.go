package cmd

import (
	"github.com/spf13/cobra"
)

// Global flags shared across commands.
var (
	cfgFile string // --config
	addr    string // --addr (server address for client commands)
	jsonOut bool   // --json
)

// NewRootCmd creates the root command with all subcommands registered.
func NewRootCmd(version, commit string) *cobra.Command {
	root := &cobra.Command{
		Use:   "ziggurat",
		Short: "Distributed research compute mesh",
		Long:  `Ziggurat is a distributed compute mesh with integrated content-addressed storage for research workloads.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (auto-detects ziggurat.yaml if present)")
	root.PersistentFlags().StringVar(&addr, "addr", "", "server address (default: ZIGGURAT_ADDR or 127.0.0.1:7100)")
	root.PersistentFlags().BoolVar(&jsonOut, "json", false, "output as JSON")

	// Server lifecycle
	root.AddCommand(newInitCmd())
	root.AddCommand(newStartCmd())

	// Task operations
	root.AddCommand(newRunCmd())
	root.AddCommand(newTasksCmd())
	root.AddCommand(newTaskCmd())
	root.AddCommand(newCancelCmd())
	root.AddCommand(newWaitCmd())
	root.AddCommand(newDeadLetterCmd())
	root.AddCommand(newBatchCmd())
	root.AddCommand(newPipelineCmd())

	// Store operations
	root.AddCommand(newPutCmd())
	root.AddCommand(newGetCmd())
	root.AddCommand(newLsCmd())
	root.AddCommand(newRmCmd())

	// Cluster
	root.AddCommand(newStatusCmd())
	root.AddCommand(newNodesCmd())
	root.AddCommand(newNodeCmd())
	root.AddCommand(newDrainCmd())
	root.AddCommand(newResumeCmd())
	root.AddCommand(newVersionCmd(version, commit))

	return root
}
