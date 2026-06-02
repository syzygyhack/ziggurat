package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/worker"
)

func newEnvCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "env",
		Short: "Manage persistent task environments",
		Long:  `List and prune persistent environments created by tasks with --env or environment config.`,
	}
	cmd.AddCommand(newEnvListCmd())
	cmd.AddCommand(newEnvPruneCmd())
	return cmd
}

func newEnvListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List persistent environments on this node",
		Args:  cobra.NoArgs,
		RunE:  runEnvList,
	}
}

var envPruneMaxAge time.Duration

func newEnvPruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Remove stale persistent environments",
		Long:  `Removes environments that haven't been used within --max-age. Defaults to the configured env_max_age (7 days).`,
		Args:  cobra.NoArgs,
		RunE:  runEnvPrune,
	}
	cmd.Flags().DurationVar(&envPruneMaxAge, "max-age", 0, "remove envs unused for this duration (default: config value)")
	return cmd
}

func runEnvList(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	dataDir := cfg.Node.DataDir
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}

	envs := worker.ListEnvs(dataDir)
	if len(envs) == 0 {
		if !jsonOut {
			fmt.Println("No persistent environments found.")
		} else {
			printJSON([]any{})
		}
		return nil
	}

	if jsonOut {
		printJSON(envs)
		return nil
	}

	fmt.Printf("%-20s %-12s %-24s %s\n", "NAME", "SIZE", "LAST USED", "FINGERPRINT")
	for _, e := range envs {
		size := formatBytes(e.SizeBytes)
		lu := e.LastUsed.Format("2006-01-02 15:04:05")
		fp := e.Fingerprint
		if len(fp) > 16 {
			fp = fp[:16] + "..."
		}
		fmt.Printf("%-20s %-12s %-24s %s\n", e.Name, size, lu, fp)
	}
	return nil
}

func runEnvPrune(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	dataDir := cfg.Node.DataDir
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}

	maxAge := envPruneMaxAge
	if maxAge == 0 {
		maxAge = cfg.Compute.EnvMaxAge
	}
	maxCount := cfg.Compute.EnvMaxCount

	removed := worker.PruneEnvs(dataDir, maxAge, maxCount)
	if jsonOut {
		printJSON(map[string]any{"removed": removed})
	} else {
		fmt.Printf("Removed %d environment(s).\n", removed)
	}
	return nil
}
