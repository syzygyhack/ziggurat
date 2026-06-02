package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/node"
)

var (
	startJoin []string
	startRole string
)

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the ziggurat node",
		Long:  `Starts the ziggurat node, initializing storage, compute, and the HTTP API server.`,
		Args:  cobra.NoArgs,
		RunE:  runStart,
	}
	cmd.Flags().StringArrayVar(&startJoin, "join", nil, "address of existing node to join (repeatable)")
	cmd.Flags().StringVar(&startRole, "role", "", "node role: hybrid (default), coordinator, worker")
	return cmd
}

func runStart(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return err
	}

	// --join flag overrides config seeds.
	if len(startJoin) > 0 {
		cfg.Cluster.Seeds = startJoin
	}

	// --role flag overrides config node role.
	if startRole != "" {
		cfg.Node.Role = startRole
	}

	// Validate role early so typos fail fast.
	switch cfg.Node.Role {
	case "", "hybrid", "coordinator", "worker":
		// ok
	default:
		return fmt.Errorf("invalid role %q: must be hybrid, coordinator, or worker", cfg.Node.Role)
	}

	level := parseLogLevel(cfg.Log.Level)
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Log.Format == config.LogFormatJSON {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	log := slog.New(handler)

	// Enforce one node per machine. Windows+WSL and separate machines use
	// distinct lock paths, so they remain valid for local multi-node dev.
	releaseLock, err := node.AcquireSingleInstance()
	if err != nil {
		return err
	}
	defer releaseLock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n, err := node.Start(ctx, cfg, log)
	if err != nil {
		return err
	}

	// Wait for interrupt signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, shutdownSignals()...)
	sig := <-sigCh
	log.Info("received signal", "signal", sig)

	return n.Shutdown(context.Background())
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
