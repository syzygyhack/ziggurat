package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/syzygyhack/ziggurat/internal/util"
)

var (
	sweepGrid        []string
	sweepRequires    []string
	sweepConstraints []string
	sweepImage       string
	sweepGPUs        int
	sweepCPUs        int
	sweepMemory      string
	sweepPriority    int
)

func newSweepCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sweep [flags] -- command [args...]",
		Short: "Expand a command template over a parameter grid into N tasks",
		Long: `Submit a parameter sweep: one command template fanned out across a grid of
values into N independent tasks. Reference parameters as ${name} in the command;
supply each axis with --grid name=v1,v2,... (repeatable). The cartesian product
of all axes is submitted.

Example:
  ziggurat sweep --grid seed=1,2,3 --grid lr=0.1,0.01 --gpus 1 -- \
    python train.py --seed '${seed}' --lr '${lr}'`,
		Args: cobra.MinimumNArgs(1),
		RunE: runSweep,
	}
	cmd.Flags().StringArrayVar(&sweepGrid, "grid", nil, "parameter axis: name=v1,v2,v3 (repeatable; cartesian product)")
	cmd.Flags().StringArrayVar(&sweepRequires, "require", nil, "required worker tag (applied to every task)")
	cmd.Flags().StringArrayVar(&sweepConstraints, "constraint", nil, "capability constraint (applied to every task)")
	cmd.Flags().StringVar(&sweepImage, "image", "", "OCI image ref (applied to every task)")
	cmd.Flags().IntVar(&sweepGPUs, "gpus", 0, "GPU devices required per task")
	cmd.Flags().IntVar(&sweepCPUs, "cpus", 0, "CPU cores required per task")
	cmd.Flags().StringVar(&sweepMemory, "memory", "", "memory requirement per task (e.g. 4GB)")
	cmd.Flags().IntVar(&sweepPriority, "priority", 0, "task priority (higher = sooner)")
	return cmd
}

func runSweep(cmd *cobra.Command, args []string) error {
	if len(sweepGrid) == 0 {
		return fmt.Errorf("at least one --grid axis is required (e.g. --grid seed=1,2,3)")
	}
	grid := make(map[string][]string, len(sweepGrid))
	for _, g := range sweepGrid {
		parts := strings.SplitN(g, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("invalid --grid %q (expected name=v1,v2,...)", g)
		}
		grid[parts[0]] = strings.Split(parts[1], ",")
	}

	template := map[string]any{"command": args}
	if len(sweepRequires) > 0 {
		template["requires"] = sweepRequires
	}
	if len(sweepConstraints) > 0 {
		template["constraints"] = sweepConstraints
	}
	if sweepImage != "" {
		template["image"] = sweepImage
	}
	resources := map[string]any{}
	if sweepCPUs > 0 {
		resources["cpu_cores"] = sweepCPUs
	}
	if sweepGPUs > 0 {
		resources["gpus"] = sweepGPUs
	}
	if sweepMemory != "" {
		size, err := util.ParseByteSize(sweepMemory)
		if err != nil {
			return fmt.Errorf("invalid --memory: %w", err)
		}
		resources["memory"] = size
	}
	if len(resources) > 0 {
		template["resources"] = resources
	}
	if sweepPriority != 0 {
		template["config"] = map[string]any{"priority": sweepPriority}
	}

	resp, err := doPost("/sweeps", map[string]any{"template": template, "grid": grid})
	if err != nil {
		return fmt.Errorf("submit sweep: %w", err)
	}
	var out struct {
		SweepID string   `json:"sweep_id"`
		Count   int      `json:"count"`
		TaskIDs []string `json:"task_ids"`
	}
	if err := readJSON(resp, &out); err != nil {
		return err
	}
	if jsonOut {
		printJSON(out)
		return nil
	}
	fmt.Printf("Submitted sweep %s: %d tasks\n", out.SweepID, out.Count)
	return nil
}
