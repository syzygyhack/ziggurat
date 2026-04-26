package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newVersionCmd(version, commit string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if jsonOut {
				printJSON(map[string]string{
					"version": version,
					"commit":  commit,
				})
				return
			}
			fmt.Printf("ziggurat %s (%s)\n", version, commit)
		},
	}
}
