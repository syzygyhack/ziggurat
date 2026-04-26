package cmd

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

var lsPrefix string

func newLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls [prefix]",
		Short: "List stored objects",
		Long:  `Lists objects in storage. An optional positional argument or --prefix flag filters by key prefix.`,
		Args:  cobra.MaximumNArgs(1),
		RunE:  runLs,
	}
	cmd.Flags().StringVar(&lsPrefix, "prefix", "", "filter by key prefix")
	return cmd
}

func runLs(cmd *cobra.Command, args []string) error {
	// Positional arg takes precedence; --prefix is the fallback.
	prefix := lsPrefix
	if len(args) > 0 {
		prefix = args[0]
	}

	path := "/store"
	if prefix != "" {
		path += "?prefix=" + url.QueryEscape(prefix)
	}

	resp, err := doGet(path)
	if err != nil {
		return err
	}

	var keys []string
	if err := readJSON(resp, &keys); err != nil {
		return err
	}

	if jsonOut {
		printJSON(keys)
		return nil
	}

	if len(keys) == 0 {
		fmt.Println("No objects.")
		return nil
	}

	for _, k := range keys {
		fmt.Println(k)
	}
	return nil
}
