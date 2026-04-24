package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/syzygyhack/ziggurat/internal/cmd"
)

// Set via ldflags.
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	root := cmd.NewRootCmd(version, commit)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		var exitErr *cmd.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.Code)
		}
		os.Exit(1)
	}
}
