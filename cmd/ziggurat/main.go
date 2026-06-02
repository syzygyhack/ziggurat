package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/syzygyhack/ziggurat/internal/cmd"
	"github.com/syzygyhack/ziggurat/internal/version"
)

func main() {
	root := cmd.NewRootCmd(version.Version, version.Commit)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		var exitErr *cmd.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.Code)
		}
		os.Exit(1)
	}
}
