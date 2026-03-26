package main

import (
	"context"
	"fmt"
	"os"

	"github.com/martin/atb-cli-codex/internal/cli"
)

func main() {
	if err := cli.NewRootCommand(context.Background()).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
