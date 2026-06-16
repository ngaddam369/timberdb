package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "timberdb",
		Short: "Time-partitioned LSM storage engine for append-only time-ordered workloads",
	}

	root.AddCommand(
		newAppendCmd(),
		newScanCmd(),
		newInspectCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
