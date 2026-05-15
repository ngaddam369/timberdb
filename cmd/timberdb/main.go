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
		newBenchCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newAppendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "append",
		Short: "Append a record to the database",
		RunE: func(_ *cobra.Command, _ []string) error {
			return fmt.Errorf("not implemented")
		},
	}
	cmd.Flags().String("db", "", "path to the database directory")
	cmd.Flags().String("source", "", "source ID for the record")
	cmd.Flags().String("timestamp", "", "record timestamp (RFC3339)")
	cmd.Flags().String("payload", "", "record payload bytes")
	cmd.Flags().String("file", "", "NDJSON file of records to append")
	return cmd
}

func newScanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scan",
		Short: "Scan records by time range",
		RunE: func(_ *cobra.Command, _ []string) error {
			return fmt.Errorf("not implemented")
		},
	}
	cmd.Flags().String("db", "", "path to the database directory")
	cmd.Flags().String("start", "", "start time (RFC3339, inclusive)")
	cmd.Flags().String("end", "", "end time (RFC3339, exclusive)")
	cmd.Flags().String("source", "", "filter by source ID")
	return cmd
}

func newInspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect database internals",
	}
	cmd.AddCommand(
		&cobra.Command{Use: "partitions", Short: "List all partitions and their state"},
		&cobra.Command{Use: "sstable", Short: "Inspect an SSTable file (footer, time index, record count)"},
		&cobra.Command{Use: "wal", Short: "Inspect the WAL (record count, size, sequence)"},
		&cobra.Command{Use: "manifest", Short: "Inspect the manifest (version edits, SSTable list)"},
	)
	return cmd
}

func newBenchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run the benchmark suite",
		RunE: func(_ *cobra.Command, _ []string) error {
			return fmt.Errorf("not implemented")
		},
	}
	cmd.Flags().String("db", "", "path to the database directory")
	cmd.Flags().Duration("duration", 0, "benchmark duration")
	cmd.Flags().Int("sources", 100, "number of distinct sources")
	cmd.Flags().Int("record-size", 512, "record payload size in bytes")
	cmd.Flags().Int("append-rate", 100000, "target appends per second")
	cmd.Flags().Float64("scan-ratio", 0.1, "fraction of operations that are scans")
	return cmd
}
