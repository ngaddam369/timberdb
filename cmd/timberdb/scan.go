package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ngaddam369/timberdb/internal/engine"
)

func newScanCmd() *cobra.Command {
	var dbPath, startStr, endStr, source string

	cmd := &cobra.Command{
		Use:   "scan",
		Short: "Scan records by time range and output as NDJSON",
		RunE: func(_ *cobra.Command, _ []string) error {
			start, err := parseTime(startStr)
			if err != nil {
				return fmt.Errorf("--start: %w", err)
			}
			end := time.Now()
			if endStr != "" {
				end, err = parseTime(endStr)
				if err != nil {
					return fmt.Errorf("--end: %w", err)
				}
			}

			e, err := engine.Open(dbPath, engine.DefaultOptions())
			if err != nil {
				return err
			}
			defer func() {
				if err := e.Close(); err != nil {
					slog.Error("engine close", "err", err)
				}
			}()

			var opts *engine.ScanOptions
			if source != "" {
				opts = &engine.ScanOptions{SourceID: []byte(source)}
			}

			it, err := e.Scan(start, end, opts)
			if err != nil {
				return err
			}
			defer func() {
				if err := it.Close(); err != nil {
					slog.Error("iterator close", "err", err)
				}
			}()

			enc := json.NewEncoder(os.Stdout)
			for it.Next() {
				r := it.Record()
				if err := enc.Encode(struct {
					Timestamp string `json:"timestamp"`
					Source    string `json:"source"`
					Payload   string `json:"payload"`
				}{
					Timestamp: time.Unix(0, r.Timestamp).UTC().Format(time.RFC3339Nano),
					Source:    base64.StdEncoding.EncodeToString(r.SourceID),
					Payload:   base64.StdEncoding.EncodeToString(r.Payload),
				}); err != nil {
					return err
				}
			}
			return it.Err()
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "", "path to the database directory")
	cmd.Flags().StringVar(&startStr, "start", "", "start time (RFC3339, inclusive)")
	cmd.Flags().StringVar(&endStr, "end", "", "end time (RFC3339, exclusive); defaults to now")
	cmd.Flags().StringVar(&source, "source", "", "filter by source ID")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("start"); err != nil {
		panic(err)
	}
	return cmd
}
