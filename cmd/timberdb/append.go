package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
)

type ndjsonRecord struct {
	Timestamp string `json:"timestamp"`
	Source    string `json:"source"`
	Payload   string `json:"payload"`
}

func newAppendCmd() *cobra.Command {
	var dbPath, source, tsStr, payload, file string

	cmd := &cobra.Command{
		Use:   "append",
		Short: "Append a record or batch of records from an NDJSON file",
		RunE: func(_ *cobra.Command, _ []string) error {
			e, err := engine.Open(dbPath, engine.DefaultOptions())
			if err != nil {
				return err
			}
			defer func() {
				if err := e.Close(); err != nil {
					slog.Error("engine close", "err", err)
				}
			}()

			if file != "" {
				return appendBatch(e, file)
			}
			return appendSingle(e, source, tsStr, payload)
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "", "path to the database directory")
	cmd.Flags().StringVar(&source, "source", "", "source ID for the record")
	cmd.Flags().StringVar(&tsStr, "timestamp", "", "record timestamp (RFC3339); defaults to now")
	cmd.Flags().StringVar(&payload, "payload", "", "record payload bytes")
	cmd.Flags().StringVar(&file, "file", "", "NDJSON file of records to append")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	return cmd
}

func appendSingle(e *engine.Engine, source, tsStr, payload string) error {
	var ts time.Time
	if tsStr == "" {
		ts = time.Now()
	} else {
		var err error
		ts, err = parseTime(tsStr)
		if err != nil {
			return fmt.Errorf("--timestamp: %w", err)
		}
	}
	return e.Append(record.Record{
		Timestamp: ts.UnixNano(),
		SourceID:  []byte(source),
		Payload:   []byte(payload),
	})
}

func appendBatch(e *engine.Engine, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Error("file close", "err", err)
		}
	}()

	var recs []record.Record
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var nr ndjsonRecord
		if err := json.Unmarshal([]byte(line), &nr); err != nil {
			return fmt.Errorf("parse error: %w", err)
		}
		ts, err := parseTime(nr.Timestamp)
		if err != nil {
			return fmt.Errorf("timestamp: %w", err)
		}
		srcID, err := base64.StdEncoding.DecodeString(nr.Source)
		if err != nil {
			return fmt.Errorf("source base64: %w", err)
		}
		pay, err := base64.StdEncoding.DecodeString(nr.Payload)
		if err != nil {
			return fmt.Errorf("payload base64: %w", err)
		}
		recs = append(recs, record.Record{
			Timestamp: ts.UnixNano(),
			SourceID:  srcID,
			Payload:   pay,
		})
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return e.AppendBatch(recs)
}
