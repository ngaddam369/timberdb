package main

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/manifest"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
	"github.com/ngaddam369/timberdb/internal/wal"
)

func newInspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect database internals",
	}
	cmd.AddCommand(
		newInspectPartitionsCmd(),
		newInspectSSTCmd(),
		newInspectWALCmd(),
		newInspectManifestCmd(),
	)
	return cmd
}

func newInspectPartitionsCmd() *cobra.Command {
	var dbPath string

	cmd := &cobra.Command{
		Use:   "partitions",
		Short: "List all partitions with their state, SSTable count, and size",
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

			parts := e.Partitions()
			if len(parts) == 0 {
				fmt.Println("no partitions")
				return nil
			}

			fmt.Printf("%-45s %-8s %-10s %s\n", "WINDOW", "STATE", "SST FILES", "SIZE")
			for _, p := range parts {
				window := fmt.Sprintf("%s → %s",
					p.Start.Format(time.RFC3339),
					p.End.Format(time.RFC3339))
				fmt.Printf("%-45s %-8s %-10d %s\n",
					window, p.State, p.SSTFiles, fmtBytes(p.SizeBytes))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "", "path to the database directory")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	return cmd
}

func newInspectSSTCmd() *cobra.Command {
	var filePath string

	cmd := &cobra.Command{
		Use:   "sstable",
		Short: "Inspect an SSTable file (footer metadata)",
		RunE: func(_ *cobra.Command, _ []string) error {
			r, err := sstable.NewReader(filePath, nil)
			if err != nil {
				return err
			}
			defer func() {
				if err := r.Close(); err != nil {
					slog.Error("reader close", "err", err)
				}
			}()

			m := r.Meta()
			fmt.Printf("File:           %s\n", m.Path)
			fmt.Printf("PartitionStart: %s\n", time.Unix(0, m.PartitionStart).UTC().Format(time.RFC3339Nano))
			fmt.Printf("PartitionEnd:   %s\n", time.Unix(0, m.PartitionEnd).UTC().Format(time.RFC3339Nano))
			fmt.Printf("MinTimestamp:   %s\n", time.Unix(0, m.MinTimestamp).UTC().Format(time.RFC3339Nano))
			fmt.Printf("MaxTimestamp:   %s\n", time.Unix(0, m.MaxTimestamp).UTC().Format(time.RFC3339Nano))
			fmt.Printf("RecordCount:    %d\n", m.RecordCount)
			return nil
		},
	}

	cmd.Flags().StringVar(&filePath, "file", "", "path to the SSTable file")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	return cmd
}

func newInspectWALCmd() *cobra.Command {
	var filePath string

	cmd := &cobra.Command{
		Use:   "wal",
		Short: "Inspect a WAL segment (record count)",
		RunE: func(_ *cobra.Command, _ []string) error {
			w, err := wal.Open(filePath, wal.SyncNever)
			if err != nil {
				return err
			}
			defer func() {
				if err := w.Close(); err != nil {
					slog.Error("wal close", "err", err)
				}
			}()

			var count int
			if err := w.Replay(func(_ record.Record) {
				count++
			}); err != nil {
				return err
			}

			fmt.Printf("File:        %s\n", filePath)
			fmt.Printf("RecordCount: %d\n", count)
			return nil
		},
	}

	cmd.Flags().StringVar(&filePath, "file", "", "path to the WAL segment")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	return cmd
}

func newInspectManifestCmd() *cobra.Command {
	var dbPath string

	cmd := &cobra.Command{
		Use:   "manifest",
		Short: "Inspect the manifest (version edit history and live SSTable list)",
		RunE: func(_ *cobra.Command, _ []string) error {
			m, err := manifest.Open(filepath.Join(dbPath, "MANIFEST"))
			if err != nil {
				return err
			}
			defer func() {
				if err := m.Close(); err != nil {
					slog.Error("manifest close", "err", err)
				}
			}()

			live := make(map[string]bool)
			if err := m.Replay(func(edit manifest.VersionEdit) {
				added := len(edit.AddedFiles)
				deleted := len(edit.DeletedFiles)
				fmt.Printf("Edit #%d: +%d file", edit.Seq, added)
				if added != 1 {
					fmt.Print("s")
				}
				fmt.Printf(", -%d file", deleted)
				if deleted != 1 {
					fmt.Print("s")
				}
				fmt.Println()

				for _, f := range edit.AddedFiles {
					live[f.Path] = true
					fmt.Printf("  + %s [%d records, %s → %s]\n",
						f.Path,
						f.RecordCount,
						time.Unix(0, f.MinTimestamp).UTC().Format(time.RFC3339),
						time.Unix(0, f.MaxTimestamp).UTC().Format(time.RFC3339))
				}
				for _, f := range edit.DeletedFiles {
					delete(live, f.Path)
					fmt.Printf("  - %s\n", f.Path)
				}
			}); err != nil {
				return err
			}

			fmt.Printf("\nLive SSTable count: %d\n", len(live))
			return nil
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "", "path to the database directory")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	return cmd
}
