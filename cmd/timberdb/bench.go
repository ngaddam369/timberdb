package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
)

func newBenchCmd() *cobra.Command {
	var dbPath string
	var duration time.Duration
	var sources, recordSize, appendRate int
	var scanRatio float64

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run a mixed append+scan workload and print a throughput summary",
		RunE: func(_ *cobra.Command, _ []string) error {
			dir := dbPath
			if dir == "" {
				var err error
				dir, err = os.MkdirTemp("", "timberdb-bench-*")
				if err != nil {
					return err
				}
				defer func() {
					if err := os.RemoveAll(dir); err != nil {
						slog.Error("bench dir cleanup", "err", err)
					}
				}()
			}

			opts := engine.DefaultOptions()
			e, err := engine.Open(dir, opts)
			if err != nil {
				return err
			}
			defer func() {
				if err := e.Close(); err != nil {
					slog.Error("engine close", "err", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			workers := runtime.NumCPU()
			perWorker := max(appendRate/workers, 1)

			var totalAppends atomic.Int64
			var appendErrors atomic.Int64

			// Writer goroutines.
			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					interval := time.Second / time.Duration(perWorker)
					ticker := time.NewTicker(interval)
					defer ticker.Stop()

					srcID := fmt.Sprintf("src-%d", rand.IntN(sources))
					payload := make([]byte, recordSize)

					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							if err := e.Append(record.Record{
								Timestamp: time.Now().UnixNano(),
								SourceID:  []byte(srcID),
								Payload:   payload,
							}); err != nil {
								appendErrors.Add(1)
							} else {
								totalAppends.Add(1)
							}
						}
					}
				})
			}

			// Optional scanner goroutine.
			var scanLatencies []int64
			var scanMu sync.Mutex
			if scanRatio > 0 {
				wg.Go(func() {
					oldest := time.Now()
					for {
						if ctx.Err() != nil {
							return
						}
						start := time.Now()
						it, err := e.Scan(oldest, time.Now(), nil)
						if err == nil {
							for it.Next() {
							}
							if err := it.Err(); err != nil {
								slog.Error("scan iterator error", "err", err)
							}
							if cerr := it.Close(); cerr != nil {
								slog.Error("scan iterator close", "err", cerr)
							}
						}
						latNs := time.Since(start).Nanoseconds()
						scanMu.Lock()
						scanLatencies = append(scanLatencies, latNs)
						scanMu.Unlock()

						// Pace scans to approximately the requested scan ratio.
						if scanRatio < 1 {
							sleepNs := time.Duration(float64(latNs) * (1 - scanRatio) / scanRatio)
							select {
							case <-ctx.Done():
								return
							case <-time.After(sleepNs):
							}
						}
					}
				})
			}

			wg.Wait()

			n := totalAppends.Load()
			errs := appendErrors.Load()
			actualRate := float64(n) / duration.Seconds()

			fmt.Printf("\n=== Benchmark Results (%s) ===\n", duration)
			fmt.Printf("Records written:  %d\n", n)
			fmt.Printf("Append errors:    %d\n", errs)
			fmt.Printf("Throughput:       %.0f records/sec\n", actualRate)
			fmt.Printf("Workers:          %d\n", workers)
			fmt.Printf("Record size:      %d B\n", recordSize)

			scanMu.Lock()
			defer scanMu.Unlock()
			if len(scanLatencies) > 0 {
				slices.Sort(scanLatencies)
				p50 := time.Duration(scanLatencies[len(scanLatencies)/2])
				p99 := time.Duration(scanLatencies[99*len(scanLatencies)/100])
				fmt.Printf("Scans:            %d\n", len(scanLatencies))
				fmt.Printf("Scan P50:         %s\n", p50)
				fmt.Printf("Scan P99:         %s\n", p99)
			}

			parts := e.Partitions()
			var sstFiles int
			var sstBytes int64
			for _, p := range parts {
				sstFiles += p.SSTFiles
				sstBytes += p.SizeBytes
			}
			fmt.Printf("Partitions:       %d\n", len(parts))
			fmt.Printf("SSTable files:    %d\n", sstFiles)
			fmt.Printf("SSTable size:     %s\n", fmtBytes(sstBytes))

			return nil
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "", "database directory (uses temp dir if omitted)")
	cmd.Flags().DurationVar(&duration, "duration", 30*time.Second, "benchmark duration")
	cmd.Flags().IntVar(&sources, "sources", 100, "number of distinct sources")
	cmd.Flags().IntVar(&recordSize, "record-size", 512, "payload size in bytes")
	cmd.Flags().IntVar(&appendRate, "append-rate", 100000, "target appends per second")
	cmd.Flags().Float64Var(&scanRatio, "scan-ratio", 0, "fraction of time spent scanning (0 = no scans)")
	return cmd
}
