// Package crash_test contains subprocess crash-recovery tests.
// Each test writes N records with WALSyncMode=SyncAlways, sends an os.Exit at a
// controlled crash point (simulating a kernel kill without engine.Close), then
// restarts the engine in the same directory and verifies all committed records
// are recoverable with no duplicates.
//
// Run with -count=100 to exercise 100 iterations per crash point.
package crash_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/metrics"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/wal"
)

// base is a fixed future timestamp used by both worker and parent so partition window
// alignment is deterministic across processes. Records at 2030-01-01 are never "late"
// regardless of the engine's LateArrivalWindow.
var base = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func drainEngine(t *testing.T, e *engine.Engine, start, end time.Time) []record.Record {
	t.Helper()
	it, err := e.Scan(start, end, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	var out []record.Record
	for it.Next() {
		out = append(out, it.Record())
	}
	require.NoError(t, it.Err())
	return out
}

func gatherCounter(m *metrics.Metrics, name string) float64 {
	families, err := m.Gather()
	if err != nil {
		return 0
	}
	for _, f := range families {
		if f.GetName() == name && f.GetType() == dto.MetricType_COUNTER {
			if mets := f.GetMetric(); len(mets) > 0 {
				return mets[0].GetCounter().GetValue()
			}
		}
	}
	return 0
}

// runCrashWorker is invoked inside the subprocess. It opens the engine at the
// directory from TIMBERDB_CRASH_DIR, writes TIMBERDB_CRASH_N records, then
// calls os.Exit(2) at the crash point named by TIMBERDB_CRASH_POINT — before
// engine.Close, so all pre-crash state is only durable if the WAL or SSTable
// has been fsynced.
func runCrashWorker() {
	dir := os.Getenv("TIMBERDB_CRASH_DIR")
	n, _ := strconv.Atoi(os.Getenv("TIMBERDB_CRASH_N"))
	point := os.Getenv("TIMBERDB_CRASH_POINT")
	if dir == "" || n == 0 || point == "" {
		os.Exit(1)
	}

	opts := engine.DefaultOptions()
	opts.WALSyncMode = wal.SyncAlways

	switch point {
	case "before_close":
		opts.MemtableSizeBytes = 1 << 30 // 1 GiB — prevent auto-flush
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour
	case "after_flush":
		opts.MemtableSizeBytes = 1 // flush on every append
		opts.CompactionCheckInterval = time.Hour
		opts.RetentionCheckInterval = time.Hour
	case "during_compaction":
		opts.MemtableSizeBytes = 1
		opts.MaxFilesPerPartition = 2
		opts.CompactionCheckInterval = 20 * time.Millisecond
		opts.RetentionCheckInterval = time.Hour
	default:
		os.Exit(1)
	}

	e, err := engine.Open(dir, opts)
	if err != nil {
		os.Exit(1)
	}

	for i := range n {
		if err := e.Append(record.Record{
			Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
			SourceID:  []byte("src"),
			Payload:   fmt.Appendf(nil, "p%d", i),
		}); err != nil {
			os.Exit(1)
		}
	}

	switch point {
	case "before_close":
		// crash immediately — all records are durable in WAL, none flushed to SSTable
		os.Exit(2)
	case "after_flush":
		// wait for at least one flush so the SSTable recovery path is exercised
		m := e.Metrics()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if gatherCounter(m, "timberdb_memtable_flushes_total") >= 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		os.Exit(2)
	case "during_compaction":
		// wait for enough flushes to trigger compaction (MaxFilesPerPartition=2 → 3 SSTables needed)
		m := e.Metrics()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if gatherCounter(m, "timberdb_memtable_flushes_total") >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		os.Exit(2)
	}

	os.Exit(1) // unreachable
}

// forkAndCrash spawns the current test binary as a subprocess with crash worker env vars.
// The subprocess writes n records then exits with code 2, simulating a kill.
func forkAndCrash(t *testing.T, dir, point string, n int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$")
	cmd.Env = append(os.Environ(),
		"TIMBERDB_CRASH_WORKER=1",
		"TIMBERDB_CRASH_DIR="+dir,
		"TIMBERDB_CRASH_POINT="+point,
		"TIMBERDB_CRASH_N="+strconv.Itoa(n),
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "subprocess must exit with an error; output:\n%s", out.String())
	require.Equal(t, 2, exitErr.ExitCode(),
		"subprocess must exit with crash code 2, got %d; output:\n%s", exitErr.ExitCode(), out.String())
}

// verifyRecovery reopens the engine at dir and confirms exactly n records are present
// with no duplicates — verifying that no committed record was lost and no record was
// replayed more than once.
func verifyRecovery(t *testing.T, dir string, n int) {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.WALSyncMode = wal.SyncAlways
	opts.CompactionCheckInterval = time.Hour
	opts.RetentionCheckInterval = time.Hour

	e, err := engine.Open(dir, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()

	got := drainEngine(t, e, base, base.Add(time.Hour))
	require.Len(t, got, n, "all WAL-committed records must survive crash")

	seen := make(map[int64]bool, n)
	for _, r := range got {
		require.False(t, seen[r.Timestamp], "duplicate record after recovery: ts=%d", r.Timestamp)
		seen[r.Timestamp] = true
	}
}

// TestCrashRecovery verifies durability across three crash points.
// Run with -count=100 for 100 iterations per crash point.
func TestCrashRecovery(t *testing.T) {
	if os.Getenv("TIMBERDB_CRASH_WORKER") != "" {
		runCrashWorker()
		panic("unreachable")
	}

	t.Run("before_close", func(t *testing.T) {
		dir := t.TempDir()
		forkAndCrash(t, dir, "before_close", 50)
		verifyRecovery(t, dir, 50)
	})

	t.Run("after_flush", func(t *testing.T) {
		dir := t.TempDir()
		forkAndCrash(t, dir, "after_flush", 50)
		verifyRecovery(t, dir, 50)
	})

	t.Run("during_compaction", func(t *testing.T) {
		dir := t.TempDir()
		forkAndCrash(t, dir, "during_compaction", 30)
		verifyRecovery(t, dir, 30)
	})
}
