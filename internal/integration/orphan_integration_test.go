package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/wal"
)

func TestOrphanDetection(t *testing.T) {
	t.Run("orphaned_sst_deleted", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		const n = 10

		// Session 1: write records and close — flushes to SST, manifest committed.
		func() {
			opts := engine.DefaultOptions()
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			for i := range n {
				require.NoError(t, e.Append(record.Record{
					Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
					SourceID:  []byte("src"),
					Payload:   []byte("p"),
				}))
			}
		}()

		// Plant an orphaned SST: a file on disk with no manifest entry.
		orphanPath := filepath.Join(dir, "orphan.sst")
		require.NoError(t, os.WriteFile(orphanPath, []byte("not a real sstable"), 0o600))

		// Session 2: reopen — orphan detection must delete the stray file.
		func() {
			opts := engine.DefaultOptions()
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			_, err = os.Stat(orphanPath)
			assert.True(t, os.IsNotExist(err), "orphaned SST must be deleted on Open")

			got := drainEngine(t, e, base, base.Add(time.Hour))
			assert.Len(t, got, n, "all real records must still be scannable")
		}()
	})

	t.Run("covered_wal_deleted", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Now().Add(time.Hour).Truncate(time.Hour)
		const n = 10

		var recs []record.Record
		for i := range n {
			recs = append(recs, record.Record{
				Timestamp: base.Add(time.Duration(i) * time.Second).UnixNano(),
				SourceID:  []byte("src"),
				Payload:   []byte("p"),
			})
		}

		// Session 1: write records and close — SST flushed, manifest committed, WAL deleted.
		func() {
			opts := engine.DefaultOptions()
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			for _, r := range recs {
				require.NoError(t, e.Append(r))
			}
		}()

		// Simulate crash-after-flush: plant a WAL whose records are fully covered
		// by the SST that was just committed to the manifest. The engine should
		// detect that this WAL contributes no new records and delete it.
		coveredWALPath := filepath.Join(dir, "wal-000001.wal")
		func() {
			w, err := wal.Open(coveredWALPath, wal.SyncAlways)
			require.NoError(t, err)
			defer func() { require.NoError(t, w.Close()) }()

			for _, r := range recs {
				require.NoError(t, w.Append(r))
			}
		}()

		// Session 2: reopen — covered WAL must be deleted and records not duplicated.
		func() {
			opts := engine.DefaultOptions()
			e, err := engine.Open(dir, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()

			_, err = os.Stat(coveredWALPath)
			assert.True(t, os.IsNotExist(err), "covered WAL must be deleted on Open")

			got := drainEngine(t, e, base, base.Add(time.Hour))
			assert.Len(t, got, n, "records must not be duplicated after covered WAL cleanup")
		}()
	})
}
