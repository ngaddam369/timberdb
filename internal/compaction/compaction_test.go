package compaction

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/manifest"
	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
)

// buildSSTable writes records to a new SSTable at path and returns its meta.
func buildSSTable(t *testing.T, path string, win partition.PartitionWindow, records []record.Record) sstable.SSTableMeta {
	t.Helper()
	w, err := sstable.NewWriter(path, sstable.WriterOptions{
		BlockSizeBytes: sstable.DefaultWriterOptions().BlockSizeBytes,
		PartitionStart: win.Start,
		PartitionEnd:   win.End,
	})
	require.NoError(t, err)
	for _, r := range records {
		require.NoError(t, w.Add(r))
	}
	meta, err := w.Finish()
	require.NoError(t, err)
	return meta
}

func TestCompaction(t *testing.T) {
	win := partition.PartitionWindow{Start: 0, End: int64(time.Hour)}
	strategy := &FIFOStrategy{MaxFilesPerPartition: 3}

	t.Run("fifo_below_threshold", func(t *testing.T) {
		files := make([]sstable.SSTableMeta, 3) // exactly at threshold — no merge
		assert.Nil(t, strategy.PickMerge(win, files))
	})

	t.Run("fifo_above_threshold", func(t *testing.T) {
		files := make([]sstable.SSTableMeta, 4) // one over — merge all
		job := strategy.PickMerge(win, files)
		require.NotNil(t, job)
		assert.Equal(t, win, job.Window)
		assert.Len(t, job.Inputs, 4)
	})

	t.Run("fifo_pick_expired_none", func(t *testing.T) {
		files := []sstable.SSTableMeta{
			{MaxTimestamp: 1000},
			{MaxTimestamp: 2000},
		}
		assert.Empty(t, strategy.PickExpired(files, 500))
	})

	t.Run("fifo_pick_expired_some", func(t *testing.T) {
		files := []sstable.SSTableMeta{
			{MaxTimestamp: 100},
			{MaxTimestamp: 500},
			{MaxTimestamp: 1000},
		}
		expired := strategy.PickExpired(files, 501)
		require.Len(t, expired, 2)
		assert.Equal(t, int64(100), expired[0].MaxTimestamp)
		assert.Equal(t, int64(500), expired[1].MaxTimestamp)
	})

	t.Run("merge_two_sstables", func(t *testing.T) {
		dir := t.TempDir()
		m, err := manifest.Open(filepath.Join(dir, "MANIFEST"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, m.Close()) })

		wopts := sstable.WriterOptions{
			BlockSizeBytes: sstable.DefaultWriterOptions().BlockSizeBytes,
			PartitionStart: win.Start,
			PartitionEnd:   win.End,
		}

		// Two SSTables with interleaved timestamps — merge must produce sorted output.
		path1 := filepath.Join(dir, "a.sst")
		path2 := filepath.Join(dir, "b.sst")
		buildSSTable(t, path1, win, []record.Record{
			{Timestamp: 1, SourceID: []byte("a"), Payload: []byte("p1")},
			{Timestamp: 3, SourceID: []byte("a"), Payload: []byte("p3")},
		})
		buildSSTable(t, path2, win, []record.Record{
			{Timestamp: 2, SourceID: []byte("b"), Payload: []byte("p2")},
			{Timestamp: 4, SourceID: []byte("b"), Payload: []byte("p4")},
		})

		r1, err := sstable.NewReader(path1)
		require.NoError(t, err)
		r2, err := sstable.NewReader(path2)
		require.NoError(t, err)

		outPath := filepath.Join(dir, "merged.sst")
		merged, err := Merge([]*sstable.Reader{r1, r2}, outPath, wopts, m)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, r1.Close()) })
		t.Cleanup(func() { require.NoError(t, r2.Close()) })
		t.Cleanup(func() { require.NoError(t, merged.Close()) })

		assert.Equal(t, uint64(4), merged.Meta().RecordCount)

		it, err := merged.Scan(math.MinInt64, math.MaxInt64, nil)
		require.NoError(t, err)
		var got []record.Record
		for it.Next() {
			got = append(got, it.Record())
		}
		require.NoError(t, it.Close())

		require.Len(t, got, 4)
		assert.Equal(t, int64(1), got[0].Timestamp)
		assert.Equal(t, int64(2), got[1].Timestamp)
		assert.Equal(t, int64(3), got[2].Timestamp)
		assert.Equal(t, int64(4), got[3].Timestamp)
	})

	t.Run("merge_manifest_updated", func(t *testing.T) {
		dir := t.TempDir()
		manifestPath := filepath.Join(dir, "MANIFEST")
		m, err := manifest.Open(manifestPath)
		require.NoError(t, err)

		wopts := sstable.WriterOptions{
			BlockSizeBytes: sstable.DefaultWriterOptions().BlockSizeBytes,
			PartitionStart: win.Start,
			PartitionEnd:   win.End,
		}

		path1 := filepath.Join(dir, "1.sst")
		path2 := filepath.Join(dir, "2.sst")
		buildSSTable(t, path1, win, []record.Record{
			{Timestamp: 10, SourceID: []byte("s"), Payload: []byte("p")},
		})
		buildSSTable(t, path2, win, []record.Record{
			{Timestamp: 20, SourceID: []byte("s"), Payload: []byte("p")},
		})

		r1, err := sstable.NewReader(path1)
		require.NoError(t, err)
		r2, err := sstable.NewReader(path2)
		require.NoError(t, err)

		outPath := filepath.Join(dir, "merged.sst")
		merged, err := Merge([]*sstable.Reader{r1, r2}, outPath, wopts, m)
		require.NoError(t, err)
		require.NoError(t, r1.Close())
		require.NoError(t, r2.Close())
		require.NoError(t, merged.Close())
		require.NoError(t, m.Close())

		// Replay manifest in a fresh session and verify live-file state.
		m2, err := manifest.Open(manifestPath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, m2.Close()) })

		live := map[string]bool{}
		require.NoError(t, m2.Replay(func(e manifest.VersionEdit) {
			for _, f := range e.AddedFiles {
				live[f.Path] = true
			}
			for _, f := range e.DeletedFiles {
				delete(live, f.Path)
			}
		}))
		assert.True(t, live[outPath], "merged file must be live in manifest")
		assert.False(t, live[path1], "input 1 must not be live in manifest")
		assert.False(t, live[path2], "input 2 must not be live in manifest")
	})

	t.Run("merge_inputs_deleted", func(t *testing.T) {
		dir := t.TempDir()
		m, err := manifest.Open(filepath.Join(dir, "MANIFEST"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, m.Close()) })

		wopts := sstable.WriterOptions{
			BlockSizeBytes: sstable.DefaultWriterOptions().BlockSizeBytes,
			PartitionStart: win.Start,
			PartitionEnd:   win.End,
		}

		path1 := filepath.Join(dir, "x.sst")
		path2 := filepath.Join(dir, "y.sst")
		buildSSTable(t, path1, win, []record.Record{
			{Timestamp: 5, SourceID: []byte("s"), Payload: []byte("p")},
		})
		buildSSTable(t, path2, win, []record.Record{
			{Timestamp: 6, SourceID: []byte("s"), Payload: []byte("p")},
		})

		r1, err := sstable.NewReader(path1)
		require.NoError(t, err)
		r2, err := sstable.NewReader(path2)
		require.NoError(t, err)

		merged, err := Merge([]*sstable.Reader{r1, r2}, filepath.Join(dir, "m.sst"), wopts, m)
		require.NoError(t, err)
		require.NoError(t, r1.Close())
		require.NoError(t, r2.Close())
		require.NoError(t, merged.Close())

		_, err1 := os.Stat(path1)
		_, err2 := os.Stat(path2)
		assert.True(t, os.IsNotExist(err1), "input file 1 must be deleted from disk")
		assert.True(t, os.IsNotExist(err2), "input file 2 must be deleted from disk")
	})
}
