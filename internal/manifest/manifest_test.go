package manifest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ngaddam369/timberdb/internal/manifest"
)

func openManifest(t *testing.T) (*manifest.Manifest, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "MANIFEST")
	m, err := manifest.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m, path
}

func replayAll(t *testing.T, path string) []manifest.VersionEdit {
	t.Helper()
	m, err := manifest.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	var edits []manifest.VersionEdit
	require.NoError(t, m.Replay(func(e manifest.VersionEdit) {
		edits = append(edits, e)
	}))
	return edits
}

func TestManifestAppendReplay(t *testing.T) {
	m, path := openManifest(t)

	edits := []manifest.VersionEdit{
		{AddedFiles: []manifest.FileEntry{
			{Path: "a.sst", PartitionStart: 0, PartitionEnd: 3600, MinTimestamp: 100, MaxTimestamp: 200, RecordCount: 10},
		}},
		{AddedFiles: []manifest.FileEntry{
			{Path: "b.sst", PartitionStart: 3600, PartitionEnd: 7200, MinTimestamp: 4000, MaxTimestamp: 5000, RecordCount: 5},
		}},
		{DeletedFiles: []manifest.FileEntry{
			{Path: "a.sst"},
		}},
	}
	for _, e := range edits {
		require.NoError(t, m.Append(e))
	}
	require.NoError(t, m.Close())

	got := replayAll(t, path)
	require.Len(t, got, 3)
	assert.Equal(t, "a.sst", got[0].AddedFiles[0].Path)
	assert.Equal(t, "b.sst", got[1].AddedFiles[0].Path)
	assert.Equal(t, "a.sst", got[2].DeletedFiles[0].Path)
}

func TestManifestSeqMonotonic(t *testing.T) {
	m, path := openManifest(t)

	for range 5 {
		require.NoError(t, m.Append(manifest.VersionEdit{
			AddedFiles: []manifest.FileEntry{{Path: "x.sst"}},
		}))
	}
	require.NoError(t, m.Close())

	got := replayAll(t, path)
	require.Len(t, got, 5)
	for i, e := range got {
		assert.Equal(t, uint64(i+1), e.Seq, "seq must be 1-indexed and monotonic")
	}
}

func TestManifestSeqResumesAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")

	// Session 1: write 3 edits.
	m1, err := manifest.Open(path)
	require.NoError(t, err)
	for range 3 {
		require.NoError(t, m1.Append(manifest.VersionEdit{AddedFiles: []manifest.FileEntry{{Path: "f.sst"}}}))
	}
	require.NoError(t, m1.Close())

	// Session 2: replay to restore seq, then write 2 more.
	m2, err := manifest.Open(path)
	require.NoError(t, err)
	require.NoError(t, m2.Replay(func(manifest.VersionEdit) {}))
	for range 2 {
		require.NoError(t, m2.Append(manifest.VersionEdit{AddedFiles: []manifest.FileEntry{{Path: "g.sst"}}}))
	}
	require.NoError(t, m2.Close())

	got := replayAll(t, path)
	require.Len(t, got, 5)
	assert.Equal(t, uint64(4), got[3].Seq)
	assert.Equal(t, uint64(5), got[4].Seq)
}

func TestManifestPartialWriteTruncated(t *testing.T) {
	_, path := openManifest(t)

	// Write 2 complete edits.
	m, err := manifest.Open(path)
	require.NoError(t, err)
	require.NoError(t, m.Append(manifest.VersionEdit{AddedFiles: []manifest.FileEntry{{Path: "a.sst"}}}))
	require.NoError(t, m.Append(manifest.VersionEdit{AddedFiles: []manifest.FileEntry{{Path: "b.sst"}}}))
	require.NoError(t, m.Close())

	// Truncate the last 4 bytes to simulate a crash mid-write.
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, info.Size()-4))

	got := replayAll(t, path)
	require.Len(t, got, 1, "only the first complete edit should be recovered")
	assert.Equal(t, "a.sst", got[0].AddedFiles[0].Path)
}

func TestManifestReplayRebuildLiveFiles(t *testing.T) {
	m, path := openManifest(t)

	require.NoError(t, m.Append(manifest.VersionEdit{
		AddedFiles: []manifest.FileEntry{
			{Path: "a.sst"}, {Path: "b.sst"},
		},
	}))
	require.NoError(t, m.Append(manifest.VersionEdit{
		AddedFiles:   []manifest.FileEntry{{Path: "c.sst"}},
		DeletedFiles: []manifest.FileEntry{{Path: "a.sst"}},
	}))
	require.NoError(t, m.Close())

	edits := replayAll(t, path)

	// Simulate engine rebuilding live file set from edits.
	live := make(map[string]manifest.FileEntry)
	for _, e := range edits {
		for _, f := range e.AddedFiles {
			live[f.Path] = f
		}
		for _, f := range e.DeletedFiles {
			delete(live, f.Path)
		}
	}

	assert.Len(t, live, 2)
	_, hasB := live["b.sst"]
	_, hasC := live["c.sst"]
	assert.True(t, hasB)
	assert.True(t, hasC)
	_, hasA := live["a.sst"]
	assert.False(t, hasA, "a.sst should have been removed by the delete edit")
}

func TestManifestEmptyReplay(t *testing.T) {
	m, path := openManifest(t)
	require.NoError(t, m.Close())

	got := replayAll(t, path)
	assert.Empty(t, got)
}
