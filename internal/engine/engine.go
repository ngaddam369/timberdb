// Package engine is the top-level coordinator. It wires together the WAL,
// PartitionRouter, Memtable, SSTable flush, Manifest, Compaction, and Metrics
// into the public Append/Scan/Close API.
package engine

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ngaddam369/timberdb/internal/compaction"
	"github.com/ngaddam369/timberdb/internal/manifest"
	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
	"github.com/ngaddam369/timberdb/internal/wal"
)

// ErrClosed is returned by Append and Scan after Close has been called.
var ErrClosed = errors.New("timberdb: engine is closed")

// Engine wires WAL, partition router, SSTable files, and manifest into a single
// durable append+scan store. All exported methods are safe for concurrent use.
type Engine struct {
	mu       sync.RWMutex
	dir      string
	opts     Options
	wal      *wal.WAL
	router   *partition.Router
	manifest *manifest.Manifest
	// files maps each flushed partition window to its open SSTable readers,
	// in the order they were flushed (oldest first).
	files        map[partition.PartitionWindow][]*sstable.Reader
	maxFlushedTS map[partition.PartitionWindow]int64 // highest ts already in an SSTable
	flushCh      chan *partition.TimePartition
	compactCh    chan partition.PartitionWindow // signals runCompactor after each flush
	closeCh      chan struct{}
	wg           sync.WaitGroup
	walSeq       int
	compactSeq   int
	// closedReaders holds readers whose underlying files were deleted by compaction or
	// retention. They are closed on engine shutdown to avoid use-after-free during
	// in-flight scans that may still hold iterators from those readers.
	closedReaders []*sstable.Reader
	strategy      compaction.Strategy
	closed        bool
}

// Open opens or creates a timberdb store at dir with the given options.
// On restart it replays the manifest to re-open all known SSTables, then
// replays WAL segments to reconstruct any unflushed memtable state.
func Open(dir string, opts Options) (*Engine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	m, err := manifest.Open(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		return nil, err
	}

	e := &Engine{
		dir:          dir,
		opts:         opts,
		manifest:     m,
		router:       partition.NewRouter(opts.PartitionDuration, opts.LateArrivalWindow, opts.LateArrivalMode),
		files:        make(map[partition.PartitionWindow][]*sstable.Reader),
		maxFlushedTS: make(map[partition.PartitionWindow]int64),
		flushCh:      make(chan *partition.TimePartition, 16),
		compactCh:    make(chan partition.PartitionWindow, 16),
		closeCh:      make(chan struct{}),
		strategy:     &compaction.FIFOStrategy{MaxFilesPerPartition: opts.MaxFilesPerPartition},
	}

	// Replay manifest — open all live SSTables and track maxFlushedTS per window.
	if err := m.Replay(func(edit manifest.VersionEdit) {
		for _, fe := range edit.AddedFiles {
			r, rerr := sstable.NewReader(fe.Path)
			if rerr != nil {
				slog.Error("engine: failed to open SSTable from manifest", "path", fe.Path, "err", rerr)
				return
			}
			win := partition.PartitionWindow{Start: fe.PartitionStart, End: fe.PartitionEnd}
			e.files[win] = append(e.files[win], r)
			if fe.MaxTimestamp > e.maxFlushedTS[win] {
				e.maxFlushedTS[win] = fe.MaxTimestamp
			}
			// Register an empty partition for this window so router.Overlapping
			// returns it during Scan even if WAL replay adds no new records.
			e.router.AddPartition(partition.NewPartition(win, ""))
		}
		for _, fe := range edit.DeletedFiles {
			win := partition.PartitionWindow{Start: fe.PartitionStart, End: fe.PartitionEnd}
			readers := e.files[win]
			for i, r := range readers {
				if r.Meta().Path == fe.Path {
					if cerr := r.Close(); cerr != nil {
						slog.Error("engine: close deleted SSTable failed", "path", fe.Path, "err", cerr)
					}
					e.files[win] = append(readers[:i], readers[i+1:]...)
					break
				}
			}
		}
	}); err != nil {
		if cerr := m.Close(); cerr != nil {
			slog.Error("engine: manifest close on replay error", "err", cerr)
		}
		return nil, err
	}

	// Discover and replay all WAL segments in sorted name order.
	walPaths, err := filepath.Glob(filepath.Join(dir, "wal-*.wal"))
	if err != nil {
		if cerr := m.Close(); cerr != nil {
			slog.Error("engine: manifest close on glob error", "err", cerr)
		}
		return nil, err
	}
	sort.Strings(walPaths)

	// Track the highest WAL sequence number so new rotations don't collide.
	for _, p := range walPaths {
		var seq int
		if _, serr := fmt.Sscanf(filepath.Base(p), "wal-%d.wal", &seq); serr == nil && seq > e.walSeq {
			e.walSeq = seq
		}
	}

	// Replay WAL segments; skip records already covered by a flushed SSTable.
	for _, walPath := range walPaths {
		w, werr := wal.Open(walPath, opts.WALSyncMode)
		if werr != nil {
			if cerr := m.Close(); cerr != nil {
				slog.Error("engine: manifest close on WAL open error", "err", cerr)
			}
			return nil, werr
		}
		replayErr := w.Replay(func(rec record.Record) {
			win := e.router.WindowFor(rec.Timestamp)
			if maxTS, ok := e.maxFlushedTS[win]; ok && rec.Timestamp <= maxTS {
				return // already persisted in an SSTable
			}
			p, rerr := e.router.Route(rec.Timestamp)
			if rerr != nil {
				slog.Error("engine: WAL replay route failed", "ts", rec.Timestamp, "err", rerr)
				return
			}
			if aerr := p.Append(rec); aerr != nil {
				slog.Error("engine: WAL replay append failed", "err", aerr)
			}
		})
		if cerr := w.Close(); cerr != nil {
			slog.Error("engine: WAL segment close failed", "path", walPath, "err", cerr)
		}
		if replayErr != nil {
			if cerr := m.Close(); cerr != nil {
				slog.Error("engine: manifest close on WAL replay error", "err", cerr)
			}
			return nil, replayErr
		}
	}

	// Open the active WAL for new writes.
	e.walSeq++
	activeWAL, err := wal.Open(e.walPath(e.walSeq), opts.WALSyncMode)
	if err != nil {
		if cerr := m.Close(); cerr != nil {
			slog.Error("engine: manifest close on active WAL open error", "err", cerr)
		}
		return nil, err
	}
	e.wal = activeWAL

	e.wg.Go(e.runFlusher)
	e.wg.Go(e.runCompactor)
	return e, nil
}

// Append durably writes rec to the WAL then routes it to the correct partition memtable.
// If the partition's memtable exceeds MemtableSizeBytes, a background flush is triggered.
func (e *Engine) Append(rec record.Record) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	if err := e.wal.Append(rec); err != nil {
		return err
	}
	p, err := e.router.Route(rec.Timestamp)
	if err != nil {
		return err
	}
	if err := p.Append(rec); err != nil {
		return err
	}
	if p.MemtableSize() > e.opts.MemtableSizeBytes {
		select {
		case e.flushCh <- p:
		default: // flush already queued for this partition
		}
	}
	return nil
}

// AppendBatch writes every record in recs via Append. The batch is not atomic —
// individual records are durable as soon as Append returns for each one.
func (e *Engine) AppendBatch(recs []record.Record) error {
	for _, r := range recs {
		if err := e.Append(r); err != nil {
			return err
		}
	}
	return nil
}

// Scan returns a merged iterator over all records with timestamps in [start, end),
// optionally filtered to a single sourceID (nil = all sources).
// The returned iterator must be closed by the caller.
func (e *Engine) Scan(start, end time.Time, sourceID []byte) (record.Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrClosed
	}

	startNs := start.UnixNano()
	endNs := end.UnixNano()

	partitions := e.router.Overlapping(startNs, endNs)
	var iters []record.Iterator
	for _, p := range partitions {
		iters = append(iters, p.Scan(startNs, endNs, sourceID))
		for _, r := range e.files[p.Window] {
			it, err := r.Scan(startNs, endNs, sourceID)
			if err != nil {
				for _, opened := range iters {
					if cerr := opened.Close(); cerr != nil {
						slog.Error("engine: scan cleanup close failed", "err", cerr)
					}
				}
				return nil, err
			}
			iters = append(iters, it)
		}
	}
	return newMergeIterator(iters), nil
}

// Close flushes all open partitions to SSTables, waits for the background flusher
// to finish, closes all SSTable readers, and closes the manifest and WAL.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeCh)
	e.mu.Unlock()

	e.wg.Wait()

	// Flush any remaining open partitions that have data.
	for _, p := range e.router.All() {
		if p.State() == partition.StateOpen && p.MemtableSize() > 0 {
			if err := e.flushPartition(p); err != nil {
				slog.Error("engine: flush on close failed", "err", err)
			}
		}
	}

	var firstErr error
	e.mu.Lock()
	for _, readers := range e.files {
		for _, r := range readers {
			if err := r.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, r := range e.closedReaders {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.mu.Unlock()

	if err := e.manifest.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (e *Engine) runFlusher() {
	for {
		select {
		case <-e.closeCh:
			return
		case p := <-e.flushCh:
			if err := e.flushPartition(p); err != nil {
				slog.Error("engine: background flush failed", "err", err)
			}
		}
	}
}

func (e *Engine) flushPartition(p *partition.TimePartition) error {
	e.mu.RLock()
	walSeq := e.walSeq
	e.mu.RUnlock()

	sstPath := filepath.Join(e.dir, fmt.Sprintf("%d-%06d.sst", p.Window.Start, walSeq))
	wopts := sstable.WriterOptions{
		BlockSizeBytes: e.opts.BlockSizeBytes,
		IndexSources:   e.opts.IndexSources,
		PartitionStart: p.Window.Start,
		PartitionEnd:   p.Window.End,
	}
	w, err := sstable.NewWriter(sstPath, wopts)
	if err != nil {
		return err
	}

	// SwapMemtable atomically snapshots the current data and resets the partition's
	// memtable so new writes can continue without hitting ErrPartitionSealed.
	it := p.SwapMemtable()
	for it.Next() {
		if err := w.Add(it.Record()); err != nil {
			if cerr := it.Close(); cerr != nil {
				slog.Error("engine: iterator close on flush error", "err", cerr)
			}
			return err
		}
	}
	if err := it.Close(); err != nil {
		return err
	}

	meta, err := w.Finish()
	if err != nil {
		return err
	}

	if meta.RecordCount == 0 {
		if err := os.Remove(sstPath); err != nil && !os.IsNotExist(err) {
			slog.Error("engine: remove empty SSTable failed", "path", sstPath, "err", err)
		}
		return nil
	}

	if err := e.manifest.Append(manifest.VersionEdit{
		AddedFiles: []manifest.FileEntry{{
			Path:           sstPath,
			PartitionStart: meta.PartitionStart,
			PartitionEnd:   meta.PartitionEnd,
			MinTimestamp:   meta.MinTimestamp,
			MaxTimestamp:   meta.MaxTimestamp,
			RecordCount:    meta.RecordCount,
		}},
	}); err != nil {
		return err
	}

	// Rotate the WAL so the next segment only contains post-flush records.
	e.mu.Lock()
	oldWALPath := e.walPath(e.walSeq)
	e.walSeq++
	newWALPath := e.walPath(e.walSeq)
	e.mu.Unlock()

	if err := e.wal.Rotate(newWALPath); err != nil {
		return err
	}
	if err := os.Remove(oldWALPath); err != nil && !os.IsNotExist(err) {
		slog.Error("engine: remove old WAL segment failed", "path", oldWALPath, "err", err)
	}

	r, err := sstable.NewReader(sstPath)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.files[p.Window] = append(e.files[p.Window], r)
	if meta.MaxTimestamp > e.maxFlushedTS[p.Window] {
		e.maxFlushedTS[p.Window] = meta.MaxTimestamp
	}
	e.mu.Unlock()

	// Signal the compactor so it can check immediately rather than waiting for the ticker.
	select {
	case e.compactCh <- p.Window:
	default:
	}

	return nil
}

func (e *Engine) walPath(seq int) string {
	return filepath.Join(e.dir, fmt.Sprintf("wal-%06d.wal", seq))
}

func (e *Engine) runCompactor() {
	compactTicker := time.NewTicker(e.opts.CompactionCheckInterval)
	defer compactTicker.Stop()

	var retentionCh <-chan time.Time
	if e.opts.RetentionDuration > 0 {
		rt := time.NewTicker(e.opts.RetentionCheckInterval)
		defer rt.Stop()
		retentionCh = rt.C
	}

	for {
		select {
		case <-e.closeCh:
			return
		case win := <-e.compactCh:
			e.maybeCompact(win)
		case <-compactTicker.C:
			e.sweepCompaction()
		case <-retentionCh:
			e.sweepRetention()
		}
	}
}

func (e *Engine) sweepCompaction() {
	e.mu.RLock()
	windows := make([]partition.PartitionWindow, 0, len(e.files))
	for win := range e.files {
		windows = append(windows, win)
	}
	e.mu.RUnlock()

	for _, win := range windows {
		e.maybeCompact(win)
	}
}

func (e *Engine) maybeCompact(win partition.PartitionWindow) {
	e.mu.RLock()
	readers := make([]*sstable.Reader, len(e.files[win]))
	copy(readers, e.files[win])
	e.mu.RUnlock()

	if len(readers) == 0 {
		return
	}

	metas := make([]sstable.SSTableMeta, len(readers))
	for i, r := range readers {
		metas[i] = r.Meta()
	}

	if e.strategy.PickMerge(win, metas) == nil {
		return
	}

	e.mu.Lock()
	e.compactSeq++
	seq := e.compactSeq
	e.mu.Unlock()

	outputPath := filepath.Join(e.dir, fmt.Sprintf("%d-compact-%06d.sst", win.Start, seq))
	wopts := sstable.WriterOptions{
		BlockSizeBytes: e.opts.BlockSizeBytes,
		IndexSources:   e.opts.IndexSources,
		PartitionStart: win.Start,
		PartitionEnd:   win.End,
	}

	merged, err := compaction.Merge(readers, outputPath, wopts, e.manifest)
	if err != nil {
		slog.Error("engine: compaction failed", "window", win.Start, "err", err)
		return
	}

	mergedPaths := make(map[string]struct{}, len(readers))
	for _, r := range readers {
		mergedPaths[r.Meta().Path] = struct{}{}
	}

	e.mu.Lock()
	current := e.files[win]
	survived := make([]*sstable.Reader, 0, len(current))
	var toClose []*sstable.Reader
	for _, r := range current {
		if _, wasCompacted := mergedPaths[r.Meta().Path]; wasCompacted {
			toClose = append(toClose, r)
		} else {
			survived = append(survived, r)
		}
	}
	e.files[win] = append(survived, merged)
	e.closedReaders = append(e.closedReaders, toClose...)
	e.mu.Unlock()
}

func (e *Engine) sweepRetention() {
	if e.opts.RetentionDuration == 0 {
		return
	}
	horizon := time.Now().Add(-e.opts.RetentionDuration).UnixNano()

	type candidate struct {
		win            partition.PartitionWindow
		expiredReaders []*sstable.Reader
		expiredMetas   []sstable.SSTableMeta
	}

	e.mu.RLock()
	var candidates []candidate
	for win, readers := range e.files {
		metas := make([]sstable.SSTableMeta, len(readers))
		for i, r := range readers {
			metas[i] = r.Meta()
		}
		expired := e.strategy.PickExpired(metas, horizon)
		if len(expired) == 0 {
			continue
		}
		expiredPaths := make(map[string]struct{}, len(expired))
		for _, m := range expired {
			expiredPaths[m.Path] = struct{}{}
		}
		var expReaders []*sstable.Reader
		for _, r := range readers {
			if _, ok := expiredPaths[r.Meta().Path]; ok {
				expReaders = append(expReaders, r)
			}
		}
		candidates = append(candidates, candidate{win: win, expiredReaders: expReaders, expiredMetas: expired})
	}
	e.mu.RUnlock()

	for _, c := range candidates {
		e.retireWindow(c.win, c.expiredReaders, c.expiredMetas)
	}
}

func (e *Engine) retireWindow(win partition.PartitionWindow, expiredReaders []*sstable.Reader, expiredMetas []sstable.SSTableMeta) {
	deletedFiles := make([]manifest.FileEntry, len(expiredMetas))
	for i, m := range expiredMetas {
		deletedFiles[i] = manifest.FileEntry{
			Path:           m.Path,
			PartitionStart: m.PartitionStart,
			PartitionEnd:   m.PartitionEnd,
			MinTimestamp:   m.MinTimestamp,
			MaxTimestamp:   m.MaxTimestamp,
			RecordCount:    m.RecordCount,
		}
	}
	if err := e.manifest.Append(manifest.VersionEdit{DeletedFiles: deletedFiles}); err != nil {
		slog.Error("engine: retention manifest update failed", "window", win.Start, "err", err)
		return
	}

	for _, m := range expiredMetas {
		if err := os.Remove(m.Path); err != nil && !os.IsNotExist(err) {
			slog.Error("engine: retention file delete failed", "path", m.Path, "err", err)
		}
	}

	expiredPaths := make(map[string]struct{}, len(expiredReaders))
	for _, r := range expiredReaders {
		expiredPaths[r.Meta().Path] = struct{}{}
	}

	var windowEmpty bool
	e.mu.Lock()
	current := e.files[win]
	survived := make([]*sstable.Reader, 0, len(current))
	var toClose []*sstable.Reader
	for _, r := range current {
		if _, wasExpired := expiredPaths[r.Meta().Path]; wasExpired {
			toClose = append(toClose, r)
		} else {
			survived = append(survived, r)
		}
	}
	if len(survived) == 0 {
		delete(e.files, win)
		delete(e.maxFlushedTS, win)
		windowEmpty = true
	} else {
		e.files[win] = survived
	}
	e.closedReaders = append(e.closedReaders, toClose...)
	e.mu.Unlock()

	if windowEmpty {
		for _, p := range e.router.All() {
			if p.Window == win && p.State() == partition.StateSealed {
				p.MarkDeleted()
				break
			}
		}
	}
}
