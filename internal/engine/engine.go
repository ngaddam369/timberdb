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
	closeCh      chan struct{}
	wg           sync.WaitGroup
	walSeq       int
	closed       bool
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
		closeCh:      make(chan struct{}),
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
	e.walSeq++
	newWALPath := e.walPath(e.walSeq)
	e.mu.Unlock()

	if err := e.wal.Rotate(newWALPath); err != nil {
		return err
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

	return nil
}

func (e *Engine) walPath(seq int) string {
	return filepath.Join(e.dir, fmt.Sprintf("wal-%06d.wal", seq))
}
