// Package engine is the public API for timberdb.
// It re-exports the types and functions from the internal packages so that
// external consumers can import this package without depending on internal paths.
package engine

import (
	internalengine "github.com/ngaddam369/timberdb/internal/engine"
	"github.com/ngaddam369/timberdb/internal/record"
	"github.com/ngaddam369/timberdb/internal/sstable"
	"github.com/ngaddam369/timberdb/internal/wal"
)

// Engine is the durable append+scan store. All exported methods are safe for
// concurrent use. Use Open to obtain an instance.
type Engine = internalengine.Engine

// Options configures an Engine at construction time.
type Options = internalengine.Options

// ScanOptions configures a call to Scan.
type ScanOptions = internalengine.ScanOptions

// AggregateOpts configures a call to Aggregate.
type AggregateOpts = internalengine.AggregateOpts

// AggFn selects the aggregation function applied per bucket by Aggregate.
type AggFn = internalengine.AggFn

// Bucket holds the aggregated result for one time window.
type Bucket = internalengine.Bucket

// PartitionInfo describes the current state of a single time-partition.
type PartitionInfo = internalengine.PartitionInfo

// Record is a single timestamped event with an optional source identifier and
// an arbitrary payload.
type Record = record.Record

// Iterator is a forward-only cursor over a sequence of records.
type Iterator = record.Iterator

// RecordView is a zero-copy view of a record returned by Iterator.View().
type RecordView = record.RecordView

// WALSyncMode controls when the WAL is fsynced to disk.
type WALSyncMode = wal.WALSyncMode

// CompressionType identifies the compression algorithm applied to SSTable data blocks.
type CompressionType = sstable.CompressionType

// WAL sync modes.
const (
	SyncAlways   = wal.SyncAlways
	SyncPeriodic = wal.SyncPeriodic
	SyncNever    = wal.SyncNever
)

// Compression algorithms.
const (
	CompressionNone   = sstable.CompressionNone
	CompressionZstd   = sstable.CompressionZstd
	CompressionSnappy = sstable.CompressionSnappy
)

// Aggregation functions.
const (
	AggCount = internalengine.AggCount
	AggRate  = internalengine.AggRate
)

// Sentinel errors returned by Engine methods.
var (
	ErrClosed             = internalengine.ErrClosed
	ErrInvalidBucketWidth = internalengine.ErrInvalidBucketWidth
)

// Open opens or creates a timberdb store at dir with the given options.
var Open = internalengine.Open

// DefaultOptions returns a production-ready Options with conservative defaults.
var DefaultOptions = internalengine.DefaultOptions
