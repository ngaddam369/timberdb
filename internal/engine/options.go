package engine

import (
	"time"

	"github.com/ngaddam369/timberdb/internal/partition"
	"github.com/ngaddam369/timberdb/internal/wal"
)

// Options configures an Engine at construction time.
type Options struct {
	// PartitionDuration is the time-window size for each partition (default: 1h).
	PartitionDuration time.Duration
	// LateArrivalWindow is how far behind wall clock a record may be and still
	// land in its natural partition rather than the late-arrival partition (default: 5min).
	LateArrivalWindow time.Duration
	// LateArrivalMode controls whether late records are rejected (Strict) or
	// silently routed to a dedicated late partition (Tolerant) (default: Strict).
	LateArrivalMode partition.LateArrivalMode
	// MemtableSizeBytes is the approximate memtable size that triggers a background
	// flush to SSTable (default: 64 MiB).
	MemtableSizeBytes int64
	// WALSyncMode controls fsync durability guarantees (default: SyncAlways).
	WALSyncMode wal.WALSyncMode
	// BlockSizeBytes is the target uncompressed size for SSTable data blocks (default: 32 KiB).
	BlockSizeBytes int
	// IndexSources enables building a per-source index inside each SSTable,
	// which accelerates source-filtered scans at the cost of extra write I/O (default: false).
	IndexSources bool
	// MaxFilesPerPartition is the number of SSTable files in a partition that
	// triggers compaction (default: 10).
	MaxFilesPerPartition int
	// RetentionDuration is the maximum age of data before it is eligible for
	// deletion. Zero means no expiry (default: 0).
	RetentionDuration time.Duration
	// RetentionCheckInterval controls how often the TTL enforcer runs (default: 1h).
	RetentionCheckInterval time.Duration
	// MetricsAddr is the address on which the Prometheus /metrics endpoint is
	// served. Empty string disables the endpoint (default: ":9090").
	MetricsAddr string
}

// DefaultOptions returns a production-ready Options with conservative defaults.
func DefaultOptions() Options {
	return Options{
		PartitionDuration:      time.Hour,
		LateArrivalWindow:      5 * time.Minute,
		LateArrivalMode:        partition.Strict,
		MemtableSizeBytes:      64 << 20, // 64 MiB
		WALSyncMode:            wal.SyncAlways,
		BlockSizeBytes:         32 << 10, // 32 KiB
		IndexSources:           false,
		MaxFilesPerPartition:   10,
		RetentionDuration:      0,
		RetentionCheckInterval: time.Hour,
		MetricsAddr:            ":9090",
	}
}
