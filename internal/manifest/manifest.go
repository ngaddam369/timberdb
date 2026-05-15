// Package manifest maintains a durable, append-only log of VersionEdit records.
// It tracks which SSTable files exist, their partition and time-range metadata,
// and reconstructs full engine state on startup.
package manifest
