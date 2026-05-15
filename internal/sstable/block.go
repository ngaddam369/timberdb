// Package sstable implements immutable on-disk SSTable files.
// Each SSTable covers a sealed time partition segment and is never modified after creation.
//
// File layout: Data Blocks | Time Index Block | Source Index Block (optional) | Footer (64B)
// Footer stores min_timestamp and max_timestamp — replaces bloom filters for time-range skipping.
package sstable
