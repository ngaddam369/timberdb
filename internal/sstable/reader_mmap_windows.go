//go:build windows

package sstable

func (r *Reader) initMmap() error  { return nil }
func (r *Reader) closeMmap() error { return nil }
