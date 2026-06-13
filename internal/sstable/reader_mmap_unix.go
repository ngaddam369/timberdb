//go:build !windows

package sstable

import "golang.org/x/sys/unix"

func (r *Reader) initMmap() error {
	if r.meta.TimeIndexOffset == 0 {
		return nil // no data blocks
	}
	data, err := unix.Mmap(
		int(r.f.Fd()), 0, int(r.meta.TimeIndexOffset),
		unix.PROT_READ, unix.MAP_SHARED,
	)
	if err != nil {
		return err
	}
	r.mmap = data
	return nil
}

func (r *Reader) closeMmap() error {
	if r.mmap == nil {
		return nil
	}
	return unix.Munmap(r.mmap)
}
