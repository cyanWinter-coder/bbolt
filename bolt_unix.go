//go:build !windows && !plan9 && !solaris && !aix && !android

package bbolt

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, exclusive bool, timeout time.Duration) error {
	var t time.Time
	if timeout != 0 {
		t = time.Now()
	}
	fd := db.file.Fd()
	flag := syscall.LOCK_NB
	if exclusive {
		flag |= syscall.LOCK_EX
	} else {
		flag |= syscall.LOCK_SH
	}
	for {
		// Attempt to obtain an exclusive lock.
		err := syscall.Flock(int(fd), flag)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// If we timed out then return an error.
		if timeout != 0 && time.Since(t) > timeout-flockRetryTimeout {
			return errors.ErrTimeout
		}

		// Wait for a bit and try again.
		time.Sleep(flockRetryTimeout)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a DB's data file and returns a new mapping generation.
// The currently installed generation is not modified, so callers can keep it
// as a fallback if preparing or installing the new generation fails.
func mmap(db *DB, sz int) (*mapping, error) {
	// Map the data file to memory.
	b, err := unix.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		// Ignore not implemented error in kernel because it still works.
		_ = unix.Munmap(b)
		return nil, fmt.Errorf("madvise: %s", err)
	}

	// Save the original byte slice and convert to a byte array pointer.
	return &mapping{
		dataref: b,
		data:    (*[common.MaxMapSize]byte)(unsafe.Pointer(&b[0])),
		datasz:  sz,
	}, nil
}

// munmap unmaps this mapping generation from memory.
func (m *mapping) munmap() error {
	// Ignore the unmap if we have no mapped data.
	if m == nil || m.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := unix.Munmap(m.dataref)
	m.dataref = nil
	m.data = nil
	m.datasz = 0
	return err
}
