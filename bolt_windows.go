package bbolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return db.file.Sync()
}

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, exclusive bool, timeout time.Duration) error {
	var t time.Time
	if timeout != 0 {
		t = time.Now()
	}
	var flags uint32 = windows.LOCKFILE_FAIL_IMMEDIATELY
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	for {
		// Fix for https://github.com/etcd-io/bbolt/issues/121. Use byte-range
		// -1..0 as the lock on the database file.
		var m1 uint32 = (1 << 32) - 1 // -1 in a uint32
		err := windows.LockFileEx(windows.Handle(db.file.Fd()), flags, 0, 1, 0, &windows.Overlapped{
			Offset:     m1,
			OffsetHigh: m1,
		})

		if err == nil {
			return nil
		} else if err != windows.ERROR_LOCK_VIOLATION {
			return err
		}

		// If we timed oumercit then return an error.
		if timeout != 0 && time.Since(t) > timeout-flockRetryTimeout {
			return errors.ErrTimeout
		}

		// Wait for a bit and try again.
		time.Sleep(flockRetryTimeout)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	var m1 uint32 = (1 << 32) - 1 // -1 in a uint32
	return windows.UnlockFileEx(windows.Handle(db.file.Fd()), 0, 1, 0, &windows.Overlapped{
		Offset:     m1,
		OffsetHigh: m1,
	})
}

// mmap memory maps a DB's data file and returns the new mapping generation.
// The generation currently installed on the DB is left untouched, so a
// failure doesn't affect the existing mapping.
// Based on: https://github.com/edsrzf/mmap-go
func mmap(db *DB, sz int) (*dbMapping, error) {
	var sizelo, sizehi uint32

	if !db.readOnly {
		// Truncate the database to the size of the mmap.
		if err := db.file.Truncate(int64(sz)); err != nil {
			return nil, fmt.Errorf("truncate: %s", err)
		}
		sizehi = uint32(sz >> 32)
		sizelo = uint32(sz)
	}

	// Open a file mapping handle.
	h, errno := syscall.CreateFileMapping(syscall.Handle(db.file.Fd()), nil, syscall.PAGE_READONLY, sizehi, sizelo, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, 0)
	if addr == 0 {
		// Do our best and report error returned from MapViewOfFile.
		_ = syscall.CloseHandle(h)
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		// Unmap the fresh view to avoid leaking it.
		_ = syscall.UnmapViewOfFile(addr)
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	// Convert to a byte array.
	return &dbMapping{
		data:   (*[common.MaxMapSize]byte)(unsafe.Pointer(addr)),
		datasz: sz,
	}, nil
}

// munmap unmaps a mapping generation from memory.
// Based on: https://github.com/edsrzf/mmap-go
func (m *dbMapping) munmap() error {
	if m == nil || m.data == nil {
		return nil
	}

	addr := (uintptr)(unsafe.Pointer(&m.data[0]))
	var err1 error
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		err1 = os.NewSyscallError("UnmapViewOfFile", err)
	}
	m.data = nil
	m.datasz = 0
	return err1
}
