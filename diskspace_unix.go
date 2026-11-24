//go:build unix

package bbolt

import (
	"syscall"
)

// platformAvailableBytes returns the number of bytes available to an
// unprivileged user at path (which may be a file or a directory).
func platformAvailableBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

// errDiskFullPlatform is the platform "no space left" error, so callers can
// match it with errors.Is in addition to ErrInsufficientDiskSpace.
var errDiskFullPlatform error = syscall.ENOSPC
