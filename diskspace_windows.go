//go:build windows

package bbolt

import (
	"golang.org/x/sys/windows"
)

// platformAvailableBytes returns the number of bytes available to the caller
// at path (which may be a file or a directory).
func platformAvailableBytes(path string) (uint64, error) {
	var available, total, totalFree uint64
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	if err := windows.GetDiskFreeSpaceEx(pathPtr, &available, &total, &totalFree); err != nil {
		return 0, err
	}
	return available, nil
}

// errDiskFullPlatform is the platform "disk full" error.
var errDiskFullPlatform error = windows.ERROR_DISK_FULL
