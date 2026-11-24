//go:build !unix && !windows

package bbolt

import "errors"

func platformAvailableBytes(path string) (uint64, error) {
	return 0, errDiskSpaceUnsupported
}

var errDiskFullPlatform error = errors.New("no space left on device")
