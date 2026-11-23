//go:build !windows

package bbolt

import "golang.org/x/sys/unix"

// mlock locks memory of db file
func mlock(m *dbMapping, fileSize int) error {
	sizeToLock := fileSize
	if sizeToLock > m.datasz {
		// Can't lock more than mmaped slice
		sizeToLock = m.datasz
	}
	if err := unix.Mlock(m.dataref[:sizeToLock]); err != nil {
		return err
	}
	return nil
}

// munlock unlocks memory of db file
func munlock(m *dbMapping, fileSize int) error {
	if m == nil || m.dataref == nil {
		return nil
	}

	sizeToUnlock := fileSize
	if sizeToUnlock > m.datasz {
		// Can't unlock more than mmaped slice
		sizeToUnlock = m.datasz
	}

	if err := unix.Munlock(m.dataref[:sizeToUnlock]); err != nil {
		return err
	}
	return nil
}
