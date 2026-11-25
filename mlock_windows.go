package bbolt

// mlock locks memory of db file
func mlock(_ *mapping, _ int) error {
	panic("mlock is supported only on UNIX systems")
}

// munlock unlocks memory of db file
func munlock(_ *mapping, _ int) error {
	panic("munlock is supported only on UNIX systems")
}
