package bbolt

// mlock locks memory of db file
func mlock(_ *dbMapping, _ int) error {
	panic("mlock is supported only on UNIX systems")
}

// munlock unlocks memory of db file
func munlock(_ *dbMapping, _ int) error {
	panic("munlock is supported only on UNIX systems")
}
