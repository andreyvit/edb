package mmap

import "os"

// Fdatasync triggers the fastest fsync-like operation that ensures durability
// of the data written to the given file and/or memory mapping.
//
// Fdatasync might be faster than f.Sync() aka fsync thanks to not syncing
// metadata (last modification/access time) that isn't necessary to ensure
// durability of the data.
//
// If mapping is provided, it's an mmap'ed slice corresponding to the given
// file, in case the operating system supports an alternative interface for
// syncing mmap'ed data.
//
// WARNING: ERRORS RETURNED BY THIS FUNCTION ARE NOT RECOVERABLE. Many operating
// systems and file systems mark modified pages as clean in case of fsync
// failures, and there is no way to ensure data correctness after a failure.
// Moreover, the data in disk caches might not correspond to the data on disk,
// so whether the data appears corrupt upon reading back might not reflect
// whether the corruption actually exists on disk. The only sensible handling
// of fsync errors is to mark the database as corrupted and require manual
// inspection and recovery (including a reboot).
func Fdatasync(f *os.File, mapping []byte) error {
	return fdatasync(f, mapping)
}
