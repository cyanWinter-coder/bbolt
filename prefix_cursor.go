package bbolt

import (
	"bytes"

	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// Traversal direction for PrefixCursor.
const (
	// Forward traverses matching keys in lexicographical order, from the
	// smallest key to the largest. PrefixCursor.Next walks in this direction.
	Forward = 0

	// Reverse traverses matching keys in reverse lexicographical order, from
	// the largest key to the smallest. PrefixCursor.Next walks in this
	// direction.
	Reverse = 1
)

// PrefixCursor iterates over the key/value pairs of a bucket whose keys share
// a common prefix, with stable traversal semantics that remain correct while
// the current transaction deletes the visited keys.
//
// A plain Cursor stores its position as a (page, index) stack. When the
// current key is deleted, node rebalancing and page splits can rearrange that
// stack, so continuing with Next/Prev may skip unvisited keys or revisit
// deleted ones. PrefixCursor instead remembers the last key it returned,
// expressed in the global lexicographical order of the bucket, and re-searches
// the tree relative to that key after every mutation. Deleting the current
// item therefore always continues from the item at the next original ordered
// position, regardless of how nodes are rearranged.
//
// A PrefixCursor is only valid as long as the transaction it was created from
// is open. When the transaction is closed or the iterated bucket is deleted,
// every method returns errors.ErrCursorTerminal and the cursor releases its
// underlying page references; subsequent calls keep returning the same error.
// To resume the work, create a new PrefixCursor in a new transaction and
// Seek() to the first unprocessed key.
//
// Keys returned from a PrefixCursor are copied and remain safe to retain after
// the cursor advances or deletes the key. Values follow the regular Cursor
// rules and are only valid for the life of the transaction.
type PrefixCursor struct {
	bucket       *Bucket
	cursor       *Cursor
	prefix       []byte
	direction    int
	allowDelete  bool
	lastKey      []byte // last key returned to the caller; nil before the first positioning call
	lastKeyValid bool
	// deleted is true between Delete() of lastKey and the next positioning
	// call. It forces traversal to re-seek from lastKey instead of trusting
	// the underlying cursor stack, which the deletion may have rearranged.
	deleted bool
	// exhausted is true once Next/Prev walked past the last matching key. It
	// is sticky: further Next/Prev calls keep returning nil until an explicit
	// First/Last/Seek repositions the cursor.
	exhausted bool
	// terminal records why the cursor became terminal. It is nil while the
	// cursor is usable.
	terminal error
	closed   bool
}

// PrefixCursor creates a stable, resumable iterator over the keys of b that
// start with prefix.
//
// direction is Forward or Reverse and determines the order in which Next
// walks the matching keys. allowDelete must be true to delete keys through
// the cursor; it can only be enabled on a writable transaction. Deletion
// during traversal is safe: after deleting the current key, Next continues
// from the next key in the original ordered position, never skipping
// unvisited keys.
//
// The returned cursor is valid as long as the transaction remains open and
// the bucket is not deleted; after that its methods return
// errors.ErrCursorTerminal.
func (b *Bucket) PrefixCursor(prefix []byte, direction int, allowDelete bool) (*PrefixCursor, error) {
	if b.tx.db == nil {
		return nil, errors.ErrTxClosed
	}
	if direction != Forward && direction != Reverse {
		return nil, errors.ErrInvalidDirection
	}
	if allowDelete && !b.Writable() {
		return nil, errors.ErrPrefixDeleteNotAllowed
	}

	// b.Cursor() accounts for the cursor in the transaction statistics.
	return &PrefixCursor{
		bucket:      b,
		cursor:      b.Cursor(),
		prefix:      cloneBytes(prefix),
		direction:   direction,
		allowDelete: allowDelete,
	}, nil
}

// Prefix returns the prefix the cursor was created with. The returned slice
// is a copy and may be retained by the caller.
func (pc *PrefixCursor) Prefix() []byte {
	return cloneBytes(pc.prefix)
}

// Bucket returns the bucket the cursor iterates over.
func (pc *PrefixCursor) Bucket() *Bucket {
	return pc.bucket
}

// First positions the cursor on the first key matching the prefix and returns
// it. If no key matches the prefix, a nil key is returned.
func (pc *PrefixCursor) First() (key []byte, value []byte, err error) {
	if err = pc.checkUsable(); err != nil {
		return nil, nil, err
	}
	k, v, flags := pc.cursor.seek(pc.prefix)
	if (flags & uint32(common.BucketLeafFlag)) != 0 {
		v = nil
	}
	if !pc.matchPrefix(k) {
		return pc.nilPosition()
	}
	return pc.adopt(k, v)
}

// Last positions the cursor on the last key matching the prefix and returns
// it. If no key matches the prefix, a nil key is returned.
func (pc *PrefixCursor) Last() (key []byte, value []byte, err error) {
	if err = pc.checkUsable(); err != nil {
		return nil, nil, err
	}
	k, v, flags, ok := pc.lastInPrefix()
	if !ok {
		return pc.nilPosition()
	}
	if (flags & uint32(common.BucketLeafFlag)) != 0 {
		v = nil
	}
	return pc.adopt(k, v)
}

// Seek positions the cursor on the first matching key at or after the given
// key in the traversal direction.
//
// In Forward direction it returns the matching key >= seek (the usual Cursor
// seek semantics), or nil when no such matching key exists. In Reverse
// direction it returns the matching key <= seek, or nil when no such matching
// key exists. The seek key need not itself carry the prefix.
func (pc *PrefixCursor) Seek(seek []byte) (key []byte, value []byte, err error) {
	if err = pc.checkUsable(); err != nil {
		return nil, nil, err
	}

	if pc.direction == Forward {
		k, v, flags := pc.cursor.seek(seek)
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		// A seek below the prefix range lands on an unrelated key; snap to
		// the first prefixed key.
		if k != nil && bytes.Compare(k, pc.prefix) < 0 {
			k, v, flags = pc.cursor.seek(pc.prefix)
			if (flags & uint32(common.BucketLeafFlag)) != 0 {
				v = nil
			}
		}
		if !pc.matchPrefix(k) {
			return pc.nilPosition()
		}
		return pc.adopt(k, v)
	}

	// Reverse: find the greatest matching key <= seek.
	if limit := prefixLimit(pc.prefix); limit != nil && bytes.Compare(seek, limit) >= 0 {
		// Seeking beyond the prefix range is equivalent to Last.
		k, v, flags, ok := pc.lastInPrefix()
		if !ok {
			return pc.nilPosition()
		}
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		return pc.adopt(k, v)
	}

	k, v, flags := pc.cursor.seek(seek)
	if (flags & uint32(common.BucketLeafFlag)) != 0 {
		v = nil
	}
	// seek lands on the first key >= seek. Back up one element when it is
	// strictly greater than seek, then keep walking backwards while the key
	// is outside the prefix.
	if k == nil || bytes.Compare(k, seek) > 0 {
		k, v, flags = pc.cursor.prev()
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
	}
	for k != nil && !pc.matchPrefix(k) {
		k, v, flags = pc.cursor.prev()
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
	}
	if k == nil {
		return pc.nilPosition()
	}
	return pc.adopt(k, v)
}

// Next advances the cursor to the next key matching the prefix, in the
// direction configured at creation, and returns it. It returns a nil key when
// every matching key has been visited.
//
// If the current key was deleted through Delete, Next re-searches the tree
// from the deleted key's original ordered position: node rebalancing or page
// splits cannot make it skip unvisited keys or revisit deleted ones.
func (pc *PrefixCursor) Next() (key []byte, value []byte, err error) {
	if err = pc.checkUsable(); err != nil {
		return nil, nil, err
	}
	// Sticky end-of-range: keep returning nil until an explicit
	// First/Last/Seek repositions the cursor.
	if pc.exhausted {
		return nil, nil, nil
	}
	if !pc.lastKeyValid {
		if pc.direction == Reverse {
			return pc.Last()
		}
		return pc.First()
	}
	if pc.direction == Reverse {
		return pc.nextReverse()
	}
	return pc.nextForward()
}

// Prev moves to the matching key on the opposite side of the current one.
// In Forward direction it returns the previous matching key, in Reverse
// direction the next matching key. It returns a nil key at the boundary of
// the prefix range.
func (pc *PrefixCursor) Prev() (key []byte, value []byte, err error) {
	if err = pc.checkUsable(); err != nil {
		return nil, nil, err
	}
	// Sticky end-of-range, mirroring Next.
	if pc.exhausted {
		return nil, nil, nil
	}
	if !pc.lastKeyValid {
		// Match the plain Cursor behavior of Prev/Next before positioning:
		// start from the boundary the traversal begins at.
		if pc.direction == Reverse {
			return pc.First()
		}
		return pc.Last()
	}

	// Temporarily flip the logical direction and reuse the same
	// delete-safe advance machinery.
	prevDirection := pc.direction
	if prevDirection == Reverse {
		pc.direction = Forward
	} else {
		pc.direction = Reverse
	}
	if pc.direction == Reverse {
		key, value, err = pc.nextReverse()
	} else {
		key, value, err = pc.nextForward()
	}
	pc.direction = prevDirection
	return key, value, err
}

// nextForward advances one matching key past lastKey in lexicographical order.
func (pc *PrefixCursor) nextForward() (key []byte, value []byte, err error) {
	if pc.deleted {
		// The underlying stack may have been rearranged by the deletion, so
		// re-search from the deleted key. Seek returns the first key >=
		// lastKey, which is the successor because lastKey no longer exists.
		k, v, flags := pc.cursor.seek(pc.lastKey)
		pc.deleted = false
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		if pc.matchPrefix(k) && !bytes.Equal(k, pc.lastKey) {
			return pc.adopt(k, v)
		}
		// Fall through to a regular advance for the (rare) case where an
		// equal key was re-inserted or the seek landed outside the range.
	}

	for {
		k, v, flags := pc.cursor.next()
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		if k == nil || !pc.matchPrefix(k) {
			return pc.nilPosition()
		}
		// Guard against revisiting the last returned key after a re-seek.
		if bytes.Equal(k, pc.lastKey) {
			continue
		}
		return pc.adopt(k, v)
	}
}

// nextReverse advances one matching key before lastKey in lexicographical order.
func (pc *PrefixCursor) nextReverse() (key []byte, value []byte, err error) {
	if pc.deleted {
		// Re-search from the deleted key. The seek lands on the successor;
		// one prev() step then reaches the original predecessor.
		k, v, flags := pc.cursor.seek(pc.lastKey)
		pc.deleted = false
		if k != nil && bytes.Equal(k, pc.lastKey) {
			// A key equal to the deleted one was re-inserted.
			if (flags & uint32(common.BucketLeafFlag)) != 0 {
				v = nil
			}
			return pc.adopt(k, v)
		}
		k, v, flags = pc.cursor.prev()
		if (flags & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		if pc.matchPrefix(k) {
			return pc.adopt(k, v)
		}
		return pc.nilPosition()
	}

	k, v, flags := pc.cursor.prev()
	if (flags & uint32(common.BucketLeafFlag)) != 0 {
		v = nil
	}
	if k == nil || !pc.matchPrefix(k) {
		return pc.nilPosition()
	}
	return pc.adopt(k, v)
}

// Delete removes the current key/value from the bucket. After Delete, the
// next Next call continues from the next key in the original ordered
// position.
//
// A second Delete before repositioning has no effect on the data and returns
// ErrCursorNotPositioned, so repeatedly running a prefix cleanup yields the
// same final result even if a key was already removed. Nested buckets cannot
// be deleted this way; use Bucket.DeleteBucket instead.
func (pc *PrefixCursor) Delete() error {
	if err := pc.checkUsable(); err != nil {
		return err
	}
	if !pc.allowDelete {
		return errors.ErrPrefixDeleteNotAllowed
	}
	if !pc.lastKeyValid || pc.deleted {
		// Nothing positioned, or the current item was already removed.
		return errors.ErrCursorNotPositioned
	}

	key, _, flags := pc.cursor.keyValue()
	// Only delete when the underlying cursor still points at the key we
	// handed out. Any drift means another mutation moved it and the caller
	// must reposition explicitly.
	if key == nil || !bytes.Equal(key, pc.lastKey) {
		return errors.ErrCursorNotPositioned
	}
	if (flags & common.BucketLeafFlag) != 0 {
		return errors.ErrIncompatibleValue
	}
	if err := pc.cursor.Delete(); err != nil {
		return err
	}

	pc.deleted = true
	return nil
}

// Close releases the cursor's references to bucket pages. After Close every
// method returns errors.ErrCursorTerminal. Calling Close more than once is a
// no-op. Closing is optional; a cursor is also detached automatically when
// its transaction closes.
func (pc *PrefixCursor) Close() {
	if pc.closed {
		return
	}
	pc.terminal = errors.ErrCursorTerminal
	pc.closed = true
	pc.cursor.stack = pc.cursor.stack[:0]
	pc.lastKey = nil
	pc.lastKeyValid = false
	pc.deleted = false
	pc.exhausted = false
}

// checkUsable returns the sticky terminal error once the transaction or
// bucket is gone, and detaches the cursor from its stale page references on
// the first failure so no later call can dereference them.
func (pc *PrefixCursor) checkUsable() error {
	if pc.terminal != nil {
		return pc.terminal
	}
	switch {
	case pc.bucket.tx.db == nil:
		pc.markTerminal()
		return errors.ErrCursorTerminal
	case pc.bucket.deleted:
		pc.markTerminal()
		return errors.ErrCursorTerminal
	default:
		return nil
	}
}

// markTerminal puts the cursor into the sticky terminal state and drops all
// references to pages that may have been returned to the freelist.
func (pc *PrefixCursor) markTerminal() {
	pc.terminal = errors.ErrCursorTerminal
	pc.cursor.stack = pc.cursor.stack[:0]
	pc.lastKey = nil
	pc.lastKeyValid = false
	pc.deleted = false
	pc.exhausted = false
}

// adopt stores the returned key as the new ordered position. The key is
// copied because the underlying memory can be invalidated by the next
// traversal step or by deleting the key.
func (pc *PrefixCursor) adopt(k, v []byte) ([]byte, []byte, error) {
	pc.lastKey = cloneBytes(k)
	pc.lastKeyValid = true
	pc.deleted = false
	pc.exhausted = false
	return pc.lastKey, v, nil
}

// nilPosition records that no matching key was found. The cursor becomes
// exhausted: Next/Prev keep returning nil until an explicit First/Last/Seek
// re-arms it.
func (pc *PrefixCursor) nilPosition() ([]byte, []byte, error) {
	pc.lastKey = nil
	pc.lastKeyValid = false
	pc.deleted = false
	pc.exhausted = true
	return nil, nil, nil
}

// matchPrefix reports whether k carries the cursor's prefix. A nil key never
// matches, including the nil "end of bucket" marker returned by Cursor.
func (pc *PrefixCursor) matchPrefix(k []byte) bool {
	if k == nil {
		return false
	}
	return bytes.HasPrefix(k, pc.prefix)
}

// lastInPrefix positions the underlying cursor on the greatest key matching
// the prefix and returns it. The boolean is false when no key matches.
func (pc *PrefixCursor) lastInPrefix() (key []byte, value []byte, flags uint32, ok bool) {
	// Seek to the smallest key strictly greater than every possible prefixed
	// key. When no such key exists (all-0xff prefix), seek with the prefix
	// itself and accept the landing key if it already matches.
	if limit := prefixLimit(pc.prefix); limit != nil {
		k, v, f := pc.cursor.seek(limit)
		if (f & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		if k != nil && pc.matchPrefix(k) {
			// Should be unreachable for a strict successor limit, but keeping
			// the check makes the boundary handling explicit and safe.
			return k, v, f, true
		}
		k, v, f = pc.cursor.prev()
		if (f & uint32(common.BucketLeafFlag)) != 0 {
			v = nil
		}
		if pc.matchPrefix(k) {
			return k, v, f, true
		}
		return nil, nil, 0, false
	}

	// All-0xff prefix: every matching key is >= prefix, so a plain seek lands
	// on the first matching key; walk forward to the last one.
	k, v, f := pc.cursor.seek(pc.prefix)
	if (f & uint32(common.BucketLeafFlag)) != 0 {
		v = nil
	}
	for k != nil && pc.matchPrefix(k) {
		nextKey, nextVal, nextFlags := pc.cursor.next()
		if (nextFlags & uint32(common.BucketLeafFlag)) != 0 {
			nextVal = nil
		}
		if nextKey == nil || !pc.matchPrefix(nextKey) {
			return k, v, f, true
		}
		k, v, f = nextKey, nextVal, nextFlags
	}
	return nil, nil, 0, false
}

// prefixLimit returns the smallest key that is strictly greater than every
// key starting with prefix. It returns nil when no such key exists because
// the prefix is already all 0xff bytes.
func prefixLimit(prefix []byte) []byte {
	limit := make([]byte, len(prefix))
	copy(limit, prefix)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] != 0xff {
			limit[i]++
			return limit[:i+1]
		}
	}
	return nil
}
