package bbolt

import (
	"bytes"
	"fmt"
	"unsafe"

	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// MoveBucketSpec describes a single sub-bucket move within a batch submitted
// to Tx.MoveBuckets or Bucket.MoveBuckets.
//
// A move relocates the sub-bucket stored under Key in the source bucket to
// Key in the destination bucket. The sub-bucket itself, including its nested
// buckets, sequence and data, is never copied: only its parent linkage
// changes.
type MoveBucketSpec struct {
	// Key is the name of the sub-bucket to move. It must identify an
	// existing sub-bucket in Src. The same key is used in Dst.
	Key []byte

	// Src is the bucket that currently contains Key. A nil Src denotes the
	// root bucket, i.e. a top level bucket is moved.
	Src *Bucket

	// Dst is the bucket that receives Key. A nil Dst denotes the root
	// bucket, i.e. the moved bucket becomes a top level bucket.
	Dst *Bucket
}

// BucketMoveError identifies the failing entry of a batch move. The Index
// field is the position of the offending MoveBucketSpec in the submitted
// slice, Key is its key, and Err wraps the concrete cause, which is one of
// the sentinel errors defined in the errors package (for example
// errors.ErrBucketNotFound, errors.ErrBucketExists,
// errors.ErrIncompatibleValue, errors.ErrSameBuckets, errors.ErrDifferentDB,
// errors.ErrCyclicBucketMove, errors.ErrTxNotWritable or
// errors.ErrTxClosed).
//
// Use errors.Is to inspect the wrapped cause.
type BucketMoveError struct {
	Index int
	Key   []byte
	Err   error
}

// Error implements the error interface.
func (e *BucketMoveError) Error() string {
	return fmt.Sprintf("move bucket %q at index %d: %v", e.Key, e.Index, e.Err)
}

// Unwrap returns the concrete cause so that errors.Is and errors.As can
// inspect it.
func (e *BucketMoveError) Unwrap() error {
	return e.Err
}

// bucketMove is the fully resolved, read-only description of a move that has
// passed every per-item and batch-wide precondition. No visible bucket
// structure is modified while resolving moves; all mutations happen in
// applyBucketMoves after the whole batch has been validated.
type bucketMove struct {
	index    int
	key      []byte
	src      *Bucket
	dst      *Bucket
	child    *Bucket  // the already opened sub-bucket being moved
	srcValue []byte   // cloned bucket header value as stored in the source
	addSlot  moveSlot // (dst, key) to which the sub-bucket is added
}

// moveSlot identifies a key position within a parent bucket.
type moveSlot struct {
	parent *Bucket
	key    string
}

// MoveBuckets moves multiple sub-buckets atomically within the current
// read/write transaction.
//
// Every spec is pre-validated before any visible bucket structure changes.
// If any spec is invalid, or the combined set of moves conflicts with itself
// (duplicate destination, key conflict or an ancestor/descendant cycle), no
// change is applied and a *BucketMoveError identifying the failing spec is
// returned. Only after every precondition succeeds are all moves applied in a
// single step within this transaction, reusing the existing bucket instances
// so that bucket pointers, sequences and sub-bucket caches keep their
// ownership.
//
// The operation is all-or-nothing: if the surrounding transaction is rolled
// back, fails to commit or is cancelled by the caller, every source, target
// and nested bucket remains in its original position.
//
// A nil src or dst denotes the root bucket, mirroring Tx.MoveBucket. An empty
// spec slice is a no-op and returns nil.
func (tx *Tx) MoveBuckets(specs []MoveBucketSpec) error {
	if tx == nil || tx.db == nil {
		return &BucketMoveError{Index: 0, Err: errors.ErrTxClosed}
	}
	if !tx.writable {
		return &BucketMoveError{Index: 0, Err: errors.ErrTxNotWritable}
	}
	if len(specs) == 0 {
		return nil
	}

	moves, err := resolveBucketMoves(tx, specs)
	if err != nil {
		return err
	}
	applyBucketMoves(moves)
	return nil
}

// MoveBuckets moves multiple sub-buckets atomically. It is the Bucket-level
// counterpart of Tx.MoveBuckets and applies the same atomicity and validation
// guarantees, delegating to the bucket's transaction. As with
// Tx.MoveBuckets, a nil Src or Dst in a spec denotes the root bucket.
func (b *Bucket) MoveBuckets(specs []MoveBucketSpec) error {
	if b == nil || b.tx == nil || b.tx.db == nil {
		return &BucketMoveError{Index: 0, Err: errors.ErrTxClosed}
	}
	if !b.tx.writable {
		return &BucketMoveError{Index: 0, Err: errors.ErrTxNotWritable}
	}
	if len(specs) == 0 {
		return nil
	}
	return b.tx.MoveBuckets(specs)
}

// resolveBucketMoves validates the whole batch and returns the fully resolved
// moves. It performs no mutation of the visible bucket structure.
func resolveBucketMoves(tx *Tx, specs []MoveBucketSpec) ([]*bucketMove, error) {
	lg := tx.db.Logger()

	// Pre-scan the declared source slots so that a destination slot released
	// by a sibling move is recognized regardless of spec ordering. The same
	// source slot can only be emptied once, so duplicates are rejected here
	// and attributed to the later spec.
	removedSlots := make(map[moveSlot]int)
	for i, spec := range specs {
		src := spec.Src
		if src == nil {
			src = tx.root
		}
		if src.tx == nil || src.tx.db == nil {
			return nil, &BucketMoveError{
				Index: i,
				Key:   cloneBytes(spec.Key),
				Err:   errors.ErrTxClosed,
			}
		}
		slot := moveSlot{parent: src, key: string(spec.Key)}
		if existing, ok := removedSlots[slot]; ok {
			if lg != discardLogger {
				lg.Errorf("Duplicate source slot for bucket move %q at index %d and index %d",
					spec.Key, i, existing)
			}
			return nil, &BucketMoveError{
				Index: i,
				Key:   cloneBytes(spec.Key),
				Err:   errors.ErrBucketNotFound,
			}
		}
		removedSlots[slot] = i
	}

	// Slots taken by an "add" within this batch. Two moves must not land on
	// the same destination slot.
	addedSlots := make(map[moveSlot]int)

	moves := make([]*bucketMove, 0, len(specs))
	for i, spec := range specs {
		move, err := resolveBucketMove(tx, i, spec, removedSlots)
		if err != nil {
			if lg != discardLogger {
				lg.Errorf("Pre-checking bucket move %q at index %d failed: %v", spec.Key, i, err)
			}
			return nil, err
		}

		if existing, ok := addedSlots[move.addSlot]; ok {
			if lg != discardLogger {
				lg.Errorf("Duplicate destination for bucket move %q at index %d and index %d",
					move.key, move.index, existing)
			}
			return nil, &BucketMoveError{
				Index: move.index,
				Key:   move.key,
				Err:   errors.ErrBucketExists,
			}
		}
		addedSlots[move.addSlot] = move.index

		moves = append(moves, move)
	}

	if err := validateBucketMovesNoCycle(tx, moves); err != nil {
		return nil, err
	}
	return moves, nil
}

// resolveBucketMove validates a single spec against the current, unmodified
// bucket structure and resolves it into a bucketMove. It mirrors the
// per-item checks of Bucket.MoveBucket without mutating anything. The
// removedSlots map describes every source slot that the batch will release,
// which makes those slots reusable as destinations.
func resolveBucketMove(tx *Tx, index int, spec MoveBucketSpec, removedSlots map[moveSlot]int) (*bucketMove, error) {
	src := spec.Src
	dst := spec.Dst
	if src == nil {
		src = tx.root
	}
	if dst == nil {
		dst = tx.root
	}

	newKey := cloneBytes(spec.Key)
	moveErr := func(err error) error {
		return &BucketMoveError{Index: index, Key: newKey, Err: err}
	}

	if src == nil || dst == nil || src.tx == nil || src.tx.db == nil ||
		dst.tx == nil || dst.tx.db == nil {
		return nil, moveErr(errors.ErrTxClosed)
	}
	if !src.Writable() || !dst.Writable() {
		return nil, moveErr(errors.ErrTxNotWritable)
	}
	if src.tx.db.Path() != dst.tx.db.Path() || src.tx != dst.tx {
		return nil, moveErr(errors.ErrDifferentDB)
	}

	// Locate the sub-bucket in the source bucket.
	c := src.Cursor()
	k, v, flags := c.seek(newKey)
	if !bytes.Equal(newKey, k) {
		return nil, moveErr(errors.ErrBucketNotFound)
	} else if (flags & common.BucketLeafFlag) == 0 {
		return nil, moveErr(errors.ErrIncompatibleValue)
	}

	// Source and destination must be distinct buckets.
	if src == dst || (src.RootPage() == dst.RootPage() && src.RootPage() != 0) {
		return nil, moveErr(errors.ErrSameBuckets)
	}

	child := src.Bucket(newKey)

	// Check the destination slot against the on-disk structure. A slot that
	// a sibling move empties is reusable and is handled via removedSlots.
	curDst := dst.Cursor()
	existingKey, _, dstFlags := curDst.seek(newKey)
	if bytes.Equal(newKey, existingKey) {
		if _, released := removedSlots[moveSlot{parent: dst, key: string(newKey)}]; !released {
			if (dstFlags & common.BucketLeafFlag) != 0 {
				return nil, moveErr(errors.ErrBucketExists)
			}
			return nil, moveErr(errors.ErrIncompatibleValue)
		}
	}

	return &bucketMove{
		index:    index,
		key:      newKey,
		src:      src,
		dst:      dst,
		child:    child,
		srcValue: cloneBytes(v),
		addSlot: moveSlot{
			parent: dst,
			key:    string(newKey),
		},
	}, nil
}

// validateBucketMovesNoCycle verifies that applying the batch cannot create a
// cycle in the bucket tree. It builds the parent relation of the current
// bucket tree, then applies the planned re-parentings one at a time. After
// each re-parenting it walks the moved bucket's ancestor chain, so the spec
// that actually closes a cycle (rather than an unrelated earlier spec) is the
// one reported.
func validateBucketMovesNoCycle(tx *Tx, moves []*bucketMove) error {
	// Current parent for every bucket, keyed by bucket pointer. Bucket
	// pointers are authoritative even for inline buckets, which share the
	// zero root page id.
	parentOf := make(map[*Bucket]*Bucket)
	buildBucketParents(tx.root, nil, parentOf)

	for _, move := range moves {
		parentOf[move.child] = move.dst

		seen := make(map[*Bucket]struct{})
		for ancestor := move.dst; ancestor != nil; ancestor = parentOf[ancestor] {
			if ancestor == move.child {
				return &BucketMoveError{
					Index: move.index,
					Key:   move.key,
					Err:   errors.ErrCyclicBucketMove,
				}
			}
			if _, ok := seen[ancestor]; ok {
				return &BucketMoveError{
					Index: move.index,
					Key:   move.key,
					Err:   errors.ErrCyclicBucketMove,
				}
			}
			seen[ancestor] = struct{}{}
		}
	}
	return nil
}

// buildBucketParents records parent as the parent of every sub-bucket of
// bucket and recurses through the whole subtree.
func buildBucketParents(bucket, parent *Bucket, parentOf map[*Bucket]*Bucket) {
	parentOf[bucket] = parent
	_ = bucket.ForEachBucket(func(k []byte) error {
		if child := bucket.Bucket(k); child != nil {
			buildBucketParents(child, bucket, parentOf)
		}
		return nil
	})
}

// applyBucketMoves performs all validated moves. It is only reached after
// every precondition has succeeded, so it cannot fail: the same primitive as
// Bucket.MoveBucket is reused for each move.
func applyBucketMoves(moves []*bucketMove) {
	for _, move := range moves {
		newKey := move.key

		// Remove the sub-bucket from the source bucket.
		srcCursor := move.src.Cursor()
		srcCursor.seek(newKey)
		delete(move.src.buckets, string(newKey))
		srcCursor.node().del(newKey)

		// Add the sub-bucket to the destination bucket. The header stored
		// in the parent is only refreshed on spill, so carry the child's
		// current header (e.g. its sequence) into the new value.
		newValue := cloneBytes(move.srcValue)
		*(*common.InBucket)(unsafe.Pointer(&newValue[0])) = *move.child.InBucket
		dstCursor := move.dst.Cursor()
		dstCursor.seek(newKey)
		dstCursor.node().put(newKey, newKey, newValue, 0, common.BucketLeafFlag)

		// Reuse the already opened child bucket so callers holding a
		// pointer to it, or to any of its descendants, keep working with
		// the same instances and their in-memory state.
		move.child.initializeBucket(newValue)
		move.dst.buckets[string(newKey)] = move.child
	}
}
