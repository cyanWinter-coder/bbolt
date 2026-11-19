package bbolt_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"

	"github.com/stretchr/testify/require"
)

var errBatchSentinel = errors.New("batch context test sentinel")

// batchCtxPut returns a BatchContext callback that writes i into the
// "widgets" bucket.
func batchCtxPut(i int) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
	}
}

// TestDB_BatchContext_Timeout verifies that a canceled context makes
// BatchContext return the context error without waiting for the delayed
// batch to run, while the batch still commits on its own afterward.
func TestDB_BatchContext_Timeout(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	db.MaxBatchSize = 1000
	// Longer than the context deadline, short enough to keep the test fast.
	db.MaxBatchDelay = 50 * time.Millisecond

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := db.BatchContext(ctx, batchCtxPut(1))
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("BatchContext blocked for %v after context deadline", elapsed)
	}

	// The caller gave up, but the batch must still run and commit.
	waitForWidget(t, db, 1)
	drainBatches(t, db)
}

// TestDB_BatchContext_Canceled verifies that an already-canceled context
// makes BatchContext give up immediately, while the call still joins a
// batch and is committed by the background batch.
func TestDB_BatchContext_Canceled(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	db.MaxBatchSize = 1000
	db.MaxBatchDelay = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := db.BatchContext(ctx, batchCtxPut(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("BatchContext blocked for %v with a pre-canceled context", elapsed)
	}

	// The batch must still trigger on its own and commit the call.
	waitForWidget(t, db, 1)
	drainBatches(t, db)
}

// TestDB_BatchContext_CancelDoesNotBlockOthers ensures that one caller
// canceling neither blocks the other callers in the same batch nor
// subsequent batches.
func TestDB_BatchContext_CancelDoesNotBlockOthers(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	db.MaxBatchSize = 1000
	// Short enough to keep the test fast, long enough that the batch
	// only triggers after all callers below have joined.
	db.MaxBatchDelay = 50 * time.Millisecond

	// Two callers join the same batch; the first one gives up before the
	// batch triggers.
	started := make(chan struct{}, 2)
	errCh1 := make(chan error, 1)
	go func() {
		started <- struct{}{}
		errCh1 <- db.BatchContext(context.Background(), batchCtxPut(1))
	}()
	<-started

	ctx1, cancel1 := context.WithCancel(context.Background())
	errCh2 := make(chan error, 1)
	go func() {
		started <- struct{}{}
		errCh2 <- db.BatchContext(ctx1, batchCtxPut(2))
	}()
	<-started
	waitForBatchCallCount(t, db, 2)
	cancel1()
	if err := <-errCh2; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller got %v, want context.Canceled", err)
	}

	// The canceled caller must not stop the batch: the surviving caller
	// still completes and its write commits.
	if err := <-errCh1; err != nil {
		t.Fatalf("non-canceled caller failed: %v", err)
	}
	waitForWidget(t, db, 1)
	waitForWidget(t, db, 2)

	// Subsequent batches must keep working after the canceled one.
	if err := db.BatchContext(context.Background(), batchCtxPut(3)); err != nil {
		t.Fatalf("subsequent batch failed: %v", err)
	}
	waitForWidget(t, db, 3)
	drainBatches(t, db)
}

// TestDB_BatchContext_ConcurrentCancel hammers batches with many callers,
// half of which cancel while waiting. No caller may block forever, the
// surviving callers must succeed, and all committed data must land.
func TestDB_BatchContext_ConcurrentCancel(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	db.MaxBatchSize = 8
	db.MaxBatchDelay = 5 * time.Millisecond

	const total = 200
	var wg sync.WaitGroup
	errs := make(chan error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if i%2 == 0 {
				// Half of the callers cancel shortly after enqueuing.
				go func() {
					time.Sleep(time.Duration(i%5) * time.Millisecond)
					cancel()
				}()
			}
			errs <- db.BatchContext(ctx, batchCtxPut(i))
		}(i)
	}
	wg.Wait()
	close(errs)

	var canceled, other int
	for err := range errs {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			canceled++
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		default:
			other++
		}
	}
	if canceled == 0 {
		t.Fatalf("expected some callers to observe cancellation, got none")
	}
	if other == 0 {
		t.Fatalf("expected some callers to complete successfully, got none")
	}

	// Every key whose call was actually committed must be present.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		c := b.Cursor()
		var n int
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			n++
		}
		if n == 0 {
			t.Fatalf("no data committed at all")
		}
		return nil
	}))

	// Drain any batch still pending or running so no timer/batch
	// goroutine survives into the test's MustCheck teardown.
	drainBatches(t, db)
}

// drainBatches waits for any pending batch to be picked up and for all
// already-started batch transactions to finish, so no timer/batch
// goroutine survives into the test's MustCheck teardown.
func drainBatches(t *testing.T, db *btesting.DB) {
	t.Helper()
	waitForBatchCallCount(t, db, 0)
	// A direct write transaction serializes behind every previously
	// started batch's transaction (write txns take rwlock), so once it
	// returns no batch can still be running. It does not create a new
	// pending batch that could wait on the timer.
	if err := db.Update(func(tx *bolt.Tx) error { return nil }); err != nil {
		t.Fatalf("failed to drain batches: %v", err)
	}
}

// TestDB_BatchContext_ErrorPropagation verifies that a callback error is
// surfaced to its own caller while the rest of the batch still commits,
// preserving the existing trySolo re-run semantics of Batch.
func TestDB_BatchContext_ErrorPropagation(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	db.MaxBatchSize = 1000
	db.MaxBatchDelay = 50 * time.Millisecond

	var failRan syncCount
	errCh := make(chan error, 3)

	// Good call, failing call, good call in the same batch. None of them
	// cancels, so the pending call count is a reliable join barrier.
	go func() { errCh <- db.BatchContext(context.Background(), batchCtxPut(1)) }()
	waitForBatchCallCount(t, db, 1)
	go func() {
		errCh <- db.BatchContext(context.Background(), func(tx *bolt.Tx) error {
			failRan.inc()
			return errBatchSentinel
		})
	}()
	waitForBatchCallCount(t, db, 2)
	go func() { errCh <- db.BatchContext(context.Background(), batchCtxPut(2)) }()
	waitForBatchCallCount(t, db, 3)

	var gotSentinel bool
	for i := 0; i < 3; i++ {
		err := <-errCh
		if errors.Is(err, errBatchSentinel) {
			gotSentinel = true
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if !gotSentinel {
		t.Fatalf("failing caller did not receive its error")
	}
	// The failing callback runs once inside the batch, then again in the
	// solo re-run which returns the same error instead of swallowing it.
	if n := failRan.get(); n != 2 {
		t.Fatalf("failing callback ran %d times, want 2 (batch + solo)", n)
	}

	waitForWidget(t, db, 1)
	waitForWidget(t, db, 2)
}

// TestDB_BatchContext_ResultWinsRace verifies that when cancellation and
// batch completion happen together, the caller gets exactly one result
// and never observes a partial/ambiguous state.
//
// The racing caller's callback blocks the batch transaction on a gate,
// the test cancels while the batch is in flight, and only then releases
// the gate so the transaction commits. This deterministically exercises
// the cancel/commit race without depending on timer timing. The batch is
// size-triggered (long MaxBatchDelay), so no timer goroutine is left
// behind at teardown.
func TestDB_BatchContext_ResultWinsRace(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	const rounds = 50
	for r := 0; r < rounds; r++ {
		db.MaxBatchSize = 3
		db.MaxBatchDelay = time.Hour

		gate := make(chan struct{})
		entered := make(chan struct{}, 1)

		errs := make(chan error, 3)
		var wg sync.WaitGroup

		// Two ordinary callers.
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func(key int) {
				defer wg.Done()
				errs <- db.BatchContext(context.Background(), batchCtxPut(key))
			}(r * 3)
		}

		// The racing caller gates the whole batch transaction: entered
		// fires once the batch has definitely started running.
		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- db.BatchContext(ctx, func(tx *bolt.Tx) error {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-gate
				return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(r*3+2)), []byte{})
			})
		}()
		<-entered

		// The batch is running but cannot commit; cancel the racing
		// caller now, then release the transaction.
		cancel()
		close(gate)

		var sawCancel, sawSuccess bool
		for j := 0; j < 3; j++ {
			err := <-errs
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				sawCancel = true
			case err == nil:
				sawSuccess = true
			default:
				t.Fatalf("round %d: unexpected error %v", r, err)
			}
		}
		wg.Wait()

		// The two non-canceling callers must always succeed, proving the
		// batch was never derailed by the cancellation.
		if !sawSuccess {
			t.Fatalf("round %d: non-canceling callers did not succeed", r)
		}
		// The gated caller's context was already canceled before its
		// transaction could commit, so it must observe cancellation.
		if !sawCancel {
			t.Fatalf("round %d: racing caller did not observe cancellation", r)
		}
	}

	// Every gated write still committed (the batch always runs to
	// completion regardless of cancellation).
	drainBatches(t, db)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for r := 0; r < rounds; r++ {
			if v := b.Get(u64tob(uint64(r*3 + 2))); v == nil {
				t.Fatalf("gated write %d not committed", r*3+2)
			}
		}
		return nil
	}))
}

// TestDB_BatchContext_RespectsBatchLimits ensures the context-aware entry
// point still coalesces up to MaxBatchSize and triggers on
// MaxBatchDelay, i.e. batching limits are unchanged.
func TestDB_BatchContext_RespectsBatchLimits(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, createWidgetsBucket(db))

	const size = 3
	db.MaxBatchSize = size
	db.MaxBatchDelay = time.Hour

	ch := make(chan error, size)
	for i := 0; i < size-1; i++ {
		i := i
		go func() { ch <- db.BatchContext(context.Background(), batchCtxPut(i)) }()
	}
	waitForBatchCallCount(t, db, size-1)

	select {
	case err := <-ch:
		t.Fatalf("batch triggered before reaching MaxBatchSize: %v", err)
	default:
	}

	go func() { ch <- db.BatchContext(context.Background(), batchCtxPut(size-1)) }()
	for i := 0; i < size; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < size; i++ {
		waitForWidget(t, db, i)
	}
}

// syncCount is a concurrency-safe counter used to track callback
// executions.
type syncCount struct {
	mu sync.Mutex
	n  int
}

func (c *syncCount) inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *syncCount) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func createWidgetsBucket(db *btesting.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
}

// waitForWidget polls until key i is visible in the widgets bucket, or
// fails the test after a deadline. It is used after batches whose
// trigger timing is not directly observable.
func waitForWidget(t *testing.T, db *btesting.DB, i int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var found bool
		if err := db.View(func(tx *bolt.Tx) error {
			found = tx.Bucket([]byte("widgets")).Get(u64tob(uint64(i))) != nil
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("widget %d not committed before deadline", i)
}

// waitForBatchCallCount polls the internal pending-batch size via the
// whitebox test hook until it reaches want.
func waitForBatchCallCount(t *testing.T, db *btesting.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := db.DB.PendingBatchCallCount(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d pending batch calls", want)
}
