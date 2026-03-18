package offset

import (
	"io"
	"log/slog"
	"sync"
	"testing"
)

// silentLogger returns a logger that discards all output.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestCoordinator creates a Coordinator with a silent logger for tests.
func newTestCoordinator() Coordinator {
	return NewCoordinator(WithLogger(silentLogger()))
}

func TestHappyPath_DispatchCompleteCommittable(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)

	offsets := c.Committable()
	if len(offsets) != 1 {
		t.Fatalf("expected 1 committable partition, got %d", len(offsets))
	}
	if offsets[0] != 101 {
		t.Fatalf("expected committable offset 101, got %d", offsets[0])
	}
}

func TestMultiplePartitions_IndependentTracking(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)
	c.BatchDispatched(1, 200)
	c.BatchDispatched(2, 300)

	// Complete partitions 0 and 2, leave 1 in-flight.
	c.BatchComplete(0, 100)
	c.BatchComplete(2, 300)

	offsets := c.Committable()
	if len(offsets) != 2 {
		t.Fatalf("expected 2 committable partitions, got %d", len(offsets))
	}
	if offsets[0] != 101 {
		t.Fatalf("expected partition 0 offset 101, got %d", offsets[0])
	}
	if offsets[2] != 301 {
		t.Fatalf("expected partition 2 offset 301, got %d", offsets[2])
	}

	// Partition 1 still in-flight, should not be committable.
	if _, ok := offsets[1]; ok {
		t.Fatal("partition 1 should not be committable while in-flight")
	}
}

func TestInFlightBatch_ExcludedFromCommittable(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 50)

	offsets := c.Committable()
	if len(offsets) != 0 {
		t.Fatalf("expected 0 committable partitions for in-flight batch, got %d", len(offsets))
	}
}

func TestCommittable_ReturnsStableResults(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)

	// First call.
	offsets1 := c.Committable()
	if offsets1[0] != 101 {
		t.Fatalf("expected 101, got %d", offsets1[0])
	}

	// Second call without any state change — same result.
	offsets2 := c.Committable()
	if offsets2[0] != 101 {
		t.Fatalf("expected stable result 101, got %d", offsets2[0])
	}
}

func TestCommittable_ClearedAfterNewDispatch(t *testing.T) {
	c := newTestCoordinator()

	// First cycle.
	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)

	offsets := c.Committable()
	if offsets[0] != 101 {
		t.Fatalf("expected 101, got %d", offsets[0])
	}

	// New dispatch for the same partition — starts a new cycle.
	c.BatchDispatched(0, 200)

	offsets = c.Committable()
	if _, ok := offsets[0]; ok {
		t.Fatal("partition 0 should not be committable while new batch is in-flight")
	}

	// Complete the second batch.
	c.BatchComplete(0, 200)

	offsets = c.Committable()
	if offsets[0] != 201 {
		t.Fatalf("expected 201, got %d", offsets[0])
	}
}

func TestReset_ClearsPartitionState(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)

	c.Reset(0)

	offsets := c.Committable()
	if len(offsets) != 0 {
		t.Fatalf("expected 0 committable partitions after reset, got %d", len(offsets))
	}
}

func TestReset_DoesNotAffectOtherPartitions(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)
	c.BatchDispatched(1, 200)
	c.BatchComplete(0, 100)
	c.BatchComplete(1, 200)

	c.Reset(0)

	offsets := c.Committable()
	if len(offsets) != 1 {
		t.Fatalf("expected 1 committable partition, got %d", len(offsets))
	}
	if offsets[1] != 201 {
		t.Fatalf("expected partition 1 offset 201, got %d", offsets[1])
	}
}

func TestReset_UnknownPartition_NoOp(t *testing.T) {
	c := newTestCoordinator()

	// Should not panic.
	c.Reset(99)
}

func TestDoubleDispatch_Panics(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on double dispatch, got none")
		}
	}()

	c.BatchDispatched(0, 200)
}

func TestBatchComplete_UnknownPartition_NoOp(t *testing.T) {
	c := newTestCoordinator()

	// Should not panic — debug log and no-op.
	c.BatchComplete(99, 100)
}

func TestBatchComplete_Idempotent(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)

	// Second complete with same offset — idempotent, no panic.
	c.BatchComplete(0, 100)

	offsets := c.Committable()
	if offsets[0] != 101 {
		t.Fatalf("expected 101 after idempotent complete, got %d", offsets[0])
	}
}

func TestBatchComplete_MismatchedOffset_Panics(t *testing.T) {
	c := newTestCoordinator()

	c.BatchDispatched(0, 100)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on mismatched offset, got none")
		}
	}()

	c.BatchComplete(0, 999)
}

func TestFullLifecycle_MultipleCycles(t *testing.T) {
	c := newTestCoordinator()

	// Cycle 1.
	c.BatchDispatched(0, 50)
	c.BatchComplete(0, 50)
	offsets := c.Committable()
	if offsets[0] != 51 {
		t.Fatalf("cycle 1: expected 51, got %d", offsets[0])
	}

	// Cycle 2.
	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)
	offsets = c.Committable()
	if offsets[0] != 101 {
		t.Fatalf("cycle 2: expected 101, got %d", offsets[0])
	}

	// Cycle 3.
	c.BatchDispatched(0, 150)
	c.BatchComplete(0, 150)
	offsets = c.Committable()
	if offsets[0] != 151 {
		t.Fatalf("cycle 3: expected 151, got %d", offsets[0])
	}
}

func TestConcurrent_DispatchAndComplete(t *testing.T) {
	c := newTestCoordinator()
	const numPartitions = 100

	var wg sync.WaitGroup
	wg.Add(numPartitions)

	// Dispatch all partitions from a single goroutine (simulates the
	// Dispatcher's Send path — sequential per partition).
	for i := int32(0); i < numPartitions; i++ {
		c.BatchDispatched(i, int64(i*1000))
	}

	// Complete from concurrent goroutines (simulates workers).
	for i := int32(0); i < numPartitions; i++ {
		go func(p int32) {
			defer wg.Done()
			c.BatchComplete(p, int64(p*1000))
		}(i)
	}

	wg.Wait()

	offsets := c.Committable()
	if len(offsets) != numPartitions {
		t.Fatalf("expected %d committable partitions, got %d", numPartitions, len(offsets))
	}

	for i := int32(0); i < numPartitions; i++ {
		expected := int64(i*1000) + 1
		if offsets[i] != expected {
			t.Errorf("partition %d: expected offset %d, got %d", i, expected, offsets[i])
		}
	}
}

func TestConcurrent_CommittableWhileCompleting(t *testing.T) {
	c := newTestCoordinator()
	const numPartitions = 50

	// Dispatch all.
	for i := int32(0); i < numPartitions; i++ {
		c.BatchDispatched(i, int64(i*100))
	}

	// Complete and read committable concurrently.
	var wg sync.WaitGroup
	wg.Add(numPartitions + 10)

	for i := int32(0); i < numPartitions; i++ {
		go func(p int32) {
			defer wg.Done()
			c.BatchComplete(p, int64(p*100))
		}(i)
	}

	// 10 concurrent Committable calls (simulates timer ticks).
	for range 10 {
		go func() {
			defer wg.Done()
			_ = c.Committable()
		}()
	}

	wg.Wait()

	// After all completions, all should be committable.
	offsets := c.Committable()
	if len(offsets) != numPartitions {
		t.Fatalf("expected %d committable partitions, got %d", numPartitions, len(offsets))
	}
}

func TestEmptyCoordinator_CommittableReturnsEmpty(t *testing.T) {
	c := newTestCoordinator()

	offsets := c.Committable()
	if len(offsets) != 0 {
		t.Fatalf("expected 0 committable partitions on empty coordinator, got %d", len(offsets))
	}
}

func TestDispatchAfterReset_WorksNormally(t *testing.T) {
	c := newTestCoordinator()

	// First cycle.
	c.BatchDispatched(0, 100)
	c.BatchComplete(0, 100)

	// Revocation — reset.
	c.Reset(0)

	// Re-assigned — new cycle.
	c.BatchDispatched(0, 500)
	c.BatchComplete(0, 500)

	offsets := c.Committable()
	if offsets[0] != 501 {
		t.Fatalf("expected 501 after reset and re-dispatch, got %d", offsets[0])
	}
}
