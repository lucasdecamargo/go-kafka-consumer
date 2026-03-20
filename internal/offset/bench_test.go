package offset

import (
	"fmt"
	"log/slog"
	"testing"
)

// BenchmarkCoordinatorCycle measures the full offset tracking cycle:
// BatchDispatched → BatchComplete → Committable. This is the hot path
// that runs for every batch processed.
func BenchmarkCoordinatorCycle(b *testing.B) {
	for _, partitions := range []int{1, 6, 12, 24} {
		b.Run(fmt.Sprintf("partitions=%d", partitions), func(b *testing.B) {
			c := NewCoordinator(WithLogger(slog.New(slog.DiscardHandler)))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for p := range int32(partitions) {
					c.BatchDispatched(p, int64(p)*1000)
				}
				for p := range int32(partitions) {
					c.BatchComplete(p, int64(p)*1000)
				}
				offsets := c.Committable()
				_ = offsets

				// Reset for next iteration.
				for p := range int32(partitions) {
					c.Reset(p)
				}
			}
		})
	}
}

// BenchmarkCommittable measures the cost of scanning completed partitions
// to produce committable offsets. This is called on every commit interval
// tick, so its cost scales with partition count.
func BenchmarkCommittable(b *testing.B) {
	for _, partitions := range []int{1, 6, 12, 24, 48} {
		b.Run(fmt.Sprintf("partitions=%d", partitions), func(b *testing.B) {
			c := NewCoordinator(WithLogger(slog.New(slog.DiscardHandler)))

			// Pre-fill with completed batches.
			for p := range int32(partitions) {
				c.BatchDispatched(p, int64(p)*1000)
				c.BatchComplete(p, int64(p)*1000)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				offsets := c.Committable()
				_ = offsets
			}
		})
	}
}

// BenchmarkBatchDispatched measures the cost of recording a single batch
// dispatch. This runs in the hot path of Send().
func BenchmarkBatchDispatched(b *testing.B) {
	c := NewCoordinator(WithLogger(slog.New(slog.DiscardHandler)))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.BatchDispatched(0, 100)
		c.BatchComplete(0, 100)
	}
}
