package offset

import (
	"fmt"
	"log/slog"
	"sync"
)

// partitionState tracks the batch lifecycle for a single partition.
type partitionState struct {
	maxOffset int64
	inFlight  bool
	completed bool
}

// coordinator is the concrete implementation of the Coordinator interface.
// It uses a mutex-protected map to track per-partition batch state.
type coordinator struct {
	mu         sync.Mutex
	partitions map[int32]*partitionState
	logger     *slog.Logger
}

// NewCoordinator creates a new OffsetCoordinator.
func NewCoordinator(opts ...CoordinatorOption) Coordinator {
	o := defaultCoordinatorOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return &coordinator{
		partitions: make(map[int32]*partitionState),
		logger:     o.logger,
	}
}

// BatchDispatched records that a batch has been dispatched to a worker.
// Panics if a batch is already in-flight for the partition.
func (c *coordinator) BatchDispatched(partition int32, maxOffset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, exists := c.partitions[partition]
	if exists && state.inFlight {
		panic(fmt.Sprintf(
			"offset coordinator: partition %d already has an in-flight batch (offset %d); "+
				"attempted to dispatch new batch (offset %d)",
			partition, state.maxOffset, maxOffset,
		))
	}

	c.partitions[partition] = &partitionState{
		maxOffset: maxOffset,
		inFlight:  true,
		completed: false,
	}

	c.logger.Debug("batch dispatched",
		slog.Int("partition", int(partition)),
		slog.Int64("max_offset", maxOffset),
	)
}

// BatchComplete records that the in-flight batch for the partition has
// been fully processed.
func (c *coordinator) BatchComplete(partition int32, maxOffset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, exists := c.partitions[partition]
	if !exists {
		c.logger.Debug("batch complete called for unknown partition",
			slog.Int("partition", int(partition)),
			slog.Int64("max_offset", maxOffset),
		)
		return
	}

	// Idempotent: already completed with the same offset.
	if state.completed && state.maxOffset == maxOffset {
		return
	}

	if state.maxOffset != maxOffset {
		panic(fmt.Sprintf(
			"offset coordinator: partition %d BatchComplete offset %d does not match "+
				"dispatched offset %d",
			partition, maxOffset, state.maxOffset,
		))
	}

	state.inFlight = false
	state.completed = true

	c.logger.Debug("batch completed",
		slog.Int("partition", int(partition)),
		slog.Int64("max_offset", maxOffset),
	)
}

// Committable returns offsets safe to commit (maxOffset+1 for each
// completed partition). Returns the same offsets on repeated calls
// until the state changes.
func (c *coordinator) Committable() map[int32]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make(map[int32]int64)
	for partition, state := range c.partitions {
		if state.completed {
			result[partition] = state.maxOffset + 1
		}
	}
	return result
}

// Reset clears all tracking state for the given partition.
func (c *coordinator) Reset(partition int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.partitions, partition)

	c.logger.Debug("partition reset",
		slog.Int("partition", int(partition)),
	)
}
