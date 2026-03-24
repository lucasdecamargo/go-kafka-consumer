package pollloop

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasdecamargo/go-kafka-consumer/internal/circuit"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/dispatcher"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/metrics"
	"github.com/lucasdecamargo/go-kafka-consumer/internal/types"
)

// ---------------------------------------------------------------------------
// Mock KafkaConsumer
// ---------------------------------------------------------------------------

type mockKafka struct {
	mu             sync.Mutex
	pollFn         func(timeoutMs int) ([]types.Message, error)
	commitFn       func(offsets map[int32]int64) error
	pauseFn        func(partitions []int32) error
	resumeFn       func(partitions []int32) error
	assignmentFn   func() ([]dispatcher.Partition, error)
	closeFn        func() error
	pausedCalls    [][]int32
	resumedCalls   [][]int32
	committedCalls []map[int32]int64
	closed         bool
}

func newMockKafka() *mockKafka {
	return &mockKafka{
		pollFn:       func(int) ([]types.Message, error) { return nil, nil },
		commitFn:     func(map[int32]int64) error { return nil },
		pauseFn:      func([]int32) error { return nil },
		resumeFn:     func([]int32) error { return nil },
		assignmentFn: func() ([]dispatcher.Partition, error) { return nil, nil },
		closeFn:      func() error { return nil },
	}
}

func (m *mockKafka) Poll(timeoutMs int) ([]types.Message, error) {
	return m.pollFn(timeoutMs)
}

func (m *mockKafka) CommitOffsets(offsets map[int32]int64) error {
	m.mu.Lock()
	cpy := make(map[int32]int64, len(offsets))
	for k, v := range offsets {
		cpy[k] = v
	}
	m.committedCalls = append(m.committedCalls, cpy)
	m.mu.Unlock()
	return m.commitFn(offsets)
}

func (m *mockKafka) Pause(partitions []int32) error {
	m.mu.Lock()
	cpy := make([]int32, len(partitions))
	copy(cpy, partitions)
	m.pausedCalls = append(m.pausedCalls, cpy)
	m.mu.Unlock()
	return m.pauseFn(partitions)
}

func (m *mockKafka) Resume(partitions []int32) error {
	m.mu.Lock()
	cpy := make([]int32, len(partitions))
	copy(cpy, partitions)
	m.resumedCalls = append(m.resumedCalls, cpy)
	m.mu.Unlock()
	return m.resumeFn(partitions)
}

func (m *mockKafka) Assignment() ([]dispatcher.Partition, error) {
	return m.assignmentFn()
}

func (m *mockKafka) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return m.closeFn()
}

// ---------------------------------------------------------------------------
// Mock Dispatcher
// ---------------------------------------------------------------------------

type mockDispatcher struct {
	mu            sync.Mutex
	sendFn        func(ctx context.Context, partition int32, msgs []types.Message) error
	readyCh       chan struct{}
	circuitCh     chan circuit.State
	assignedCalls [][]dispatcher.Partition
	revokedCalls  [][]dispatcher.Partition
	closeFn       func(ctx context.Context) error
	sendCalls     []sendCall
}

type sendCall struct {
	Partition int32
	Messages  []types.Message
}

func newMockDispatcher() *mockDispatcher {
	return &mockDispatcher{
		sendFn:    func(context.Context, int32, []types.Message) error { return nil },
		readyCh:   make(chan struct{}, 1),
		circuitCh: make(chan circuit.State, 3),
		closeFn:   func(context.Context) error { return nil },
	}
}

func (m *mockDispatcher) Start(_ context.Context, _ context.CancelFunc) {}

func (m *mockDispatcher) Send(ctx context.Context, partition int32, msgs []types.Message) error {
	m.mu.Lock()
	m.sendCalls = append(m.sendCalls, sendCall{Partition: partition, Messages: msgs})
	m.mu.Unlock()
	return m.sendFn(ctx, partition, msgs)
}

func (m *mockDispatcher) Ready() <-chan struct{} {
	return m.readyCh
}

func (m *mockDispatcher) CircuitStateChanged() <-chan circuit.State {
	return m.circuitCh
}

func (m *mockDispatcher) OnPartitionsAssigned(partitions []dispatcher.Partition) {
	m.mu.Lock()
	m.assignedCalls = append(m.assignedCalls, partitions)
	m.mu.Unlock()
}

func (m *mockDispatcher) OnPartitionsRevoked(partitions []dispatcher.Partition) {
	m.mu.Lock()
	m.revokedCalls = append(m.revokedCalls, partitions)
	m.mu.Unlock()
}

func (m *mockDispatcher) Close(ctx context.Context) error {
	return m.closeFn(ctx)
}

func (m *mockDispatcher) getSendCalls() []sendCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cpy := make([]sendCall, len(m.sendCalls))
	copy(cpy, m.sendCalls)
	return cpy
}

// ---------------------------------------------------------------------------
// Mock OffsetCoordinator
// ---------------------------------------------------------------------------

type mockCoordinator struct {
	mu            sync.Mutex
	committable   map[int32]int64
	resetCalls    []int32
	dispatchCalls []dispatchCall
	completeCalls []completeCall
}

type dispatchCall struct {
	Partition int32
	MaxOffset int64
}

type completeCall struct {
	Partition int32
	MaxOffset int64
}

func newMockCoordinator() *mockCoordinator {
	return &mockCoordinator{
		committable: make(map[int32]int64),
	}
}

func (m *mockCoordinator) BatchDispatched(partition int32, maxOffset int64) {
	m.mu.Lock()
	m.dispatchCalls = append(m.dispatchCalls, dispatchCall{partition, maxOffset})
	m.mu.Unlock()
}

func (m *mockCoordinator) BatchComplete(partition int32, maxOffset int64) {
	m.mu.Lock()
	m.completeCalls = append(m.completeCalls, completeCall{partition, maxOffset})
	m.mu.Unlock()
}

func (m *mockCoordinator) Committable() map[int32]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	cpy := make(map[int32]int64, len(m.committable))
	for k, v := range m.committable {
		cpy[k] = v
	}
	return cpy
}

func (m *mockCoordinator) Reset(partition int32) {
	m.mu.Lock()
	m.resetCalls = append(m.resetCalls, partition)
	delete(m.committable, partition)
	m.mu.Unlock()
}

func (m *mockCoordinator) setCommittable(offsets map[int32]int64) {
	m.mu.Lock()
	m.committable = offsets
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

func testConfig() Config {
	return Config{
		PollInterval:           10 * time.Millisecond,
		CommitInterval:         20 * time.Millisecond,
		CommitFailureThreshold: 3,
		ShutdownTimeout:        1 * time.Second,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestPollLoop(
	kafka *mockKafka,
	disp *mockDispatcher,
	coord *mockCoordinator,
) (*PollLoop, *Health) {
	h := &Health{}
	reg := prometheus.NewRegistry()
	pl, err := New(
		testConfig(),
		kafka,
		disp,
		coord,
		h,
		WithLogger(discardLogger()),
		WithMetrics(metrics.NewPollLoopMetrics(reg)),
	)
	if err != nil {
		panic(err)
	}
	return pl, h
}

// runFor starts the poll loop and cancels it after the given duration.
// Returns after the poll loop exits.
func runFor(t *testing.T, pl *PollLoop, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_ = pl.Run(ctx)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNew_RequiredDependencies(t *testing.T) {
	h := &Health{}
	m := metrics.NewPollLoopMetrics(prometheus.NewRegistry())
	cfg := testConfig()
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	// All valid.
	_, err := New(cfg, kafka, disp, coord, h, WithMetrics(m))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Missing kafka.
	_, err = New(cfg, nil, disp, coord, h, WithMetrics(m))
	if err == nil {
		t.Fatal("expected error for nil kafka")
	}

	// Missing dispatcher.
	_, err = New(cfg, kafka, nil, coord, h, WithMetrics(m))
	if err == nil {
		t.Fatal("expected error for nil dispatcher")
	}

	// Missing coordinator.
	_, err = New(cfg, kafka, disp, nil, h, WithMetrics(m))
	if err == nil {
		t.Fatal("expected error for nil coordinator")
	}

	// Missing health.
	_, err = New(cfg, kafka, disp, coord, nil, WithMetrics(m))
	if err == nil {
		t.Fatal("expected error for nil health")
	}
}

func TestHappyPath_PollAndDispatch(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	msgs := []types.Message{
		{Partition: 0, Offset: 10, Value: []byte("a")},
		{Partition: 0, Offset: 11, Value: []byte("b")},
		{Partition: 1, Offset: 20, Value: []byte("c")},
	}

	var pollCount int
	kafka.pollFn = func(int) ([]types.Message, error) {
		pollCount++
		if pollCount == 1 {
			return msgs, nil
		}
		return nil, nil
	}

	pl, health := newTestPollLoop(kafka, disp, coord)
	runFor(t, pl, 50*time.Millisecond)

	// After Run returns, live should be false.
	if health.IsLive() {
		t.Error("expected IsLive() == false after Run returns")
	}

	calls := disp.getSendCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one Send call")
	}

	// Verify messages were grouped by partition.
	gotPartitions := make(map[int32]int)
	for _, c := range calls {
		gotPartitions[c.Partition] += len(c.Messages)
	}

	if gotPartitions[0] < 2 {
		t.Errorf("expected at least 2 messages for partition 0, got %d", gotPartitions[0])
	}
	if gotPartitions[1] < 1 {
		t.Errorf("expected at least 1 message for partition 1, got %d", gotPartitions[1])
	}
}

func TestPeriodicOffsetCommit(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	coord.setCommittable(map[int32]int64{0: 150, 1: 300})

	pl, _ := newTestPollLoop(kafka, disp, coord)
	runFor(t, pl, 80*time.Millisecond) // enough for ~3 commit ticks

	kafka.mu.Lock()
	commitCount := len(kafka.committedCalls)
	kafka.mu.Unlock()

	if commitCount == 0 {
		t.Fatal("expected at least one commit call")
	}

	// Verify committed offsets.
	kafka.mu.Lock()
	first := kafka.committedCalls[0]
	kafka.mu.Unlock()

	if first[0] != 150 || first[1] != 300 {
		t.Errorf("unexpected committed offsets: %v", first)
	}
}

func TestBackpressure_PauseAndResume(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	kafka.pollFn = func(int) ([]types.Message, error) {
		return []types.Message{
			{Partition: 0, Offset: 10, Value: []byte("a")},
		}, nil
	}

	// Dispatcher returns backpressure on first call.
	var sendCount int
	disp.sendFn = func(_ context.Context, _ int32, _ []types.Message) error {
		sendCount++
		if sendCount == 1 {
			return dispatcher.ErrBackpressure
		}
		return nil
	}

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, _ := newTestPollLoop(kafka, disp, coord)

	// Run briefly, expect partition 0 to be paused.
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Wait for the pause to happen.
	time.Sleep(50 * time.Millisecond)

	kafka.mu.Lock()
	pauseCount := len(kafka.pausedCalls)
	kafka.mu.Unlock()

	if pauseCount == 0 {
		t.Fatal("expected partition to be paused on backpressure")
	}

	// Signal ready — should resume.
	disp.readyCh <- struct{}{}
	time.Sleep(30 * time.Millisecond)

	kafka.mu.Lock()
	resumeCount := len(kafka.resumedCalls)
	kafka.mu.Unlock()

	if resumeCount == 0 {
		t.Fatal("expected partition to be resumed after Ready signal")
	}

	cancel()
	<-done
}

func TestBrokerUnavailable_DegradedMode(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	// Always have committable offsets so commit is attempted.
	coord.setCommittable(map[int32]int64{0: 100})

	// CommitOffsets always fails.
	kafka.commitFn = func(map[int32]int64) error {
		return errors.New("broker unreachable")
	}

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Simulate a prior partition assignment so that degraded mode — not a
	// missing partition — is the sole reason /readyz returns 503.
	time.Sleep(10 * time.Millisecond)
	health.SetPartitionsAssigned(true)

	// Wait for threshold to be reached (3 failures × 20ms interval = ~60ms).
	time.Sleep(120 * time.Millisecond)

	if !pl.degraded.Has(BrokerUnavailable) {
		t.Error("expected BrokerUnavailable degraded reason to be set")
	}

	if health.IsReady() {
		t.Error("expected readiness to be false in degraded mode")
	}

	cancel()
	<-done
}

func TestBrokerRecovery_ExitsDegradedMode(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	coord.setCommittable(map[int32]int64{0: 100})

	// Fail commits, then succeed.
	var commitCount int
	kafka.commitFn = func(map[int32]int64) error {
		commitCount++
		if commitCount <= 3 {
			return errors.New("broker unreachable")
		}
		return nil // Broker recovered.
	}

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Simulate an initial partition assignment so that /readyz can
	// return 200 once the consumer recovers from degraded mode.
	// In tests the mock kafka never fires rebalance callbacks, so we
	// set the flag directly on the Health object.
	time.Sleep(10 * time.Millisecond)
	health.SetPartitionsAssigned(true)

	// Wait for degraded mode then recovery.
	time.Sleep(200 * time.Millisecond)

	if pl.degraded.IsDegraded() {
		t.Error("expected degraded mode to be cleared after broker recovery")
	}

	if !health.IsReady() {
		t.Error("expected readiness to be true after recovery")
	}

	cancel()
	<-done
}

func TestCircuitBreakerOpen_EntersDegradedMode(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Simulate an initial partition assignment. In tests the mock kafka
	// never fires rebalance callbacks, so we set the flag directly.
	time.Sleep(10 * time.Millisecond)
	health.SetPartitionsAssigned(true)

	// Simulate circuit breaker opening.
	disp.circuitCh <- circuit.Open
	time.Sleep(30 * time.Millisecond)

	if !pl.degraded.Has(TargetUnavailable) {
		t.Error("expected TargetUnavailable degraded reason")
	}
	if health.IsReady() {
		t.Error("expected readiness to be false when circuit is open")
	}

	// Simulate circuit recovery.
	disp.circuitCh <- circuit.Closed
	time.Sleep(30 * time.Millisecond)

	if pl.degraded.Has(TargetUnavailable) {
		t.Error("expected TargetUnavailable to be cleared after circuit closes")
	}
	if !health.IsReady() {
		t.Error("expected readiness to be true after circuit closes")
	}

	cancel()
	<-done
}

func TestDualDegradedMode_BothReasons(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	coord.setCommittable(map[int32]int64{0: 100})
	kafka.commitFn = func(map[int32]int64) error {
		return errors.New("broker down")
	}

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Wait for broker unavailable degraded mode.
	time.Sleep(120 * time.Millisecond)

	// Also trip the circuit breaker.
	disp.circuitCh <- circuit.Open
	time.Sleep(30 * time.Millisecond)

	if !pl.degraded.Has(BrokerUnavailable) || !pl.degraded.Has(TargetUnavailable) {
		t.Error("expected both degraded reasons to be set")
	}
	if health.IsReady() {
		t.Error("expected not ready")
	}

	// Clear circuit — should still be degraded (broker still down).
	disp.circuitCh <- circuit.Closed
	time.Sleep(30 * time.Millisecond)

	if !pl.degraded.Has(BrokerUnavailable) {
		t.Error("expected BrokerUnavailable to still be set")
	}
	if health.IsReady() {
		t.Error("expected not ready — broker still unavailable")
	}

	cancel()
	<-done
}

func TestOnPartitionsAssigned(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	pl, _ := newTestPollLoop(kafka, disp, coord)

	partitions := []dispatcher.Partition{
		{Topic: "test", Partition: 0},
		{Topic: "test", Partition: 1},
	}
	pl.OnPartitionsAssigned(partitions)

	disp.mu.Lock()
	defer disp.mu.Unlock()
	if len(disp.assignedCalls) != 1 {
		t.Fatalf("expected 1 assigned call, got %d", len(disp.assignedCalls))
	}
	if len(disp.assignedCalls[0]) != 2 {
		t.Errorf("expected 2 partitions assigned, got %d", len(disp.assignedCalls[0]))
	}
}

func TestOnPartitionsRevoked_CommitsAndResets(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	// Set up committable offsets.
	coord.setCommittable(map[int32]int64{0: 150, 1: 300, 2: 450})

	pl, _ := newTestPollLoop(kafka, disp, coord)

	// Revoke partitions 0 and 2 (not 1).
	revoked := []dispatcher.Partition{
		{Topic: "test", Partition: 0},
		{Topic: "test", Partition: 2},
	}
	pl.OnPartitionsRevoked(revoked)

	// Verify dispatcher was notified.
	disp.mu.Lock()
	if len(disp.revokedCalls) != 1 {
		t.Fatalf("expected 1 revoked call, got %d", len(disp.revokedCalls))
	}
	disp.mu.Unlock()

	// Verify only revoked partitions were committed.
	kafka.mu.Lock()
	if len(kafka.committedCalls) != 1 {
		t.Fatalf("expected 1 commit call, got %d", len(kafka.committedCalls))
	}
	committed := kafka.committedCalls[0]
	kafka.mu.Unlock()

	if committed[0] != 150 || committed[2] != 450 {
		t.Errorf("expected revoked offsets {0:150, 2:450}, got %v", committed)
	}
	if _, ok := committed[1]; ok {
		t.Error("partition 1 should not have been committed (not revoked)")
	}

	// Verify resets.
	coord.mu.Lock()
	if len(coord.resetCalls) != 2 {
		t.Fatalf("expected 2 reset calls, got %d", len(coord.resetCalls))
	}
	coord.mu.Unlock()
}

func TestGracefulShutdown_CommitsAndCloses(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	coord.setCommittable(map[int32]int64{0: 200, 1: 400})

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Let it start.
	time.Sleep(20 * time.Millisecond)

	// Trigger shutdown.
	cancel()
	err := <-done

	if err != nil {
		t.Errorf("expected nil error from clean shutdown, got: %v", err)
	}

	// Verify final commit happened.
	kafka.mu.Lock()
	hasCommit := false
	for _, c := range kafka.committedCalls {
		if c[0] == 200 && c[1] == 400 {
			hasCommit = true
			break
		}
	}
	kafka.mu.Unlock()

	if !hasCommit {
		t.Error("expected final offset commit during shutdown")
	}

	// Verify Kafka consumer was closed.
	kafka.mu.Lock()
	wasClosed := kafka.closed
	kafka.mu.Unlock()

	if !wasClosed {
		t.Error("expected Kafka consumer to be closed")
	}

	// Verify health.
	if health.IsReady() {
		t.Error("expected readiness to be false after shutdown")
	}
	if health.IsLive() {
		t.Error("expected liveness to be false after shutdown")
	}
}

func TestShutdownTimeout_DispatcherSlowDrain(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	// Dispatcher.Close blocks for longer than shutdown timeout.
	disp.closeFn = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	coord.setCommittable(map[int32]int64{0: 100})

	// Use a very short shutdown timeout.
	h := &Health{}
	reg := prometheus.NewRegistry()
	cfg := testConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond

	pl, err := New(cfg, kafka, disp, coord, h,
		WithLogger(discardLogger()),
		WithMetrics(metrics.NewPollLoopMetrics(reg)),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	runErr := <-done
	if runErr == nil {
		t.Error("expected error from slow dispatcher shutdown")
	}

	// Even on timeout, Kafka consumer should be closed.
	kafka.mu.Lock()
	wasClosed := kafka.closed
	kafka.mu.Unlock()

	if !wasClosed {
		t.Error("expected Kafka consumer to be closed even on timeout")
	}
}

func TestHealthProbes(t *testing.T) {
	h := &Health{}

	if h.IsLive() {
		t.Error("expected not live initially")
	}
	if h.IsReady() {
		t.Error("expected not ready initially")
	}

	h.SetLive(true)
	// Readiness requires both ready=true AND partitionsAssigned=true.
	h.SetReady(true)
	if h.IsReady() {
		t.Error("expected not ready when ready=true but no partition assigned yet")
	}

	h.SetPartitionsAssigned(true)
	if !h.IsLive() {
		t.Error("expected live after SetLive(true)")
	}
	if !h.IsReady() {
		t.Error("expected ready after SetReady(true) and SetPartitionsAssigned(true)")
	}

	h.SetReady(false)
	if h.IsReady() {
		t.Error("expected not ready after SetReady(false)")
	}
	if !h.IsLive() {
		t.Error("expected still live when only readiness changed")
	}

	// Restore ready, then revoke all partitions — should become not ready.
	h.SetReady(true)
	h.SetPartitionsAssigned(false)
	if h.IsReady() {
		t.Error("expected not ready after all partitions revoked")
	}
}

func TestReadyz_NotReadyBeforePartitionAssignment(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	// Poll loop is running (live) but no partition has been assigned yet.
	time.Sleep(30 * time.Millisecond)

	if !health.IsLive() {
		t.Fatal("expected IsLive() == true once Run() starts")
	}
	if health.IsReady() {
		t.Error("expected IsReady() == false before any partition is assigned")
	}
}

func TestReadyz_ReadyAfterPartitionAssignment(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	time.Sleep(20 * time.Millisecond)

	// Simulate the rebalance callback that the Kafka adapter fires.
	pl.OnPartitionsAssigned([]dispatcher.Partition{
		{Topic: "test", Partition: 0},
		{Topic: "test", Partition: 1},
	})

	if !health.IsReady() {
		t.Error("expected IsReady() == true after partitions are assigned")
	}
}

func TestReadyz_NotReadyAfterAllPartitionsRevoked(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	time.Sleep(20 * time.Millisecond)

	partitions := []dispatcher.Partition{
		{Topic: "test", Partition: 0},
		{Topic: "test", Partition: 1},
	}

	// Assign then immediately revoke all partitions (scale-down scenario).
	pl.OnPartitionsAssigned(partitions)
	if !health.IsReady() {
		t.Fatal("expected IsReady() == true after assignment")
	}

	pl.OnPartitionsRevoked(partitions)
	if health.IsReady() {
		t.Error("expected IsReady() == false after all partitions are revoked")
	}
}

func TestReadyz_StillReadyAfterPartialRevoke(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	time.Sleep(20 * time.Millisecond)

	all := []dispatcher.Partition{
		{Topic: "test", Partition: 0},
		{Topic: "test", Partition: 1},
		{Topic: "test", Partition: 2},
	}
	subset := []dispatcher.Partition{
		{Topic: "test", Partition: 2},
	}

	pl.OnPartitionsAssigned(all)
	if !health.IsReady() {
		t.Fatal("expected IsReady() == true after assignment")
	}

	// Revoke only one of three — two remain assigned, must still be ready.
	pl.OnPartitionsRevoked(subset)
	if !health.IsReady() {
		t.Errorf("expected IsReady() == true after partial revoke (2 of 3 partitions remain)")
	}
}

func TestReadyz_ReadyAfterReassignment(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	time.Sleep(20 * time.Millisecond)

	partitions := []dispatcher.Partition{
		{Topic: "test", Partition: 0},
		{Topic: "test", Partition: 1},
	}

	// Full cycle: assign → full revoke → re-assign (KEDA scale-down/up).
	pl.OnPartitionsAssigned(partitions)
	if !health.IsReady() {
		t.Fatal("expected ready after first assignment")
	}

	pl.OnPartitionsRevoked(partitions)
	if health.IsReady() {
		t.Fatal("expected not ready after full revoke")
	}

	pl.OnPartitionsAssigned(partitions)
	if !health.IsReady() {
		t.Error("expected IsReady() == true after re-assignment following full revoke")
	}
}

func TestGroupByPartition(t *testing.T) {
	msgs := []types.Message{
		{Partition: 0, Offset: 1},
		{Partition: 1, Offset: 2},
		{Partition: 0, Offset: 3},
		{Partition: 2, Offset: 4},
		{Partition: 1, Offset: 5},
	}

	grouped := groupByPartition(msgs)

	if len(grouped[0]) != 2 {
		t.Errorf("expected 2 messages for partition 0, got %d", len(grouped[0]))
	}
	if len(grouped[1]) != 2 {
		t.Errorf("expected 2 messages for partition 1, got %d", len(grouped[1]))
	}
	if len(grouped[2]) != 1 {
		t.Errorf("expected 1 message for partition 2, got %d", len(grouped[2]))
	}
}

func TestDegradedMode_SkipsDispatchButKeepsPoll(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	var pollCount atomic.Int64
	kafka.pollFn = func(int) ([]types.Message, error) {
		n := pollCount.Add(1)
		return []types.Message{
			{Partition: 0, Offset: n, Value: []byte("x")},
		}, nil
	}

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, _ := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Wait a bit then enter degraded mode.
	time.Sleep(30 * time.Millisecond)

	// Simulate circuit breaker open.
	disp.circuitCh <- circuit.Open

	// Wait for the degraded mode to be picked up by the select loop.
	time.Sleep(30 * time.Millisecond)

	// Record sends AFTER degraded mode is active.
	sendsBefore := len(disp.getSendCalls())
	time.Sleep(60 * time.Millisecond)
	sendsAfter := len(disp.getSendCalls())

	// No new dispatches should have happened during degraded mode.
	if sendsAfter > sendsBefore {
		t.Errorf("expected no dispatches during degraded mode, before=%d after=%d",
			sendsBefore, sendsAfter)
	}

	// But poll was still called (session keepalive).
	if pollCount.Load() < 5 {
		t.Errorf("expected Poll to continue during degraded mode, count=%d", pollCount.Load())
	}

	cancel()
	<-done
}

func TestCommitFailureCounter_ResetsOnSuccess(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	coord.setCommittable(map[int32]int64{0: 100})

	// Fail twice, then succeed.
	var commitAttempt int
	kafka.commitFn = func(map[int32]int64) error {
		commitAttempt++
		if commitAttempt <= 2 {
			return errors.New("temporary failure")
		}
		return nil
	}

	pl, _ := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Wait long enough for commits to cycle, then shut down.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Check after Run returns to avoid racing on internal state.
	// Should not have entered degraded mode (threshold=3, only 2 consecutive failures).
	if pl.degraded.Has(BrokerUnavailable) {
		t.Error("should not have entered degraded mode with only 2 consecutive failures")
	}

	// Counter should be reset after the successful commit.
	if pl.commitFailures != 0 {
		t.Errorf("expected commit failures counter to be 0, got %d", pl.commitFailures)
	}
}

func TestHalfOpen_StaysDegraded(t *testing.T) {
	kafka := newMockKafka()
	disp := newMockDispatcher()
	coord := newMockCoordinator()

	kafka.assignmentFn = func() ([]dispatcher.Partition, error) {
		return []dispatcher.Partition{{Topic: "test", Partition: 0}}, nil
	}

	pl, health := newTestPollLoop(kafka, disp, coord)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pl.Run(ctx)
	}()

	// Simulate an initial partition assignment. In tests the mock kafka
	// never fires rebalance callbacks, so we set the flag directly.
	time.Sleep(10 * time.Millisecond)
	health.SetPartitionsAssigned(true)

	// Circuit opens.
	disp.circuitCh <- circuit.Open
	time.Sleep(20 * time.Millisecond)

	if !pl.degraded.Has(TargetUnavailable) {
		t.Fatal("expected TargetUnavailable")
	}

	// Circuit goes half-open — should remain degraded.
	disp.circuitCh <- circuit.HalfOpen
	time.Sleep(20 * time.Millisecond)

	if !pl.degraded.Has(TargetUnavailable) {
		t.Error("expected TargetUnavailable to remain set during half-open")
	}
	if health.IsReady() {
		t.Error("expected not ready during half-open")
	}

	// Circuit closes — should exit degraded.
	disp.circuitCh <- circuit.Closed
	time.Sleep(20 * time.Millisecond)

	if pl.degraded.Has(TargetUnavailable) {
		t.Error("expected TargetUnavailable to be cleared after circuit closes")
	}
	if !health.IsReady() {
		t.Error("expected ready after circuit closes")
	}

	cancel()
	<-done
}
