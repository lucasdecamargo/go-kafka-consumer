// Package metrics defines and registers all Prometheus metrics for the
// Kafka consumer framework. It provides typed metric structs for each
// component (poll loop, dispatcher) with factory functions that handle
// registration against a prometheus.Registerer.
//
// This package is the single source of truth for metric names, labels,
// and help text. The corresponding reference documentation lives in
// docs/metrics.md.
//
// Usage:
//
//	reg := prometheus.NewRegistry()
//	plm := metrics.NewPollLoopMetrics(reg)
//	dm  := metrics.NewDispatcherMetrics(reg)
//
// The poll loop and dispatcher receive these structs via their option
// functions and use them for instrumentation without knowing how
// registration works.
package metrics
