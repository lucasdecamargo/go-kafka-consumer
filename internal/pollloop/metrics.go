package pollloop

// Metric name constants for the poll loop. Provisional prefix: kafka_consumer_.
// Final naming convention to be finalized in Phase 5.
const (
	MetricPollLoopMessagesTotal       = "kafka_consumer_poll_loop_messages_total"
	MetricPollLoopPollErrorsTotal     = "kafka_consumer_poll_loop_poll_errors_total"
	MetricPollLoopCommitsTotal        = "kafka_consumer_poll_loop_commits_total"
	MetricPollLoopCommitFailuresTotal = "kafka_consumer_poll_loop_commit_failures_total"
	MetricPollLoopDegradedMode        = "kafka_consumer_poll_loop_degraded_mode"
)
