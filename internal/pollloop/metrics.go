package pollloop

// Metric name constants for the poll loop. All metrics follow the naming
// convention: kafka_consumer_poll_loop_<metric>_<unit>.
const (
	MetricPollLoopMessagesTotal       = "kafka_consumer_poll_loop_messages_total"
	MetricPollLoopPollErrorsTotal     = "kafka_consumer_poll_loop_poll_errors_total"
	MetricPollLoopCommitsTotal        = "kafka_consumer_poll_loop_commits_total"
	MetricPollLoopCommitFailuresTotal = "kafka_consumer_poll_loop_commit_failures_total"
	MetricPollLoopDegradedMode        = "kafka_consumer_poll_loop_degraded_mode"
)
