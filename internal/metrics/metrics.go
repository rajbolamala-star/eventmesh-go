package metrics

import (
	"net/http"
	"sync/atomic"
	"time"
)

var eventsConsumed uint64
var eventsProcessed uint64
var eventsProcessingFailed uint64
var eventProcessingDurationMicros uint64
var idempotencyAcquired uint64
var idempotencyDuplicate uint64
var idempotencyProcessingConflict uint64
var idempotencyErrors uint64
var retriesPublished uint64
var retryProcessing uint64
var retryExhausted uint64
var dlqPublished uint64
var dlqPublishFailed uint64
var retryPublishFailed uint64
var dlqReplayRequested uint64
var dlqReplaySuccess uint64
var dlqReplayFailed uint64
var dlqReplayRejected uint64

type Recorder struct{}

func NewRecorder() *Recorder {
	return &Recorder{}
}

func (r *Recorder) RecordEventReceived() {
	r.RecordEventConsumed()
}

func (r *Recorder) RecordEventConsumed() {
	atomic.AddUint64(&eventsConsumed, 1)
}

func (r *Recorder) RecordEventProcessed(duration time.Duration) {
	atomic.AddUint64(&eventsProcessed, 1)
	atomic.AddUint64(&eventProcessingDurationMicros, uint64(duration.Microseconds()))
}

func (r *Recorder) RecordEventProcessingFailed(duration time.Duration) {
	atomic.AddUint64(&eventsProcessingFailed, 1)
	atomic.AddUint64(&eventProcessingDurationMicros, uint64(duration.Microseconds()))
}

func (r *Recorder) RecordIdempotencyAcquired() {
	atomic.AddUint64(&idempotencyAcquired, 1)
}

func (r *Recorder) RecordIdempotencyDuplicate() {
	atomic.AddUint64(&idempotencyDuplicate, 1)
}

func (r *Recorder) RecordIdempotencyProcessingConflict() {
	atomic.AddUint64(&idempotencyProcessingConflict, 1)
}

func (r *Recorder) RecordIdempotencyError() {
	atomic.AddUint64(&idempotencyErrors, 1)
}

func (r *Recorder) RecordRetryPublished() {
	atomic.AddUint64(&retriesPublished, 1)
}

func (r *Recorder) RecordRetryProcessing() {
	atomic.AddUint64(&retryProcessing, 1)
}

func (r *Recorder) RecordRetryExhausted() {
	atomic.AddUint64(&retryExhausted, 1)
}

func (r *Recorder) RecordDLQPublished() {
	atomic.AddUint64(&dlqPublished, 1)
}

func (r *Recorder) RecordDLQPublishFailed() {
	atomic.AddUint64(&dlqPublishFailed, 1)
}

func (r *Recorder) RecordRetryPublishFailed() {
	atomic.AddUint64(&retryPublishFailed, 1)
}

func (r *Recorder) RecordDLQReplayRequested() {
	atomic.AddUint64(&dlqReplayRequested, 1)
}

func (r *Recorder) RecordDLQReplaySuccess() {
	atomic.AddUint64(&dlqReplaySuccess, 1)
}

func (r *Recorder) RecordDLQReplayFailed() {
	atomic.AddUint64(&dlqReplayFailed, 1)
}

func (r *Recorder) RecordDLQReplayRejected() {
	atomic.AddUint64(&dlqReplayRejected, 1)
}

func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		writeMetric(w, "eventmesh_events_consumed_total", "Total Kafka events consumed by workers.", atomic.LoadUint64(&eventsConsumed))
		writeMetric(w, "eventmesh_events_processed_total", "Total Kafka events processed successfully by workers.", atomic.LoadUint64(&eventsProcessed))
		writeMetric(w, "eventmesh_events_processing_failed_total", "Total Kafka events that failed processing.", atomic.LoadUint64(&eventsProcessingFailed))
		writeMetric(w, "eventmesh_idempotency_acquired_total", "Total events that acquired an idempotency processing lease.", atomic.LoadUint64(&idempotencyAcquired))
		writeMetric(w, "eventmesh_idempotency_duplicate_total", "Total completed duplicate events skipped by idempotency.", atomic.LoadUint64(&idempotencyDuplicate))
		writeMetric(w, "eventmesh_idempotency_processing_conflict_total", "Total events skipped because another worker is processing them.", atomic.LoadUint64(&idempotencyProcessingConflict))
		writeMetric(w, "eventmesh_idempotency_errors_total", "Total idempotency store errors.", atomic.LoadUint64(&idempotencyErrors))
		writeMetric(w, "eventmesh_retries_published_total", "Total retry messages published.", atomic.LoadUint64(&retriesPublished))
		writeMetric(w, "eventmesh_retry_processing_total", "Total retry messages processed.", atomic.LoadUint64(&retryProcessing))
		writeMetric(w, "eventmesh_retry_exhausted_total", "Total events whose retry attempts were exhausted.", atomic.LoadUint64(&retryExhausted))
		writeMetric(w, "eventmesh_dlq_published_total", "Total messages published to the DLQ.", atomic.LoadUint64(&dlqPublished))
		writeMetric(w, "eventmesh_dlq_publish_failed_total", "Total DLQ publish failures.", atomic.LoadUint64(&dlqPublishFailed))
		writeMetric(w, "eventmesh_retry_publish_failed_total", "Total retry publish failures.", atomic.LoadUint64(&retryPublishFailed))
		writeMetric(w, "eventmesh_dlq_replay_requested_total", "Total DLQ replay requests.", atomic.LoadUint64(&dlqReplayRequested))
		writeMetric(w, "eventmesh_dlq_replay_success_total", "Total successful DLQ replay publications.", atomic.LoadUint64(&dlqReplaySuccess))
		writeMetric(w, "eventmesh_dlq_replay_failed_total", "Total failed DLQ replay attempts.", atomic.LoadUint64(&dlqReplayFailed))
		writeMetric(w, "eventmesh_dlq_replay_rejected_total", "Total rejected DLQ replay attempts.", atomic.LoadUint64(&dlqReplayRejected))

		_, _ = w.Write([]byte("# HELP eventmesh_event_processing_duration_seconds Total event processing duration in seconds.\n"))
		_, _ = w.Write([]byte("# TYPE eventmesh_event_processing_duration_seconds counter\n"))
		_, _ = w.Write([]byte("eventmesh_event_processing_duration_seconds " + formatDurationSeconds(atomic.LoadUint64(&eventProcessingDurationMicros)) + "\n"))
	})
}

func writeMetric(w http.ResponseWriter, name, help string, value uint64) {
	_, _ = w.Write([]byte("# HELP " + name + " " + help + "\n"))
	_, _ = w.Write([]byte("# TYPE " + name + " counter\n"))
	_, _ = w.Write([]byte(name + " " + formatUint(value) + "\n"))
}

func formatUint(value uint64) string {
	if value == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}

func formatDurationSeconds(micros uint64) string {
	whole := micros / 1_000_000
	fraction := micros % 1_000_000
	if fraction == 0 {
		return formatUint(whole)
	}
	return formatUint(whole) + "." + formatFixedSix(fraction)
}

func formatFixedSix(value uint64) string {
	var buf [6]byte
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[:])
}
