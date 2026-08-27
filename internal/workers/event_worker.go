package workers

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"eventmesh/internal/events"
	"eventmesh/internal/metrics"
	"eventmesh/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var ErrAlreadyProcessing = errors.New("event is already being processed")

type EventWorker struct {
	repository       events.EventRepository
	idempotency      events.IdempotencyStore
	failurePublisher events.FailurePublisher
	retryDelays      []time.Duration
	metrics          *metrics.Recorder
	log              *slog.Logger
	process          func(context.Context, events.Event) error
	now              func() time.Time
}

func NewEventWorker(
	repository events.EventRepository,
	idempotency events.IdempotencyStore,
	failurePublisher events.FailurePublisher,
	retryDelays []time.Duration,
	recorder *metrics.Recorder,
	log *slog.Logger,
) *EventWorker {
	if recorder == nil {
		recorder = metrics.NewRecorder()
	}
	return &EventWorker{
		repository:       repository,
		idempotency:      idempotency,
		failurePublisher: failurePublisher,
		retryDelays:      retryDelays,
		metrics:          recorder,
		log:              log,
		process:          processEvent,
		now:              time.Now,
	}
}

func (w *EventWorker) Handle(ctx context.Context, event events.Event) error {
	started := time.Now()
	metadata, _ := events.MetadataFromContext(ctx)
	retryMetadata, _ := events.RetryMetadataFromContext(ctx)
	ctx, span := observability.Tracer().Start(
		ctx,
		"eventmesh.process_event",
		trace.WithAttributes(
			attribute.String("event.type", event.Type),
			attribute.String("event.source", event.Source),
			attribute.String("messaging.destination.name", metadata.Topic),
			attribute.Int("messaging.kafka.partition", metadata.Partition),
			attribute.Int("eventmesh.attempt", retryMetadata.Attempt),
			attribute.Int("eventmesh.retry_stage", retryMetadata.RetryStage),
		),
	)
	defer span.End()
	log := observability.WithTrace(ctx, w.log)

	acquireResult, err := w.idempotency.Acquire(ctx, event.EventID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "idempotency acquire failed")
		w.metrics.RecordIdempotencyError()
		log.Error(
			"idempotency_failed",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return err
	}

	switch acquireResult {
	case events.IdempotencyCompleted:
		w.metrics.RecordIdempotencyDuplicate()
		span.AddEvent("duplicate_skipped")
		log.Info(
			"duplicate_event_skipped",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
		)
		if err := w.repository.MarkProcessed(ctx, event.EventID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "mark processed failed")
			log.Error(
				"idempotency_failed",
				"event_id", event.EventID,
				"event_type", event.Type,
				"source", event.Source,
				"topic", metadata.Topic,
				"partition", metadata.Partition,
				"offset", metadata.Offset,
				"duration_ms", time.Since(started).Milliseconds(),
				"error", err,
			)
			return err
		}
		return nil
	case events.IdempotencyAlreadyProcessing:
		w.metrics.RecordIdempotencyProcessingConflict()
		span.AddEvent("idempotency_conflict")
		log.Info(
			"idempotency_processing_conflict",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
		)
		return ErrAlreadyProcessing
	case events.IdempotencyAcquired:
		w.metrics.RecordIdempotencyAcquired()
		log.Info(
			"idempotency_acquired",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	default:
		w.metrics.RecordIdempotencyError()
		return errors.New("unknown idempotency acquire result")
	}

	if err := w.repository.MarkProcessing(ctx, event.EventID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "mark processing failed")
		log.Error(
			"event_processing_failed",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return err
	}

	log.Info(
		"event_processing_started",
		"event_id", event.EventID,
		"event_type", event.Type,
		"source", event.Source,
		"topic", metadata.Topic,
		"partition", metadata.Partition,
		"offset", metadata.Offset,
	)

	if err := w.process(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "event processing failed")
		log.Error(
			"event_processing_failed",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return w.handleProcessingFailure(ctx, event, err, metadata, started)
	}

	if err := w.idempotency.MarkCompleted(ctx, event.EventID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "idempotency complete failed")
		w.metrics.RecordIdempotencyError()
		log.Error(
			"idempotency_failed",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return err
	}
	log.Info(
		"idempotency_completed",
		"event_id", event.EventID,
		"event_type", event.Type,
		"source", event.Source,
		"topic", metadata.Topic,
		"partition", metadata.Partition,
		"offset", metadata.Offset,
		"duration_ms", time.Since(started).Milliseconds(),
	)

	if err := w.repository.MarkProcessed(ctx, event.EventID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "mark processed failed")
		log.Error(
			"event_processing_failed",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", metadata.Topic,
			"partition", metadata.Partition,
			"offset", metadata.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return err
	}

	log.Info(
		"event_processing_succeeded",
		"event_id", event.EventID,
		"event_type", event.Type,
		"source", event.Source,
		"topic", metadata.Topic,
		"partition", metadata.Partition,
		"offset", metadata.Offset,
		"duration_ms", time.Since(started).Milliseconds(),
	)

	return nil
}

func (w *EventWorker) handleProcessingFailure(ctx context.Context, event events.Event, processErr error, metadata events.Metadata, started time.Time) error {
	if w.failurePublisher == nil {
		return errors.New("failure publisher is not configured")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	kind := events.ClassifyFailure(processErr)
	reason := sanitizeReason(processErr)
	retryMetadata := w.nextRetryMetadata(ctx, metadata, reason)

	if kind == events.FailureRetryable && retryMetadata.RetryStage <= len(w.retryDelays) {
		if err := w.repository.MarkRetrying(ctx, event.EventID, reason); err != nil {
			return err
		}
		if err := w.idempotency.Release(ctx, event.EventID); err != nil {
			w.metrics.RecordIdempotencyError()
			trace.SpanFromContext(ctx).RecordError(err)
			trace.SpanFromContext(ctx).SetStatus(codes.Error, "idempotency release failed")
			observability.WithTrace(ctx, w.log).Error("idempotency_failed", "event_id", event.EventID, "event_type", event.Type, "duration_ms", time.Since(started).Milliseconds(), "error", err)
			return err
		}
		if err := w.failurePublisher.PublishRetry(ctx, event, retryMetadata); err != nil {
			w.metrics.RecordRetryPublishFailed()
			trace.SpanFromContext(ctx).RecordError(err)
			trace.SpanFromContext(ctx).SetStatus(codes.Error, "retry publish failed")
			observability.WithTrace(ctx, w.log).Error("retry_publish_failed", "event_id", event.EventID, "event_type", event.Type, "attempt", retryMetadata.Attempt, "retry_stage", retryMetadata.RetryStage, "delay", w.retryDelays[retryMetadata.RetryStage-1].String(), "error", err)
			return err
		}
		w.metrics.RecordRetryPublished()
		trace.SpanFromContext(ctx).AddEvent("retry_scheduled", trace.WithAttributes(attribute.Int("eventmesh.retry_stage", retryMetadata.RetryStage)))
		observability.WithTrace(ctx, w.log).Info("retry_scheduled", "event_id", event.EventID, "event_type", event.Type, "attempt", retryMetadata.Attempt, "retry_stage", retryMetadata.RetryStage, "delay", w.retryDelays[retryMetadata.RetryStage-1].String())
		return nil
	}

	dlqMetadata := w.failureMetadata(ctx, metadata, reason)
	if kind == events.FailureRetryable {
		w.metrics.RecordRetryExhausted()
		observability.WithTrace(ctx, w.log).Info("retry_exhausted", "event_id", event.EventID, "event_type", event.Type, "attempt", dlqMetadata.Attempt, "retry_stage", dlqMetadata.RetryStage)
	}

	dlqMessage := events.DeadLetterMessage{
		Event: &event,
		Failure: events.FailureDetail{
			Kind:   kind,
			Reason: reason,
		},
		Metadata: dlqMetadata,
	}
	if err := w.failurePublisher.PublishDLQ(ctx, dlqMessage); err != nil {
		w.metrics.RecordDLQPublishFailed()
		trace.SpanFromContext(ctx).RecordError(err)
		trace.SpanFromContext(ctx).SetStatus(codes.Error, "dlq publish failed")
		observability.WithTrace(ctx, w.log).Error("dlq_publish_failed", "event_id", event.EventID, "event_type", event.Type, "attempt", dlqMetadata.Attempt, "retry_stage", dlqMetadata.RetryStage, "error", err)
		return err
	}
	w.metrics.RecordDLQPublished()
	trace.SpanFromContext(ctx).AddEvent("dlq_routed", trace.WithAttributes(attribute.Int("eventmesh.retry_stage", dlqMetadata.RetryStage)))
	observability.WithTrace(ctx, w.log).Info("dlq_published", "event_id", event.EventID, "event_type", event.Type, "attempt", dlqMetadata.Attempt, "retry_stage", dlqMetadata.RetryStage)

	if err := w.repository.MarkDLQ(ctx, event.EventID, reason, dlqMetadata); err != nil {
		return err
	}
	if err := w.idempotency.Release(ctx, event.EventID); err != nil {
		w.metrics.RecordIdempotencyError()
		trace.SpanFromContext(ctx).RecordError(err)
		trace.SpanFromContext(ctx).SetStatus(codes.Error, "idempotency release failed")
		observability.WithTrace(ctx, w.log).Error("idempotency_failed", "event_id", event.EventID, "event_type", event.Type, "duration_ms", time.Since(started).Milliseconds(), "error", err)
		return err
	}
	return nil
}

func (w *EventWorker) nextRetryMetadata(ctx context.Context, source events.Metadata, reason string) events.RetryMetadata {
	current := w.failureMetadata(ctx, source, reason)
	nextStage := current.RetryStage + 1
	notBefore := time.Time{}
	if nextStage >= 1 && nextStage <= len(w.retryDelays) {
		notBefore = current.LastFailedAt.Add(w.retryDelays[nextStage-1])
	}

	current.Attempt++
	current.RetryStage = nextStage
	current.NotBefore = notBefore
	return current
}

func (w *EventWorker) failureMetadata(ctx context.Context, source events.Metadata, reason string) events.RetryMetadata {
	now := w.now().UTC()
	current, ok := events.RetryMetadataFromContext(ctx)
	if !ok || current.Attempt == 0 {
		current = events.RetryMetadata{
			Attempt:           1,
			RetryStage:        0,
			OriginalTopic:     source.Topic,
			OriginalPartition: source.Partition,
			OriginalOffset:    source.Offset,
		}
	}
	firstFailedAt := current.FirstFailedAt
	if firstFailedAt.IsZero() {
		firstFailedAt = now
	}

	return events.RetryMetadata{
		Attempt:           current.Attempt,
		RetryStage:        current.RetryStage,
		OriginalTopic:     current.OriginalTopic,
		OriginalPartition: current.OriginalPartition,
		OriginalOffset:    current.OriginalOffset,
		FirstFailedAt:     firstFailedAt,
		LastFailedAt:      now,
		LastError:         reason,
	}
}

func processEvent(ctx context.Context, event events.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !devFailuresEnabled() {
		return nil
	}

	metadata, _ := events.RetryMetadataFromContext(ctx)
	attempt := metadata.Attempt
	if attempt == 0 {
		attempt = 1
	}

	switch event.Type {
	case "test.retry_twice":
		if attempt < 3 {
			return events.Retryable(errors.New("development retry_twice simulated retryable failure"))
		}
	case "test.permanent_failure":
		if !devPermanentFailureRecovered() {
			return events.NonRetryable(errors.New("development permanent_failure simulated non-retryable failure"))
		}
	case "test.slow_processing":
		delay := devSlowDuration()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func devFailuresEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("EVENTMESH_DEV_FAILURES_ENABLED")), "true")
}

func devPermanentFailureRecovered() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("EVENTMESH_DEV_PERMANENT_FAILURE_RECOVERED")), "true")
}

func devSlowDuration() time.Duration {
	value := strings.TrimSpace(os.Getenv("EVENTMESH_DEV_SLOW_DURATION"))
	if value == "" {
		return 10 * time.Second
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 10 * time.Second
	}
	return duration
}

func sanitizeReason(err error) string {
	if err == nil {
		return "event processing failed"
	}
	message := strings.TrimSpace(err.Error())
	if message == "" {
		return "event processing failed"
	}
	if errors.Is(err, context.Canceled) {
		return "processing canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "processing deadline exceeded"
	}
	return message
}
