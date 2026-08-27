package workers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"eventmesh/internal/events"
	"eventmesh/internal/metrics"
)

func TestEventWorkerNewEventAcquiresAndProcessesNormally(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	failures := &fakeFailurePublisher{}
	worker := newTestWorker(repository, idempotency, failures)

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	assertOps(t, repository.ops, []string{"processing", "processed"})
	assertOps(t, idempotency.ops, []string{"acquire", "completed"})
	if failures.retryCalls != 0 || failures.dlqCalls != 0 {
		t.Fatalf("expected no failure publications, got retry=%d dlq=%d", failures.retryCalls, failures.dlqCalls)
	}
	if worker.processCalls != 1 {
		t.Fatalf("expected business processing once, got %d", worker.processCalls)
	}
}

func TestEventWorkerCompletedDuplicateDoesNotInvokeBusinessHandler(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyCompleted}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	assertOps(t, idempotency.ops, []string{"acquire"})
	assertOps(t, repository.ops, []string{"processed"})
	if worker.processCalls != 0 {
		t.Fatalf("expected business processing to be skipped, got %d calls", worker.processCalls)
	}
}

func TestEventWorkerCompletedDuplicateCanCommitSafely(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyCompleted}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("expected completed duplicate to return success for Kafka commit, got %v", err)
	}
}

func TestEventWorkerAlreadyProcessingDoesNotInvokeHandler(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAlreadyProcessing}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})

	err := worker.Handle(testContext(), testEvent())
	if !errors.Is(err, ErrAlreadyProcessing) {
		t.Fatalf("expected ErrAlreadyProcessing, got %v", err)
	}
	assertOps(t, idempotency.ops, []string{"acquire"})
	assertOps(t, repository.ops, nil)
	if worker.processCalls != 0 {
		t.Fatalf("expected business processing to be skipped, got %d calls", worker.processCalls)
	}
}

func TestEventWorkerAcquireErrorPreventsBusinessProcessing(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{err: errors.New("redis unavailable")}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})

	err := worker.Handle(testContext(), testEvent())
	if err == nil {
		t.Fatal("expected acquire error")
	}
	assertOps(t, repository.ops, nil)
	if worker.processCalls != 0 {
		t.Fatalf("expected business processing to be skipped, got %d calls", worker.processCalls)
	}
}

func TestEventWorkerHandlerFailureDoesNotMarkCompleted(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	failures := &fakeFailurePublisher{}
	worker := newTestWorker(repository, idempotency, failures)
	worker.processErr = errors.New("business rule failed")

	err := worker.Handle(testContext(), testEvent())
	if err != nil {
		t.Fatalf("expected retry publication to make source commit safe, got %v", err)
	}

	assertOps(t, repository.ops, []string{"processing", "retrying"})
	assertOps(t, idempotency.ops, []string{"acquire", "release"})
	if failures.retryCalls != 1 {
		t.Fatalf("expected one retry publication, got %d", failures.retryCalls)
	}
	if repository.lastError != "business rule failed" {
		t.Fatalf("expected sanitized failure reason, got %q", repository.lastError)
	}
}

func TestEventWorkerSuccessfulHandlerMarksCompleted(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	assertOps(t, idempotency.ops, []string{"acquire", "completed"})
}

func TestEventWorkerMarkCompletedFailurePreventsSuccessfulPath(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{
		result:           events.IdempotencyAcquired,
		markCompletedErr: errors.New("redis write failed"),
	}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})

	err := worker.Handle(testContext(), testEvent())
	if err == nil {
		t.Fatal("expected mark completed error")
	}

	assertOps(t, repository.ops, []string{"processing"})
	assertOps(t, idempotency.ops, []string{"acquire", "completed"})
}

func TestEventWorkerProcessedStateFailureAfterCompletedCanBeReconciledOnRedelivery(t *testing.T) {
	firstRepository := &fakeRepository{markProcessedErr: errors.New("db unavailable")}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	firstWorker := newTestWorker(firstRepository, idempotency, &fakeFailurePublisher{})

	firstErr := firstWorker.Handle(testContext(), testEvent())
	if firstErr == nil {
		t.Fatal("expected first processed state error")
	}
	assertOps(t, idempotency.ops, []string{"acquire", "completed"})
	assertOps(t, firstRepository.ops, []string{"processing", "processed"})

	secondRepository := &fakeRepository{}
	idempotency.ops = nil
	idempotency.result = events.IdempotencyCompleted
	secondWorker := newTestWorker(secondRepository, idempotency, &fakeFailurePublisher{})

	if err := secondWorker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("expected redelivery reconciliation to succeed, got %v", err)
	}
	assertOps(t, secondRepository.ops, []string{"processed"})
	if secondWorker.processCalls != 0 {
		t.Fatalf("expected duplicate redelivery to skip business processing, got %d calls", secondWorker.processCalls)
	}
}

func TestEventWorkerContextCancellationPropagates(t *testing.T) {
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	worker := newTestWorker(repository, idempotency, &fakeFailurePublisher{})
	ctx, cancel := context.WithCancel(testContext())
	cancel()

	err := worker.Handle(ctx, testEvent())
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	assertOps(t, repository.ops, []string{"processing"})
	assertOps(t, idempotency.ops, []string{"acquire"})
}

func TestEventWorkerProcessingLeaseExpirationBehavior(t *testing.T) {
	idempotency := &fakeIdempotency{result: events.IdempotencyAlreadyProcessing}
	worker := newTestWorker(&fakeRepository{}, idempotency, &fakeFailurePublisher{})

	err := worker.Handle(testContext(), testEvent())
	if !errors.Is(err, ErrAlreadyProcessing) {
		t.Fatalf("expected processing conflict while lease exists, got %v", err)
	}

	idempotency.result = events.IdempotencyAcquired
	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("expected processing after simulated lease expiration, got %v", err)
	}
}

func TestEventWorkerRetryablePrimaryFailurePublishesRetryOne(t *testing.T) {
	failures := &fakeFailurePublisher{}
	worker := newTestWorker(&fakeRepository{}, &fakeIdempotency{result: events.IdempotencyAcquired}, failures)
	worker.processErr = errors.New("temporary downstream timeout")

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("handle retryable failure: %v", err)
	}
	if failures.retryCalls != 1 {
		t.Fatalf("expected retry publication, got %d", failures.retryCalls)
	}
	if failures.retryMetadata.RetryStage != 1 || failures.retryMetadata.Attempt != 2 {
		t.Fatalf("expected retry.1 attempt 2, got stage=%d attempt=%d", failures.retryMetadata.RetryStage, failures.retryMetadata.Attempt)
	}
}

func TestEventWorkerRetryPublicationFailurePreventsSourceCommit(t *testing.T) {
	failures := &fakeFailurePublisher{retryErr: errors.New("retry topic unavailable")}
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	worker := newTestWorker(repository, idempotency, failures)
	worker.processErr = errors.New("temporary failure")

	err := worker.Handle(testContext(), testEvent())
	if err == nil {
		t.Fatal("expected retry publish failure")
	}
	assertOps(t, repository.ops, []string{"processing", "retrying"})
	assertOps(t, idempotency.ops, []string{"acquire", "release"})
}

func TestEventWorkerRetryOneFailurePublishesRetryTwo(t *testing.T) {
	assertRetryStage(t, 1, 2, 3)
}

func TestEventWorkerRetryTwoFailurePublishesRetryThree(t *testing.T) {
	assertRetryStage(t, 2, 3, 4)
}

func TestEventWorkerRetryThreeFailurePublishesDLQ(t *testing.T) {
	failures := &fakeFailurePublisher{}
	repository := &fakeRepository{}
	idempotency := &fakeIdempotency{result: events.IdempotencyAcquired}
	worker := newTestWorker(repository, idempotency, failures)
	worker.processErr = errors.New("still failing")

	ctx := events.ContextWithRetryMetadata(testContext(), events.RetryMetadata{
		Attempt:           4,
		RetryStage:        3,
		OriginalTopic:     "eventmesh.events",
		OriginalPartition: 1,
		OriginalOffset:    42,
		FirstFailedAt:     fixedTime,
		LastFailedAt:      fixedTime,
		LastError:         "still failing",
	})

	if err := worker.Handle(ctx, testEvent()); err != nil {
		t.Fatalf("expected dlq path to allow commit, got %v", err)
	}
	if failures.dlqCalls != 1 {
		t.Fatalf("expected DLQ publication, got %d", failures.dlqCalls)
	}
	if failures.dlqMessage.Metadata.Attempt != 4 {
		t.Fatalf("expected DLQ attempt 4, got %d", failures.dlqMessage.Metadata.Attempt)
	}
	assertOps(t, repository.ops, []string{"processing", "dlq"})
	assertOps(t, idempotency.ops, []string{"acquire", "release"})
}

func TestEventWorkerNonRetryableErrorPublishesDirectlyToDLQ(t *testing.T) {
	failures := &fakeFailurePublisher{}
	repository := &fakeRepository{}
	worker := newTestWorker(repository, &fakeIdempotency{result: events.IdempotencyAcquired}, failures)
	worker.processErr = events.NonRetryable(errors.New("unsupported event type"))

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("expected DLQ publication to make source commit safe, got %v", err)
	}
	if failures.retryCalls != 0 || failures.dlqCalls != 1 {
		t.Fatalf("expected direct DLQ, got retry=%d dlq=%d", failures.retryCalls, failures.dlqCalls)
	}
	assertOps(t, repository.ops, []string{"processing", "dlq"})
}

func TestEventWorkerDLQPublicationMarksDBDLQ(t *testing.T) {
	failures := &fakeFailurePublisher{}
	repository := &fakeRepository{}
	worker := newTestWorker(repository, &fakeIdempotency{result: events.IdempotencyAcquired}, failures)
	worker.processErr = events.NonRetryable(errors.New("invalid business data"))

	if err := worker.Handle(testContext(), testEvent()); err != nil {
		t.Fatalf("handle non-retryable: %v", err)
	}
	assertOps(t, repository.ops, []string{"processing", "dlq"})
}

func TestEventWorkerDLQPublishFailurePreventsKafkaCommit(t *testing.T) {
	failures := &fakeFailurePublisher{dlqErr: errors.New("dlq unavailable")}
	repository := &fakeRepository{}
	worker := newTestWorker(repository, &fakeIdempotency{result: events.IdempotencyAcquired}, failures)
	worker.processErr = events.NonRetryable(errors.New("invalid business data"))

	err := worker.Handle(testContext(), testEvent())
	if err == nil {
		t.Fatal("expected DLQ publish error")
	}
	assertOps(t, repository.ops, []string{"processing"})
}

func TestEventWorkerMaximumRetryCountIsEnforced(t *testing.T) {
	failures := &fakeFailurePublisher{}
	worker := newTestWorker(&fakeRepository{}, &fakeIdempotency{result: events.IdempotencyAcquired}, failures)
	worker.processErr = errors.New("exhausted")
	ctx := events.ContextWithRetryMetadata(testContext(), events.RetryMetadata{
		Attempt:           4,
		RetryStage:        3,
		OriginalTopic:     "eventmesh.events",
		OriginalPartition: 1,
		OriginalOffset:    42,
		FirstFailedAt:     fixedTime,
		LastFailedAt:      fixedTime,
	})

	if err := worker.Handle(ctx, testEvent()); err != nil {
		t.Fatalf("handle exhausted retry: %v", err)
	}
	if failures.retryCalls != 0 || failures.dlqCalls != 1 {
		t.Fatalf("expected exhausted retry to go to DLQ, got retry=%d dlq=%d", failures.retryCalls, failures.dlqCalls)
	}
}

type testWorker struct {
	*EventWorker
	processCalls int
	processErr   error
}

func newTestWorker(repository *fakeRepository, idempotency *fakeIdempotency, failures *fakeFailurePublisher) *testWorker {
	tw := &testWorker{}
	tw.EventWorker = NewEventWorker(repository, idempotency, failures, []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}, metrics.NewRecorder(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	tw.EventWorker.now = func() time.Time { return fixedTime }
	tw.EventWorker.process = func(ctx context.Context, _ events.Event) error {
		tw.processCalls++
		if tw.processErr != nil {
			return tw.processErr
		}
		return ctx.Err()
	}
	return tw
}

type fakeRepository struct {
	ops                 []string
	markProcessingCalls int
	lastError           string
	markProcessingErr   error
	markProcessedErr    error
	markFailedErr       error
}

func (f *fakeRepository) Create(context.Context, events.Event) error {
	return nil
}

func (f *fakeRepository) MarkProcessing(context.Context, string) error {
	f.ops = append(f.ops, "processing")
	f.markProcessingCalls++
	return f.markProcessingErr
}

func (f *fakeRepository) MarkProcessed(context.Context, string) error {
	f.ops = append(f.ops, "processed")
	return f.markProcessedErr
}

func (f *fakeRepository) MarkFailed(_ context.Context, _ string, reason string) error {
	f.ops = append(f.ops, "failed")
	f.lastError = reason
	return f.markFailedErr
}

func (f *fakeRepository) MarkRetrying(_ context.Context, _ string, reason string) error {
	f.ops = append(f.ops, "retrying")
	f.lastError = reason
	return nil
}

func (f *fakeRepository) MarkDLQ(_ context.Context, _ string, reason string, _ events.RetryMetadata) error {
	f.ops = append(f.ops, "dlq")
	f.lastError = reason
	return nil
}

func (f *fakeRepository) ListDLQ(context.Context, int, int) ([]events.DLQRecord, error) {
	return nil, nil
}

func (f *fakeRepository) GetDLQ(context.Context, string) (events.DLQRecord, error) {
	return events.DLQRecord{}, events.ErrEventNotFound
}

func (f *fakeRepository) BeginReplay(context.Context, string) (events.DLQRecord, error) {
	return events.DLQRecord{}, events.ErrReplayNotAllowed
}

func (f *fakeRepository) MarkReplayPublished(context.Context, string) error {
	return nil
}

func (f *fakeRepository) MarkReplayFailed(context.Context, string, string) error {
	return nil
}

func (f *fakeRepository) GetByEventID(context.Context, string) (events.Record, error) {
	return events.Record{}, events.ErrEventNotFound
}

type fakeIdempotency struct {
	ops              []string
	result           events.IdempotencyAcquireResult
	err              error
	markCompletedErr error
	releaseErr       error
}

func (f *fakeIdempotency) Acquire(context.Context, string) (events.IdempotencyAcquireResult, error) {
	f.ops = append(f.ops, "acquire")
	if f.err != nil {
		return "", f.err
	}
	return f.result, nil
}

func (f *fakeIdempotency) MarkCompleted(context.Context, string) error {
	f.ops = append(f.ops, "completed")
	return f.markCompletedErr
}

func (f *fakeIdempotency) Release(context.Context, string) error {
	f.ops = append(f.ops, "release")
	return f.releaseErr
}

func (f *fakeIdempotency) IsCompleted(context.Context, string) (bool, error) {
	return f.result == events.IdempotencyCompleted, f.err
}

func (f *fakeIdempotency) State(context.Context, string) (events.IdempotencyState, error) {
	if f.err != nil {
		return "", f.err
	}
	switch f.result {
	case events.IdempotencyCompleted:
		return events.IdempotencyStateCompleted, nil
	case events.IdempotencyAlreadyProcessing:
		return events.IdempotencyStateProcessing, nil
	default:
		return events.IdempotencyStateAbsent, nil
	}
}

type fakeFailurePublisher struct {
	retryCalls    int
	dlqCalls      int
	retryErr      error
	dlqErr        error
	retryMetadata events.RetryMetadata
	dlqMessage    events.DeadLetterMessage
}

func (f *fakeFailurePublisher) PublishRetry(_ context.Context, _ events.Event, metadata events.RetryMetadata) error {
	f.retryCalls++
	f.retryMetadata = metadata
	return f.retryErr
}

func (f *fakeFailurePublisher) PublishDLQ(_ context.Context, message events.DeadLetterMessage) error {
	f.dlqCalls++
	f.dlqMessage = message
	return f.dlqErr
}

func assertRetryStage(t *testing.T, currentStage, wantNextStage, wantAttempt int) {
	t.Helper()

	failures := &fakeFailurePublisher{}
	worker := newTestWorker(&fakeRepository{}, &fakeIdempotency{result: events.IdempotencyAcquired}, failures)
	worker.processErr = errors.New("temporary failure")
	ctx := events.ContextWithRetryMetadata(testContext(), events.RetryMetadata{
		Attempt:           wantAttempt - 1,
		RetryStage:        currentStage,
		OriginalTopic:     "eventmesh.events",
		OriginalPartition: 1,
		OriginalOffset:    42,
		FirstFailedAt:     fixedTime,
		LastFailedAt:      fixedTime,
		LastError:         "temporary failure",
	})

	if err := worker.Handle(ctx, testEvent()); err != nil {
		t.Fatalf("handle retry stage %d: %v", currentStage, err)
	}
	if failures.retryCalls != 1 {
		t.Fatalf("expected retry publication, got %d", failures.retryCalls)
	}
	if failures.retryMetadata.RetryStage != wantNextStage || failures.retryMetadata.Attempt != wantAttempt {
		t.Fatalf("expected stage=%d attempt=%d, got stage=%d attempt=%d", wantNextStage, wantAttempt, failures.retryMetadata.RetryStage, failures.retryMetadata.Attempt)
	}
}

var fixedTime = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

func testContext() context.Context {
	return events.ContextWithMetadata(context.Background(), events.Metadata{
		Topic:     "eventmesh.events",
		Partition: 1,
		Offset:    42,
	})
}

func testEvent() events.Event {
	return events.Event{
		EventID:   "018fc1e0-b5d1-4f5e-9e39-70b6f1f05262",
		Type:      "order.created",
		Source:    "order-service",
		Payload:   json.RawMessage(`{"order_id":"ORD-123"}`),
		CreatedAt: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
	}
}

func assertOps(t *testing.T, got []string, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected ops %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected ops %v, got %v", want, got)
		}
	}
}
