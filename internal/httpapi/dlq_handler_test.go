package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"eventmesh/internal/events"
	"eventmesh/internal/metrics"
)

func TestDLQListReturnsOnlyRepositoryDLQEvents(t *testing.T) {
	repository := &fakeRepository{dlqRecords: []events.DLQRecord{testDLQRecord()}}
	rec := serveDLQ(t, newTestDLQHandler(repository, &fakeReplayer{}, &fakeIdempotencyStore{}), http.MethodGet, "/v1/admin/dlq?limit=20&offset=0")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	var response dlqListResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(response.Items))
	}
	if response.Items[0].EventID != "evt-dlq-1" {
		t.Fatalf("expected dlq event id, got %q", response.Items[0].EventID)
	}
}

func TestDLQListPaginationValidation(t *testing.T) {
	rec := serveDLQ(t, newTestDLQHandler(&fakeRepository{}, &fakeReplayer{}, &fakeIdempotencyStore{}), http.MethodGet, "/v1/admin/dlq?limit=500")

	assertJSONError(t, rec, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100")
}

func TestDLQGetSpecificEvent(t *testing.T) {
	repository := &fakeRepository{dlqRecord: testDLQRecord()}
	rec := serveDLQ(t, newTestDLQHandler(repository, &fakeReplayer{}, &fakeIdempotencyStore{}), http.MethodGet, "/v1/admin/dlq/evt-dlq-1")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	var response events.DLQRecord
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.EventID != "evt-dlq-1" {
		t.Fatalf("expected event id evt-dlq-1, got %q", response.EventID)
	}
	if len(response.Payload) == 0 {
		t.Fatal("expected detailed DLQ response to include payload")
	}
}

func TestDLQGetUnknownReturnsNotFound(t *testing.T) {
	repository := &fakeRepository{getDLQErr: events.ErrEventNotFound}
	rec := serveDLQ(t, newTestDLQHandler(repository, &fakeReplayer{}, &fakeIdempotencyStore{}), http.MethodGet, "/v1/admin/dlq/missing")

	assertJSONError(t, rec, http.StatusNotFound, "dlq_event_not_found", "DLQ event not found")
}

func TestDLQReplayPublishesToMainTopic(t *testing.T) {
	replayer := &fakeReplayer{}
	repository := replayableRepository()
	rec := serveDLQ(t, newTestDLQHandler(repository, replayer, &fakeIdempotencyStore{}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
	if replayer.calls != 1 {
		t.Fatalf("expected one replay publish, got %d", replayer.calls)
	}
	if replayer.event.EventID != "evt-dlq-1" {
		t.Fatalf("expected replay to preserve event id, got %q", replayer.event.EventID)
	}
	if repository.markReplayPublishedCalls != 1 {
		t.Fatalf("expected replay published state update, got %d", repository.markReplayPublishedCalls)
	}
}

func TestDLQReplayFromNonDLQRejected(t *testing.T) {
	repository := replayableRepository()
	repository.record.Status = events.StatusProcessed
	rec := serveDLQ(t, newTestDLQHandler(repository, &fakeReplayer{}, &fakeIdempotencyStore{}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	assertJSONError(t, rec, http.StatusConflict, "dlq_replay_conflict", "event is not in DLQ status")
}

func TestDLQReplayRejectsCompletedRedisState(t *testing.T) {
	rec := serveDLQ(t, newTestDLQHandler(replayableRepository(), &fakeReplayer{}, &fakeIdempotencyStore{state: events.IdempotencyStateCompleted}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	assertJSONError(t, rec, http.StatusConflict, "dlq_replay_conflict", "event is marked completed in idempotency store")
}

func TestDLQReplayRejectsProcessingRedisState(t *testing.T) {
	rec := serveDLQ(t, newTestDLQHandler(replayableRepository(), &fakeReplayer{}, &fakeIdempotencyStore{state: events.IdempotencyStateProcessing}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	assertJSONError(t, rec, http.StatusConflict, "dlq_replay_conflict", "event is currently processing")
}

func TestDLQReplayAbsentRedisStateAllowsReplay(t *testing.T) {
	replayer := &fakeReplayer{}
	rec := serveDLQ(t, newTestDLQHandler(replayableRepository(), replayer, &fakeIdempotencyStore{state: events.IdempotencyStateAbsent}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
	if replayer.calls != 1 {
		t.Fatalf("expected replay publish, got %d", replayer.calls)
	}
}

func TestDLQReplayPublishFailureIsRecorded(t *testing.T) {
	repository := replayableRepository()
	replayer := &fakeReplayer{err: errors.New("kafka unavailable")}
	rec := serveDLQ(t, newTestDLQHandler(repository, replayer, &fakeIdempotencyStore{}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	assertJSONError(t, rec, http.StatusServiceUnavailable, "dlq_replay_publish_failed", "DLQ event could not be replayed")
	if repository.markReplayFailedCalls != 1 {
		t.Fatalf("expected replay failure state update, got %d", repository.markReplayFailedCalls)
	}
	if repository.markReplayPublishedCalls != 0 {
		t.Fatal("expected replay not to be marked published")
	}
}

func TestDLQReplayRepositoryFailureReturns5xx(t *testing.T) {
	repository := replayableRepository()
	repository.beginReplayErr = errors.New("db unavailable")
	rec := serveDLQ(t, newTestDLQHandler(repository, &fakeReplayer{}, &fakeIdempotencyStore{}), http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay")

	assertJSONError(t, rec, http.StatusInternalServerError, "dlq_replay_failed", "DLQ event could not be replayed")
}

func TestDLQReplayRequestCancellationPropagates(t *testing.T) {
	replayer := &fakeReplayer{useContextErr: true}
	handler := newTestDLQHandler(replayableRepository(), replayer, &fakeIdempotencyStore{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assertJSONError(t, rec, http.StatusServiceUnavailable, "dlq_replay_publish_failed", "DLQ event could not be replayed")
}

func TestConcurrentDoubleReplayOnlyPublishesOnce(t *testing.T) {
	repository := &concurrentReplayRepository{record: replayableRepository().record, dlqRecord: testDLQRecord()}
	replayer := &fakeReplayer{}
	handler := newTestDLQHandler(repository, replayer, &fakeIdempotencyStore{})

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/dlq/evt-dlq-1/replay", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		}()
	}
	wg.Wait()

	if replayer.calls != 1 {
		t.Fatalf("expected exactly one replay publish, got %d", replayer.calls)
	}
}

func newTestDLQHandler(repository events.EventRepository, replayer events.EventReplayer, idempotency events.IdempotencyStore) http.Handler {
	handler := NewDLQHandler(repository, replayer, idempotency, metrics.NewRecorder(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	handler.now = func() time.Time { return fixedTime }
	return RequestIDMiddleware(handler)
}

func serveDLQ(t *testing.T, handler http.Handler, method string, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

type fakeReplayer struct {
	mu            sync.Mutex
	calls         int
	event         events.Event
	metadata      events.ReplayMetadata
	err           error
	useContextErr bool
}

func (f *fakeReplayer) Replay(ctx context.Context, event events.Event, metadata events.ReplayMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.event = event
	f.metadata = metadata
	if f.useContextErr {
		return ctx.Err()
	}
	return f.err
}

type fakeIdempotencyStore struct {
	state events.IdempotencyState
	err   error
}

func (f *fakeIdempotencyStore) Acquire(context.Context, string) (events.IdempotencyAcquireResult, error) {
	return events.IdempotencyAcquired, nil
}

func (f *fakeIdempotencyStore) MarkCompleted(context.Context, string) error {
	return nil
}

func (f *fakeIdempotencyStore) Release(context.Context, string) error {
	return nil
}

func (f *fakeIdempotencyStore) IsCompleted(context.Context, string) (bool, error) {
	return f.state == events.IdempotencyStateCompleted, f.err
}

func (f *fakeIdempotencyStore) State(context.Context, string) (events.IdempotencyState, error) {
	if f.err != nil {
		return "", f.err
	}
	if f.state == "" {
		return events.IdempotencyStateAbsent, nil
	}
	return f.state, nil
}

type concurrentReplayRepository struct {
	mu        sync.Mutex
	record    events.Record
	dlqRecord events.DLQRecord
	pending   bool
}

func (r *concurrentReplayRepository) Create(context.Context, events.Event) error         { return nil }
func (r *concurrentReplayRepository) MarkProcessing(context.Context, string) error       { return nil }
func (r *concurrentReplayRepository) MarkProcessed(context.Context, string) error        { return nil }
func (r *concurrentReplayRepository) MarkFailed(context.Context, string, string) error   { return nil }
func (r *concurrentReplayRepository) MarkRetrying(context.Context, string, string) error { return nil }
func (r *concurrentReplayRepository) MarkDLQ(context.Context, string, string, events.RetryMetadata) error {
	return nil
}
func (r *concurrentReplayRepository) ListDLQ(context.Context, int, int) ([]events.DLQRecord, error) {
	return nil, nil
}
func (r *concurrentReplayRepository) GetDLQ(context.Context, string) (events.DLQRecord, error) {
	return r.dlqRecord, nil
}
func (r *concurrentReplayRepository) GetByEventID(context.Context, string) (events.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending {
		record := r.record
		record.Status = events.StatusReplayPending
		return record, nil
	}
	return r.record, nil
}
func (r *concurrentReplayRepository) BeginReplay(context.Context, string) (events.DLQRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending {
		return events.DLQRecord{}, events.ErrReplayNotAllowed
	}
	r.pending = true
	return r.dlqRecord, nil
}
func (r *concurrentReplayRepository) MarkReplayPublished(context.Context, string) error { return nil }
func (r *concurrentReplayRepository) MarkReplayFailed(context.Context, string, string) error {
	return nil
}

func replayableRepository() *fakeRepository {
	record := testDLQRecord()
	return &fakeRepository{
		record: events.Record{
			EventID:      record.EventID,
			Type:         record.Type,
			Source:       record.Source,
			Status:       events.StatusDLQ,
			AttemptCount: record.AttemptCount,
			CreatedAt:    record.CreatedAt,
			UpdatedAt:    record.UpdatedAt,
		},
		dlqRecord: record,
	}
}

func testDLQRecord() events.DLQRecord {
	partition := 1
	offset := int64(42)
	dlqAt := fixedTime.Add(time.Minute)
	return events.DLQRecord{
		EventID:           "evt-dlq-1",
		Type:              "order.created",
		Source:            "order-service",
		Payload:           json.RawMessage(`{"order_id":"ORD-123"}`),
		Status:            events.StatusDLQ,
		AttemptCount:      4,
		LastError:         "downstream timeout",
		DLQAt:             &dlqAt,
		OriginalTopic:     "eventmesh.events",
		OriginalPartition: &partition,
		OriginalOffset:    &offset,
		CreatedAt:         fixedTime,
		UpdatedAt:         dlqAt,
	}
}
