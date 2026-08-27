package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"eventmesh/internal/events"
)

func TestEventsHandlerValidEventReturnsAccepted(t *testing.T) {
	publisher := &fakePublisher{}
	repository := &fakeRepository{}
	handler := newTestHandler(publisher, repository)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{
		"type": "order.created",
		"source": "order-service",
		"payload": {
			"order_id": "ORD-123",
			"customer_id": "CUST-1001",
			"amount": 149.99
		}
	}`))
	req.Header.Set("X-Request-ID", "req-123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "req-123" {
		t.Fatalf("expected request id header to be reused, got %q", got)
	}

	var response map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["event_id"] == "" {
		t.Fatal("expected generated event_id in response")
	}
	if response["status"] != "accepted" {
		t.Fatalf("expected accepted status, got %q", response["status"])
	}

	if repository.creates != 1 {
		t.Fatalf("expected repository create once, got %d", repository.creates)
	}
	if repository.created.Status != "" {
		t.Fatalf("expected created fake event status to be empty domain event, got %q", repository.created.Status)
	}
	if publisher.calls != 1 {
		t.Fatalf("expected publisher to be called once, got %d", publisher.calls)
	}
	if publisher.event.EventID == "" {
		t.Fatal("expected publisher event_id to be populated")
	}
	if publisher.event.Type != "order.created" {
		t.Fatalf("expected event type order.created, got %q", publisher.event.Type)
	}
	if publisher.event.Source != "order-service" {
		t.Fatalf("expected source order-service, got %q", publisher.event.Source)
	}
	if publisher.event.CreatedAt.IsZero() {
		t.Fatal("expected created_at to be populated")
	}
	if publisher.event.CreatedAt != fixedTime {
		t.Fatalf("expected created_at %s, got %s", fixedTime, publisher.event.CreatedAt)
	}
	if !publisher.hadDeadline {
		t.Fatal("expected publisher context to include a deadline")
	}

	var payload map[string]any
	if err := json.Unmarshal(publisher.event.Payload, &payload); err != nil {
		t.Fatalf("decode published payload: %v", err)
	}
	if payload["order_id"] != "ORD-123" {
		t.Fatalf("expected payload order_id ORD-123, got %v", payload["order_id"])
	}
}

func TestEventsHandlerSuccessfulPostPersistsReceived(t *testing.T) {
	publisher := &fakePublisher{}
	repository := &fakeRepository{}
	rec := serveEvent(t, publisher, repository, `{"type":"order.created","source":"order-service","payload":{}}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
	if repository.creates != 1 {
		t.Fatalf("expected repository create once, got %d", repository.creates)
	}
	if repository.created.EventID == "" {
		t.Fatal("expected persisted event_id")
	}
	if publisher.calls != 1 {
		t.Fatalf("expected publisher call after persistence, got %d", publisher.calls)
	}
}

func TestEventsHandlerDBFailurePreventsKafkaPublish(t *testing.T) {
	publisher := &fakePublisher{}
	repository := &fakeRepository{createErr: errors.New("db unavailable")}
	rec := serveEvent(t, publisher, repository, `{"type":"order.created","source":"order-service","payload":{}}`)

	assertJSONError(t, rec, http.StatusInternalServerError, "event_persistence_failed", "event could not be accepted")
	if publisher.calls != 0 {
		t.Fatalf("expected kafka publish to be skipped, got %d calls", publisher.calls)
	}
}

func TestEventsHandlerMissingTypeReturnsBadRequest(t *testing.T) {
	rec := serveEvent(t, &fakePublisher{}, &fakeRepository{}, `{"source":"order-service","payload":{}}`)

	assertJSONError(t, rec, http.StatusBadRequest, "invalid_request", "type is required")
}

func TestEventsHandlerMissingSourceReturnsBadRequest(t *testing.T) {
	rec := serveEvent(t, &fakePublisher{}, &fakeRepository{}, `{"type":"order.created","payload":{}}`)

	assertJSONError(t, rec, http.StatusBadRequest, "invalid_request", "source is required")
}

func TestEventsHandlerInvalidJSONReturnsBadRequest(t *testing.T) {
	rec := serveEvent(t, &fakePublisher{}, &fakeRepository{}, `{"type":`)

	assertJSONError(t, rec, http.StatusBadRequest, "invalid_request", "invalid JSON request body")
}

func TestEventsHandlerPublishFailureReturnsServiceUnavailable(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	repository := &fakeRepository{}
	rec := serveEvent(t, publisher, repository, `{"type":"order.created","source":"order-service","payload":{}}`)

	assertJSONError(t, rec, http.StatusServiceUnavailable, "event_publish_failed", "event could not be accepted")
	if publisher.calls != 1 {
		t.Fatalf("expected publisher to be called once, got %d", publisher.calls)
	}
	if repository.markFailedCalls != 1 {
		t.Fatalf("expected publish failure to mark event failed, got %d calls", repository.markFailedCalls)
	}
}

func TestEventsHandlerGeneratesRequestID(t *testing.T) {
	rec := serveEvent(t, &fakePublisher{}, &fakeRepository{}, `{"type":"order.created","source":"order-service","payload":{}}`)

	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected generated X-Request-ID response header")
	}
}

func TestEventsHandlerGetExistingEventReturnsStatus(t *testing.T) {
	processedAt := fixedTime.Add(time.Second)
	repository := &fakeRepository{record: events.Record{
		EventID:             "evt-123",
		Type:                "order.created",
		Source:              "order-service",
		Status:              events.StatusProcessed,
		AttemptCount:        1,
		CreatedAt:           fixedTime,
		ProcessingStartedAt: &fixedTime,
		ProcessedAt:         &processedAt,
		UpdatedAt:           processedAt,
	}}
	handler := newTestHandler(&fakePublisher{}, repository)
	req := httptest.NewRequest(http.MethodGet, "/v1/events/evt-123", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var response events.Record
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.EventID != "evt-123" {
		t.Fatalf("expected event_id evt-123, got %q", response.EventID)
	}
	if response.Status != events.StatusProcessed {
		t.Fatalf("expected status %q, got %q", events.StatusProcessed, response.Status)
	}
}

func TestEventsHandlerGetUnknownEventReturnsNotFound(t *testing.T) {
	repository := &fakeRepository{getErr: events.ErrEventNotFound}
	handler := newTestHandler(&fakePublisher{}, repository)
	req := httptest.NewRequest(http.MethodGet, "/v1/events/missing", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assertJSONError(t, rec, http.StatusNotFound, "event_not_found", "event not found")
}

func TestEventsHandlerGetRepositoryFailureReturnsInternalServerError(t *testing.T) {
	repository := &fakeRepository{getErr: errors.New("db unavailable")}
	handler := newTestHandler(&fakePublisher{}, repository)
	req := httptest.NewRequest(http.MethodGet, "/v1/events/evt-123", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assertJSONError(t, rec, http.StatusInternalServerError, "event_lookup_failed", "event could not be retrieved")
}

var fixedTime = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

type fakePublisher struct {
	calls       int
	event       events.Event
	err         error
	hadDeadline bool
}

func (f *fakePublisher) Publish(ctx context.Context, event events.Event) error {
	f.calls++
	f.event = event
	_, f.hadDeadline = ctx.Deadline()
	return f.err
}

type fakeRepository struct {
	creates                  int
	created                  storedEvent
	createErr                error
	markFailedCalls          int
	record                   events.Record
	getErr                   error
	dlqRecords               []events.DLQRecord
	dlqRecord                events.DLQRecord
	listDLQErr               error
	getDLQErr                error
	beginReplayErr           error
	beginReplayCalls         int
	markReplayPublishedCalls int
	markReplayFailedCalls    int
}

type storedEvent struct {
	events.Event
	Status events.Status
}

func (f *fakeRepository) Create(_ context.Context, event events.Event) error {
	f.creates++
	f.created = storedEvent{Event: event}
	return f.createErr
}

func (f *fakeRepository) MarkProcessing(_ context.Context, _ string) error {
	return nil
}

func (f *fakeRepository) MarkProcessed(_ context.Context, _ string) error {
	return nil
}

func (f *fakeRepository) MarkFailed(_ context.Context, _ string, _ string) error {
	f.markFailedCalls++
	return nil
}

func (f *fakeRepository) MarkRetrying(_ context.Context, _ string, _ string) error {
	return nil
}

func (f *fakeRepository) MarkDLQ(_ context.Context, _ string, _ string, _ events.RetryMetadata) error {
	return nil
}

func (f *fakeRepository) ListDLQ(_ context.Context, _ int, _ int) ([]events.DLQRecord, error) {
	return f.dlqRecords, f.listDLQErr
}

func (f *fakeRepository) GetDLQ(_ context.Context, _ string) (events.DLQRecord, error) {
	if f.getDLQErr != nil {
		return events.DLQRecord{}, f.getDLQErr
	}
	return f.dlqRecord, nil
}

func (f *fakeRepository) BeginReplay(_ context.Context, _ string) (events.DLQRecord, error) {
	f.beginReplayCalls++
	if f.beginReplayErr != nil {
		return events.DLQRecord{}, f.beginReplayErr
	}
	return f.dlqRecord, nil
}

func (f *fakeRepository) MarkReplayPublished(_ context.Context, _ string) error {
	f.markReplayPublishedCalls++
	return nil
}

func (f *fakeRepository) MarkReplayFailed(_ context.Context, _ string, _ string) error {
	f.markReplayFailedCalls++
	return nil
}

func (f *fakeRepository) GetByEventID(_ context.Context, _ string) (events.Record, error) {
	if f.getErr != nil {
		return events.Record{}, f.getErr
	}
	return f.record, nil
}

func newTestHandler(publisher *fakePublisher, repository *fakeRepository) http.Handler {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewEventsHandler(publisher, repository, log, time.Second)
	handler.now = func() time.Time { return fixedTime }
	handler.newEventID = func() (string, error) { return "018fc1e0-b5d1-4f5e-9e39-70b6f1f05262", nil }
	return RequestIDMiddleware(handler)
}

func serveEvent(t *testing.T, publisher *fakePublisher, repository *fakeRepository, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	newTestHandler(publisher, repository).ServeHTTP(rec, req)
	return rec
}

func assertJSONError(t *testing.T, rec *httptest.ResponseRecorder, status int, code, message string) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("expected status %d, got %d", status, rec.Code)
	}

	var response errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error != code {
		t.Fatalf("expected error code %q, got %q", code, response.Error)
	}
	if response.Message != message {
		t.Fatalf("expected message %q, got %q", message, response.Message)
	}
}
