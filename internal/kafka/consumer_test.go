package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/events"
	"eventmesh/internal/metrics"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"
)

func TestDecodeEventValidMessage(t *testing.T) {
	event := testEvent()
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	got, err := DecodeEvent(value)
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}

	if got.EventID != event.EventID {
		t.Fatalf("expected event_id %q, got %q", event.EventID, got.EventID)
	}
	if got.Type != event.Type {
		t.Fatalf("expected type %q, got %q", event.Type, got.Type)
	}
	if got.Source != event.Source {
		t.Fatalf("expected source %q, got %q", event.Source, got.Source)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("expected created_at to be populated")
	}
}

func TestConsumeEventsHandlerReceivesCorrectEventAndCommits(t *testing.T) {
	event := testEvent()
	reader := &fakeReader{messages: []kafkago.Message{testMessage(t, event)}}
	handler := &fakeHandler{}
	consumer := newTestConsumer(reader)

	if err := consumer.ConsumeEvents(context.Background(), handler); err != nil {
		t.Fatalf("consume events: %v", err)
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}
	if handler.event.EventID != event.EventID {
		t.Fatalf("expected event_id %q, got %q", event.EventID, handler.event.EventID)
	}
	if reader.commits != 1 {
		t.Fatalf("expected one commit, got %d", reader.commits)
	}
	if reader.committed[0].Offset != 42 {
		t.Fatalf("expected committed offset 42, got %d", reader.committed[0].Offset)
	}
	if handler.metadata.Offset != 42 {
		t.Fatalf("expected metadata offset 42, got %d", handler.metadata.Offset)
	}
}

func TestConsumeEventsCommitsAfterSuccessfulHandling(t *testing.T) {
	ops := []string{}
	event := testEvent()
	reader := &fakeReader{
		messages: []kafkago.Message{testMessage(t, event)},
		onCommit: func() {
			ops = append(ops, "commit")
		},
	}
	handler := &fakeHandler{
		onHandle: func() {
			ops = append(ops, "handle")
		},
	}
	consumer := newTestConsumer(reader)

	if err := consumer.ConsumeEvents(context.Background(), handler); err != nil {
		t.Fatalf("consume events: %v", err)
	}

	if len(ops) != 2 || ops[0] != "handle" || ops[1] != "commit" {
		t.Fatalf("expected handle before commit, got %v", ops)
	}
}

func TestConsumeEventsHandlerFailureDoesNotCommit(t *testing.T) {
	event := testEvent()
	reader := &fakeReader{messages: []kafkago.Message{testMessage(t, event)}}
	handler := &fakeHandler{err: errors.New("processing failed")}
	consumer := newTestConsumer(reader)

	err := consumer.ConsumeEvents(context.Background(), handler)
	if err == nil {
		t.Fatal("expected handler error")
	}
	if reader.commits != 0 {
		t.Fatalf("expected no commits, got %d", reader.commits)
	}
}

func TestConsumeEventsMalformedJSONIsHandledSafely(t *testing.T) {
	failurePublisher := &fakeFailurePublisher{}
	reader := &fakeReader{messages: []kafkago.Message{{
		Topic:     "eventmesh.events",
		Partition: 1,
		Offset:    5,
		Value:     []byte(`{"event_id":`),
	}}}
	handler := &fakeHandler{}
	consumer := newTestConsumerWithFailurePublisher(reader, failurePublisher)

	if err := consumer.ConsumeEvents(context.Background(), handler); err != nil {
		t.Fatalf("consume events: %v", err)
	}
	if handler.calls != 0 {
		t.Fatalf("expected handler not to be called, got %d calls", handler.calls)
	}
	if reader.commits != 1 {
		t.Fatalf("expected malformed message to be committed once, got %d", reader.commits)
	}
	if failurePublisher.dlqCalls != 1 {
		t.Fatalf("expected malformed message to be published to DLQ once, got %d", failurePublisher.dlqCalls)
	}
}

func TestConsumeEventsInvalidEnvelopeIsHandledSafely(t *testing.T) {
	event := testEvent()
	event.EventID = ""
	failurePublisher := &fakeFailurePublisher{}
	reader := &fakeReader{messages: []kafkago.Message{testMessage(t, event)}}
	handler := &fakeHandler{}
	consumer := newTestConsumerWithFailurePublisher(reader, failurePublisher)

	if err := consumer.ConsumeEvents(context.Background(), handler); err != nil {
		t.Fatalf("consume events: %v", err)
	}
	if handler.calls != 0 {
		t.Fatalf("expected handler not to be called, got %d calls", handler.calls)
	}
	if reader.commits != 1 {
		t.Fatalf("expected invalid envelope to be committed once, got %d", reader.commits)
	}
	if failurePublisher.dlqCalls != 1 {
		t.Fatalf("expected invalid envelope to be published to DLQ once, got %d", failurePublisher.dlqCalls)
	}
}

func TestConsumeEventsDLQPublishFailurePreventsCommit(t *testing.T) {
	failurePublisher := &fakeFailurePublisher{dlqErr: errors.New("dlq unavailable")}
	reader := &fakeReader{messages: []kafkago.Message{{
		Topic:     "eventmesh.events",
		Partition: 1,
		Offset:    5,
		Value:     []byte(`{"event_id":`),
	}}}
	consumer := newTestConsumerWithFailurePublisher(reader, failurePublisher)

	err := consumer.ConsumeEvents(context.Background(), &fakeHandler{})
	if err == nil {
		t.Fatal("expected dlq publish error")
	}
	if reader.commits != 0 {
		t.Fatalf("expected no commit when DLQ publish fails, got %d", reader.commits)
	}
}

func TestDecodeMalformedRetryHeadersDoesNotPanic(t *testing.T) {
	reader := &fakeReader{messages: []kafkago.Message{{
		Topic:     "eventmesh.retry.1",
		Partition: 1,
		Offset:    5,
		Value:     []byte(`{}`),
		Headers: []kafkago.Header{
			{Key: headerAttempt, Value: []byte("not-a-number")},
		},
	}}}
	failurePublisher := &fakeFailurePublisher{}
	consumer := newTestConsumerWithFailurePublisher(reader, failurePublisher)

	if err := consumer.ConsumeEvents(context.Background(), &fakeHandler{}); err != nil {
		t.Fatalf("expected malformed headers to route to DLQ, got %v", err)
	}
	if failurePublisher.dlqCalls != 1 {
		t.Fatalf("expected DLQ publish, got %d", failurePublisher.dlqCalls)
	}
}

func TestConsumeEventsContextCancellationStopsProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{blockUntilCanceled: true}
	handler := &fakeHandler{}
	consumer := newTestConsumer(reader)

	cancel()
	if err := consumer.ConsumeEvents(ctx, handler); err != nil {
		t.Fatalf("expected graceful stop, got %v", err)
	}
}

func TestConsumerCloseClosesReader(t *testing.T) {
	reader := &fakeReader{}
	consumer := newTestConsumer(reader)

	consumer.Close()

	if !reader.closed {
		t.Fatal("expected reader to be closed")
	}
}

type fakeReader struct {
	messages           []kafkago.Message
	commits            int
	committed          []kafkago.Message
	closed             bool
	blockUntilCanceled bool
	onCommit           func()
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if f.blockUntilCanceled {
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	if len(f.messages) == 0 {
		return kafkago.Message{}, io.EOF
	}

	msg := f.messages[0]
	f.messages = f.messages[1:]
	return msg, nil
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.commits++
	f.committed = append(f.committed, msgs...)
	if f.onCommit != nil {
		f.onCommit()
	}
	return nil
}

func (f *fakeReader) Close() error {
	f.closed = true
	return nil
}

type fakeHandler struct {
	calls    int
	event    events.Event
	metadata events.Metadata
	traceID  trace.TraceID
	err      error
	onHandle func()
}

func (f *fakeHandler) Handle(ctx context.Context, event events.Event) error {
	f.calls++
	f.event = event
	f.metadata, _ = events.MetadataFromContext(ctx)
	f.traceID = trace.SpanContextFromContext(ctx).TraceID()
	if f.onHandle != nil {
		f.onHandle()
	}
	return f.err
}

func newTestConsumer(reader *fakeReader) *Consumer {
	return newTestConsumerWithFailurePublisher(reader, nil)
}

func newTestConsumerWithFailurePublisher(reader *fakeReader, failurePublisher events.FailurePublisher) *Consumer {
	return NewConsumerWithReader(config.KafkaConfig{
		Brokers:               []string{"localhost:9092"},
		Topic:                 "eventmesh.events",
		RetryTopics:           []string{"eventmesh.retry.1", "eventmesh.retry.2", "eventmesh.retry.3"},
		DLQTopic:              "eventmesh.dlq",
		RetryDelays:           []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute},
		ConsumerGroup:         "eventmesh-processors",
		ConsumerMinBytes:      1,
		ConsumerMaxBytes:      10e6,
		ConsumerMaxWait:       time.Second,
		ProcessingConcurrency: 1,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), reader, metrics.NewRecorder(), failurePublisher)
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

type fakeFailurePublisher struct {
	retryCalls int
	dlqCalls   int
	dlqErr     error
}

func (f *fakeFailurePublisher) PublishRetry(context.Context, events.Event, events.RetryMetadata) error {
	f.retryCalls++
	return nil
}

func (f *fakeFailurePublisher) PublishDLQ(_ context.Context, _ events.DeadLetterMessage) error {
	f.dlqCalls++
	return f.dlqErr
}

func testMessage(t *testing.T, event events.Event) kafkago.Message {
	t.Helper()

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	return kafkago.Message{
		Topic:     "eventmesh.events",
		Partition: 1,
		Offset:    42,
		Key:       []byte(event.EventID),
		Value:     value,
	}
}
