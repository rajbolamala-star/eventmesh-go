package kafka

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"eventmesh/internal/events"
	"eventmesh/internal/metrics"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestProducerInjectsTraceContext(t *testing.T) {
	installTestTracer(t)
	writer := &fakeWriter{}
	producer := NewProducerWithWriter(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), writer)

	if err := producer.Publish(contextWithSpan(), testEvent()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if got := headerValue(writer.messages[0].Headers, "traceparent"); got == "" {
		t.Fatal("expected traceparent header")
	}
}

func TestReplayInjectsTraceContextAndReplayHeaders(t *testing.T) {
	installTestTracer(t)
	writer := &fakeWriter{}
	producer := NewProducerWithWriter(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), writer)

	err := producer.Replay(contextWithSpan(), testEvent(), events.ReplayMetadata{
		ReplayAt:             time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		OriginalAttemptCount: 4,
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	headers := writer.messages[0].Headers
	if got := headerValue(headers, "traceparent"); got == "" {
		t.Fatal("expected traceparent header")
	}
	if got := headerValue(headers, headerReplayed); got != "true" {
		t.Fatalf("expected replay header true, got %q", got)
	}
}

func TestRetryPublishInjectsTraceContext(t *testing.T) {
	installTestTracer(t)
	writer := &fakeWriter{}
	cfg := testKafkaConfig()
	producer := NewFailureProducerWithWriters(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), map[string]messageWriter{
		cfg.RetryTopics[0]: writer,
		cfg.DLQTopic:       &fakeWriter{},
	})

	err := producer.PublishRetry(contextWithSpan(), testEvent(), events.RetryMetadata{
		Attempt:           2,
		RetryStage:        1,
		OriginalTopic:     cfg.Topic,
		OriginalPartition: 0,
		OriginalOffset:    1,
		FirstFailedAt:     time.Now().UTC(),
		LastFailedAt:      time.Now().UTC(),
		LastError:         "temporary failure",
	})
	if err != nil {
		t.Fatalf("publish retry: %v", err)
	}

	headers := writer.messages[0].Headers
	if got := headerValue(headers, "traceparent"); got == "" {
		t.Fatal("expected traceparent header")
	}
	if got := headerValue(headers, headerRetryStage); got != "1" {
		t.Fatalf("expected retry stage 1, got %q", got)
	}
}

func TestConsumerExtractsTraceContextForHandler(t *testing.T) {
	installTestTracer(t)
	parentTraceID := trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	parentSpanID := trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8}
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    parentTraceID,
		SpanID:     parentSpanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), parent)
	headers := injectTraceHeaders(ctx, nil)
	msg := testMessage(t, testEvent())
	msg.Headers = headers
	reader := &fakeReader{messages: []kafkago.Message{msg}}
	handler := &fakeHandler{}
	consumer := NewConsumerWithReader(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), reader, metrics.NewRecorder(), nil)

	if err := consumer.ConsumeEvents(context.Background(), handler); err != nil {
		t.Fatalf("consume events: %v", err)
	}

	if handler.traceID != parentTraceID {
		t.Fatalf("expected handler trace ID %s, got %s", parentTraceID.String(), handler.traceID.String())
	}
}

type fakeWriter struct {
	messages []kafkago.Message
	closed   bool
	err      error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, msgs...)
	return nil
}

func (f *fakeWriter) Close() error {
	f.closed = true
	return nil
}

func installTestTracer(t *testing.T) {
	t.Helper()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(nooptrace.NewTracerProvider())
	})
}

func contextWithSpan() context.Context {
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(context.Background(), spanContext)
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
