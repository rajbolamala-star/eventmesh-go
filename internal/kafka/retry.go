package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/events"
	"eventmesh/internal/observability"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	headerAttempt              = "eventmesh-attempt"
	headerRetryStage           = "eventmesh-retry-stage"
	headerOriginalTopic        = "eventmesh-original-topic"
	headerOriginalPartition    = "eventmesh-original-partition"
	headerOriginalOffset       = "eventmesh-original-offset"
	headerFirstFailedAt        = "eventmesh-first-failed-at"
	headerLastFailedAt         = "eventmesh-last-failed-at"
	headerLastError            = "eventmesh-last-error"
	headerNotBeforeAt          = "eventmesh-not-before-at"
	headerReplayed             = "eventmesh-replayed"
	headerReplayAt             = "eventmesh-replay-at"
	headerOriginalAttemptCount = "eventmesh-original-attempt-count"
)

type FailureProducer struct {
	cfg     config.KafkaConfig
	log     *slog.Logger
	writers map[string]messageWriter
	now     func() time.Time
}

func NewFailureProducer(cfg config.KafkaConfig, log *slog.Logger) *FailureProducer {
	writers := make(map[string]messageWriter, len(cfg.RetryTopics)+1)
	for _, topic := range append(cfg.RetryTopics, cfg.DLQTopic) {
		if strings.TrimSpace(topic) == "" {
			continue
		}
		writers[topic] = &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireAll,
			Async:        false,
		}
	}

	return &FailureProducer{cfg: cfg, log: log, writers: writers, now: time.Now}
}

func NewFailureProducerWithWriters(cfg config.KafkaConfig, log *slog.Logger, writers map[string]messageWriter) *FailureProducer {
	return &FailureProducer{cfg: cfg, log: log, writers: writers, now: time.Now}
}

func (p *FailureProducer) PublishRetry(ctx context.Context, event events.Event, metadata events.RetryMetadata) error {
	topic := ""
	if metadata.RetryStage >= 1 && metadata.RetryStage <= len(p.cfg.RetryTopics) {
		topic = p.cfg.RetryTopics[metadata.RetryStage-1]
	}
	ctx, span := observability.Tracer().Start(
		ctx,
		"kafka.publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("event.type", event.Type),
			attribute.String("event.source", event.Source),
			attribute.Int("eventmesh.attempt", metadata.Attempt),
			attribute.Int("eventmesh.retry_stage", metadata.RetryStage),
		),
	)
	defer span.End()

	if metadata.RetryStage < 1 || metadata.RetryStage > len(p.cfg.RetryTopics) {
		err := fmt.Errorf("retry stage %d is out of range", metadata.RetryStage)
		span.RecordError(err)
		span.SetStatus(codes.Error, "retry stage out of range")
		return err
	}

	writer, ok := p.writers[topic]
	if !ok {
		err := fmt.Errorf("missing retry writer for topic %s", topic)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing retry writer")
		return err
	}

	value, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "event marshal failed")
		return err
	}

	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(event.EventID),
		Value:   value,
		Headers: injectTraceHeaders(ctx, encodeRetryHeaders(metadata)),
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "retry publish failed")
		return err
	}
	return nil
}

func (p *FailureProducer) PublishDLQ(ctx context.Context, message events.DeadLetterMessage) error {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", p.cfg.DLQTopic),
		attribute.Int("eventmesh.attempt", message.Metadata.Attempt),
		attribute.Int("eventmesh.retry_stage", message.Metadata.RetryStage),
	}
	if message.Event != nil {
		attrs = append(attrs,
			attribute.String("event.type", message.Event.Type),
			attribute.String("event.source", message.Event.Source),
		)
	}
	ctx, span := observability.Tracer().Start(
		ctx,
		"kafka.publish "+p.cfg.DLQTopic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
	defer span.End()

	writer, ok := p.writers[p.cfg.DLQTopic]
	if !ok {
		err := fmt.Errorf("missing dlq writer for topic %s", p.cfg.DLQTopic)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing dlq writer")
		return err
	}

	value, err := json.Marshal(message)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "dlq marshal failed")
		return err
	}

	key := []byte(message.Metadata.OriginalTopic)
	if message.Event != nil && message.Event.EventID != "" {
		key = []byte(message.Event.EventID)
	}

	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     key,
		Value:   value,
		Headers: injectTraceHeaders(ctx, encodeRetryHeaders(message.Metadata)),
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "dlq publish failed")
		return err
	}
	return nil
}

func (p *FailureProducer) Close() {
	for topic, writer := range p.writers {
		if err := writer.Close(); err != nil {
			p.log.Error("kafka failure writer close failed", "topic", topic, "error", err)
		}
	}
}

func encodeRetryHeaders(metadata events.RetryMetadata) []kafkago.Header {
	return []kafkago.Header{
		{Key: headerAttempt, Value: []byte(strconv.Itoa(metadata.Attempt))},
		{Key: headerRetryStage, Value: []byte(strconv.Itoa(metadata.RetryStage))},
		{Key: headerOriginalTopic, Value: []byte(metadata.OriginalTopic)},
		{Key: headerOriginalPartition, Value: []byte(strconv.Itoa(metadata.OriginalPartition))},
		{Key: headerOriginalOffset, Value: []byte(strconv.FormatInt(metadata.OriginalOffset, 10))},
		{Key: headerFirstFailedAt, Value: []byte(formatTime(metadata.FirstFailedAt))},
		{Key: headerLastFailedAt, Value: []byte(formatTime(metadata.LastFailedAt))},
		{Key: headerLastError, Value: []byte(metadata.LastError)},
		{Key: headerNotBeforeAt, Value: []byte(formatTime(metadata.NotBefore))},
	}
}

func decodeRetryHeaders(headers []kafkago.Header) (events.RetryMetadata, error) {
	values := map[string]string{}
	for _, header := range headers {
		values[header.Key] = string(header.Value)
	}
	if _, ok := values[headerAttempt]; !ok {
		return events.RetryMetadata{}, nil
	}

	attempt, err := parseIntHeader(values, headerAttempt)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	stage, err := parseIntHeader(values, headerRetryStage)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	partition, err := parseIntHeader(values, headerOriginalPartition)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	offset, err := parseInt64Header(values, headerOriginalOffset)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	firstFailedAt, err := parseTimeHeader(values, headerFirstFailedAt)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	lastFailedAt, err := parseTimeHeader(values, headerLastFailedAt)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	notBefore, err := parseOptionalTimeHeader(values, headerNotBeforeAt)
	if err != nil {
		return events.RetryMetadata{}, err
	}

	return events.RetryMetadata{
		Attempt:           attempt,
		RetryStage:        stage,
		OriginalTopic:     values[headerOriginalTopic],
		OriginalPartition: partition,
		OriginalOffset:    offset,
		FirstFailedAt:     firstFailedAt,
		LastFailedAt:      lastFailedAt,
		LastError:         values[headerLastError],
		NotBefore:         notBefore,
	}, nil
}

func retryMetadataFromMessage(msg kafkago.Message) (events.RetryMetadata, error) {
	metadata, err := decodeRetryHeaders(msg.Headers)
	if err != nil {
		return events.RetryMetadata{}, err
	}
	if metadata.Attempt == 0 {
		return events.RetryMetadata{
			Attempt:           1,
			RetryStage:        0,
			OriginalTopic:     msg.Topic,
			OriginalPartition: msg.Partition,
			OriginalOffset:    msg.Offset,
		}, nil
	}
	return metadata, events.ValidateRetryMetadata(metadata)
}

func parseIntHeader(values map[string]string, key string) (int, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing retry header %s", key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid retry header %s", key)
	}
	return parsed, nil
}

func parseInt64Header(values map[string]string, key string) (int64, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing retry header %s", key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid retry header %s", key)
	}
	return parsed, nil
}

func parseTimeHeader(values map[string]string, key string) (time.Time, error) {
	value, ok := values[key]
	if !ok || value == "" {
		return time.Time{}, fmt.Errorf("missing retry header %s", key)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid retry header %s", key)
	}
	return parsed, nil
}

func parseOptionalTimeHeader(values map[string]string, key string) (time.Time, error) {
	value := values[key]
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid retry header %s", key)
	}
	return parsed, nil
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
