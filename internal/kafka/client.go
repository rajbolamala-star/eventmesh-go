package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/events"
	"eventmesh/internal/metrics"
	"eventmesh/internal/observability"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Message struct {
	Key   []byte
	Value []byte
}

type Producer struct {
	cfg    config.KafkaConfig
	log    *slog.Logger
	writer messageWriter
}

type Consumer struct {
	cfg              config.KafkaConfig
	topic            string
	log              *slog.Logger
	reader           messageReader
	metrics          *metrics.Recorder
	failurePublisher events.FailurePublisher
	now              func() time.Time
}

func NewProducer(cfg config.KafkaConfig, log *slog.Logger) *Producer {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
	}

	return &Producer{cfg: cfg, log: log, writer: writer}
}

func NewProducerWithWriter(cfg config.KafkaConfig, log *slog.Logger, writer messageWriter) *Producer {
	return &Producer{cfg: cfg, log: log, writer: writer}
}

func (p *Producer) Publish(ctx context.Context, event events.Event) error {
	ctx, span := observability.Tracer().Start(
		ctx,
		"kafka.publish "+p.cfg.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.cfg.Topic),
			attribute.String("event.type", event.Type),
			attribute.String("event.source", event.Source),
		),
	)
	defer span.End()

	value, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "event marshal failed")
		return err
	}

	headers := injectTraceHeaders(ctx, []kafkago.Header{
		{Key: "event_id", Value: []byte(event.EventID)},
		{Key: "event_type", Value: []byte(event.Type)},
		{Key: "source", Value: []byte(event.Source)},
	})

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(event.EventID),
		Value:   value,
		Headers: headers,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "kafka publish failed")
		return err
	}
	return nil
}

func (p *Producer) Replay(ctx context.Context, event events.Event, metadata events.ReplayMetadata) error {
	ctx, span := observability.Tracer().Start(
		ctx,
		"kafka.replay "+p.cfg.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.cfg.Topic),
			attribute.String("event.type", event.Type),
			attribute.String("event.source", event.Source),
			attribute.Bool("eventmesh.replayed", true),
			attribute.Int("eventmesh.original_attempt_count", metadata.OriginalAttemptCount),
		),
	)
	defer span.End()

	value, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "event marshal failed")
		return err
	}

	headers := injectTraceHeaders(ctx, []kafkago.Header{
		{Key: "event_id", Value: []byte(event.EventID)},
		{Key: "event_type", Value: []byte(event.Type)},
		{Key: "source", Value: []byte(event.Source)},
		{Key: headerReplayed, Value: []byte("true")},
		{Key: headerReplayAt, Value: []byte(formatTime(metadata.ReplayAt))},
		{Key: headerOriginalAttemptCount, Value: []byte(fmt.Sprintf("%d", metadata.OriginalAttemptCount))},
	})

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(event.EventID),
		Value:   value,
		Headers: headers,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "kafka replay publish failed")
		return err
	}
	return nil
}

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		p.log.Error("kafka producer close failed", "error", err)
		return
	}
	p.log.Debug("kafka producer closed")
}

func NewConsumer(cfg config.KafkaConfig, log *slog.Logger) *Consumer {
	return NewConsumerForTopic(cfg, cfg.Topic, log, nil)
}

func NewConsumerForTopic(cfg config.KafkaConfig, topic string, log *slog.Logger, failurePublisher events.FailurePublisher) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          topic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       cfg.ConsumerMinBytes,
		MaxBytes:       cfg.ConsumerMaxBytes,
		MaxWait:        cfg.ConsumerMaxWait,
		CommitInterval: 0,
	})

	consumer := NewConsumerWithReader(cfg, log, reader, metrics.NewRecorder(), failurePublisher)
	consumer.topic = topic
	return consumer
}

func NewConsumerWithReader(cfg config.KafkaConfig, log *slog.Logger, reader messageReader, recorder *metrics.Recorder, failurePublisher events.FailurePublisher) *Consumer {
	if recorder == nil {
		recorder = metrics.NewRecorder()
	}
	return &Consumer{cfg: cfg, topic: cfg.Topic, log: log, reader: reader, metrics: recorder, failurePublisher: failurePublisher, now: time.Now}
}

func (c *Consumer) ConsumeEvents(ctx context.Context, handler events.EventHandler) error {
	c.log.Info(
		"consumer_started",
		"topic", c.topic,
		"group_id", c.cfg.ConsumerGroup,
		"min_bytes", c.cfg.ConsumerMinBytes,
		"max_bytes", c.cfg.ConsumerMaxBytes,
		"max_wait_ms", c.cfg.ConsumerMaxWait.Milliseconds(),
		"configured_processing_concurrency", c.cfg.ProcessingConcurrency,
		"effective_processing_concurrency", 1,
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if isContextDone(ctx, err) || errors.Is(err, io.EOF) {
				c.log.Info("consumer_stopped", "topic", c.topic, "group_id", c.cfg.ConsumerGroup)
				return nil
			}

			c.log.Error("message_fetch_failed", "topic", c.topic, "group_id", c.cfg.ConsumerGroup, "error", err)
			if !sleepWithContext(ctx, 250*time.Millisecond) {
				c.log.Info("consumer_stopped", "topic", c.topic, "group_id", c.cfg.ConsumerGroup)
				return nil
			}
			continue
		}

		if err := c.handleMessage(ctx, handler, msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		c.log.Error("kafka consumer close failed", "error", err)
		return
	}
	c.log.Debug("kafka consumer closed")
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

func (c *Consumer) handleMessage(ctx context.Context, handler events.EventHandler, msg kafkago.Message) error {
	started := time.Now()
	extractedCtx := extractTraceContext(ctx, msg.Headers)
	c.metrics.RecordEventConsumed()
	c.log.Info(
		"message_received",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	retryMetadata, metadataErr := retryMetadataFromMessage(msg)
	if metadataErr != nil {
		messageCtx, span := c.startConsumerSpan(extractedCtx, msg, events.RetryMetadata{})
		defer span.End()
		return c.publishPoisonDLQ(messageCtx, msg, metadataErr, started)
	}
	if retryMetadata.RetryStage > 0 {
		c.metrics.RecordRetryProcessing()
		c.log.Info(
			"retry_processing_started",
			"attempt", retryMetadata.Attempt,
			"retry_stage", retryMetadata.RetryStage,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
		if !c.waitUntilDue(ctx, retryMetadata.NotBefore) {
			return ctx.Err()
		}
	}

	messageCtx, span := c.startConsumerSpan(extractedCtx, msg, retryMetadata)
	defer span.End()
	log := observability.WithTrace(messageCtx, c.log)

	event, err := DecodeEvent(msg.Value)
	if err != nil {
		c.metrics.RecordEventProcessingFailed(time.Since(started))
		span.RecordError(err)
		span.SetStatus(codes.Error, "event decode failed")
		log.Error(
			"event_processing_failed",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)

		return c.publishPoisonDLQ(messageCtx, msg, err, started)
	}
	span.SetAttributes(
		attribute.String("event.type", event.Type),
		attribute.String("event.source", event.Source),
	)

	eventCtx := events.ContextWithMetadata(messageCtx, events.Metadata{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	})
	eventCtx = events.ContextWithRetryMetadata(eventCtx, retryMetadata)

	log.Info(
		"event_processing_started",
		"event_id", event.EventID,
		"event_type", event.Type,
		"source", event.Source,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	if err := handler.Handle(eventCtx, event); err != nil {
		c.metrics.RecordEventProcessingFailed(time.Since(started))
		span.RecordError(err)
		span.SetStatus(codes.Error, "event handling failed")
		if retryMetadata.RetryStage > 0 {
			log.Error(
				"retry_processing_failed",
				"event_id", event.EventID,
				"event_type", event.Type,
				"attempt", retryMetadata.Attempt,
				"retry_stage", retryMetadata.RetryStage,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"duration_ms", time.Since(started).Milliseconds(),
				"error", err,
			)
		}
		log.Error(
			"event_processing_failed",
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return fmt.Errorf("handle event %s: %w", event.EventID, err)
	}

	if err := c.reader.CommitMessages(messageCtx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "offset commit failed")
		return fmt.Errorf("commit event offset %s: %w", event.EventID, err)
	}

	c.metrics.RecordEventProcessed(time.Since(started))
	if retryMetadata.RetryStage > 0 {
		log.Info(
			"retry_processing_succeeded",
			"event_id", event.EventID,
			"event_type", event.Type,
			"attempt", retryMetadata.Attempt,
			"retry_stage", retryMetadata.RetryStage,
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}
	log.Info(
		"event_processing_succeeded",
		"event_id", event.EventID,
		"event_type", event.Type,
		"source", event.Source,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"duration_ms", time.Since(started).Milliseconds(),
	)
	log.Info("offset_committed", "event_id", event.EventID, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)

	return nil
}

func (c *Consumer) startConsumerSpan(ctx context.Context, msg kafkago.Message, retryMetadata events.RetryMetadata) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", msg.Topic),
		attribute.Int("messaging.kafka.partition", msg.Partition),
		attribute.Int64("messaging.kafka.offset", msg.Offset),
		attribute.Int("eventmesh.attempt", retryMetadata.Attempt),
		attribute.Int("eventmesh.retry_stage", retryMetadata.RetryStage),
	}
	return observability.Tracer().Start(
		ctx,
		"kafka.consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
}

func (c *Consumer) publishPoisonDLQ(ctx context.Context, msg kafkago.Message, cause error, started time.Time) error {
	if c.failurePublisher == nil {
		return fmt.Errorf("poison message cannot be routed to dlq: %w", cause)
	}
	log := observability.WithTrace(ctx, c.log)

	metadata := events.RetryMetadata{
		Attempt:           1,
		RetryStage:        0,
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		FirstFailedAt:     c.now().UTC(),
		LastFailedAt:      c.now().UTC(),
		LastError:         sanitizeFailureReason(cause.Error()),
	}
	raw := msg.Value
	if len(raw) > 2048 {
		raw = raw[:2048]
	}

	span := trace.SpanFromContext(ctx)
	span.RecordError(cause)
	span.SetStatus(codes.Error, "poison message routed to DLQ")

	err := c.failurePublisher.PublishDLQ(ctx, events.DeadLetterMessage{
		RawPayload: raw,
		Failure: events.FailureDetail{
			Kind:   events.FailureNonRetryable,
			Reason: sanitizeFailureReason(cause.Error()),
		},
		Metadata: metadata,
	})
	if err != nil {
		c.metrics.RecordDLQPublishFailed()
		log.Error("dlq_publish_failed", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return fmt.Errorf("publish poison dlq: %w", err)
	}
	c.metrics.RecordDLQPublished()
	log.Info("dlq_published", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "duration_ms", time.Since(started).Milliseconds())

	if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
		span.RecordError(commitErr)
		span.SetStatus(codes.Error, "poison offset commit failed")
		return fmt.Errorf("commit poison event offset: %w", commitErr)
	}
	log.Info("offset_committed", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	return nil
}

func (c *Consumer) waitUntilDue(ctx context.Context, notBefore time.Time) bool {
	if notBefore.IsZero() {
		return true
	}
	delay := time.Until(notBefore)
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func DecodeEvent(value []byte) (events.Event, error) {
	var event events.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return events.Event{}, fmt.Errorf("decode event: %w", err)
	}
	if strings.TrimSpace(event.EventID) == "" {
		return events.Event{}, errors.New("event_id is required")
	}
	if strings.TrimSpace(event.Type) == "" {
		return events.Event{}, errors.New("type is required")
	}
	if strings.TrimSpace(event.Source) == "" {
		return events.Event{}, errors.New("source is required")
	}
	if event.CreatedAt.IsZero() {
		return events.Event{}, errors.New("created_at is required")
	}
	return event, nil
}

func isContextDone(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func sleepWithContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sanitizeFailureReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "event processing failed"
	}
	runes := []rune(reason)
	if len(runes) > 1000 {
		return string(runes[:1000])
	}
	return reason
}

type ConsumerGroup struct {
	consumers []*Consumer
}

func NewConsumerGroup(cfg config.KafkaConfig, log *slog.Logger, failurePublisher events.FailurePublisher) *ConsumerGroup {
	topics := append([]string{cfg.Topic}, cfg.RetryTopics...)
	consumers := make([]*Consumer, 0, len(topics))
	for _, topic := range topics {
		consumers = append(consumers, NewConsumerForTopic(cfg, topic, log, failurePublisher))
	}
	return &ConsumerGroup{consumers: consumers}
}

func (g *ConsumerGroup) Run(ctx context.Context, handler events.EventHandler) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(g.consumers))
	for _, consumer := range g.consumers {
		consumer := consumer
		go func() {
			errCh <- consumer.ConsumeEvents(runCtx, handler)
		}()
	}

	var firstErr error
	for range g.consumers {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

func (g *ConsumerGroup) Close() {
	for _, consumer := range g.consumers {
		consumer.Close()
	}
}
