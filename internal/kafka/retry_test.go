package kafka

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/events"
	"eventmesh/internal/metrics"
)

func TestRetryHeadersRoundTrip(t *testing.T) {
	metadata := events.RetryMetadata{
		Attempt:           2,
		RetryStage:        1,
		OriginalTopic:     "eventmesh.events",
		OriginalPartition: 3,
		OriginalOffset:    99,
		FirstFailedAt:     time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
		LastFailedAt:      time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC),
		LastError:         "temporary timeout",
		NotBefore:         time.Date(2026, 8, 26, 12, 1, 5, 0, time.UTC),
	}

	got, err := decodeRetryHeaders(encodeRetryHeaders(metadata))
	if err != nil {
		t.Fatalf("decode retry headers: %v", err)
	}
	if got.Attempt != metadata.Attempt || got.RetryStage != metadata.RetryStage {
		t.Fatalf("expected attempt/stage %d/%d, got %d/%d", metadata.Attempt, metadata.RetryStage, got.Attempt, got.RetryStage)
	}
	if got.OriginalTopic != metadata.OriginalTopic || got.OriginalPartition != metadata.OriginalPartition || got.OriginalOffset != metadata.OriginalOffset {
		t.Fatalf("unexpected original metadata: %+v", got)
	}
	if got.LastError != metadata.LastError {
		t.Fatalf("expected last error %q, got %q", metadata.LastError, got.LastError)
	}
}

func TestConsumerGroupContextCancellationStopsConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	readers := []*fakeReader{{blockUntilCanceled: true}, {blockUntilCanceled: true}}
	group := &ConsumerGroup{consumers: []*Consumer{
		NewConsumerWithReader(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), readers[0], metrics.NewRecorder(), nil),
		NewConsumerWithReader(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), readers[1], metrics.NewRecorder(), nil),
	}}

	done := make(chan error, 1)
	go func() {
		done <- group.Run(ctx, &fakeHandler{})
	}()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected graceful cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected consumer group to stop after cancellation")
	}
}

func TestConsumerGroupCloseClosesAllReaders(t *testing.T) {
	readers := []*fakeReader{{}, {}}
	group := &ConsumerGroup{consumers: []*Consumer{
		NewConsumerWithReader(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), readers[0], metrics.NewRecorder(), nil),
		NewConsumerWithReader(testKafkaConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), readers[1], metrics.NewRecorder(), nil),
	}}

	group.Close()

	for i, reader := range readers {
		if !reader.closed {
			t.Fatalf("expected reader %d to be closed", i)
		}
	}
}

func testKafkaConfig() config.KafkaConfig {
	return config.KafkaConfig{
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
	}
}
