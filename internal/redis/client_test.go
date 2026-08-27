package redis

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/events"

	"github.com/alicebob/miniredis/v2"
)

func TestIdempotencyKey(t *testing.T) {
	got := IdempotencyKey("  evt-123  ")
	want := "eventmesh:idempotency:evt-123"
	if got != want {
		t.Fatalf("expected key %q, got %q", want, got)
	}
}

func TestIdempotencyStateValues(t *testing.T) {
	if processingValue != "PROCESSING" {
		t.Fatalf("expected processing value PROCESSING, got %q", processingValue)
	}
	if completedValue != "COMPLETED" {
		t.Fatalf("expected completed value COMPLETED, got %q", completedValue)
	}
}

func TestAcquireSetsProcessingAtomically(t *testing.T) {
	server, client := newTestClient(t)

	result, err := client.Acquire(context.Background(), "evt-123")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if result != events.IdempotencyAcquired {
		t.Fatalf("expected acquired, got %q", result)
	}
	got, err := server.Get(IdempotencyKey("evt-123"))
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	if got != processingValue {
		t.Fatalf("expected processing value, got %q", got)
	}
	if ttl := server.TTL(IdempotencyKey("evt-123")); ttl <= 0 {
		t.Fatalf("expected processing key to have ttl, got %s", ttl)
	}
}

func TestAcquireReturnsProcessingConflict(t *testing.T) {
	_, client := newTestClient(t)

	if _, err := client.Acquire(context.Background(), "evt-123"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	result, err := client.Acquire(context.Background(), "evt-123")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if result != events.IdempotencyAlreadyProcessing {
		t.Fatalf("expected already processing, got %q", result)
	}
}

func TestMarkCompletedAndIsCompleted(t *testing.T) {
	server, client := newTestClient(t)

	if err := client.MarkCompleted(context.Background(), "evt-123"); err != nil {
		t.Fatalf("mark completed: %v", err)
	}

	completed, err := client.IsCompleted(context.Background(), "evt-123")
	if err != nil {
		t.Fatalf("is completed: %v", err)
	}
	if !completed {
		t.Fatal("expected event to be completed")
	}
	got, err := server.Get(IdempotencyKey("evt-123"))
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	if got != completedValue {
		t.Fatalf("expected completed value, got %q", got)
	}
}

func TestAcquireReturnsCompletedForDuplicate(t *testing.T) {
	_, client := newTestClient(t)

	if err := client.MarkCompleted(context.Background(), "evt-123"); err != nil {
		t.Fatalf("mark completed: %v", err)
	}

	result, err := client.Acquire(context.Background(), "evt-123")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if result != events.IdempotencyCompleted {
		t.Fatalf("expected completed, got %q", result)
	}
}

func TestReleaseDeletesProcessingKey(t *testing.T) {
	server, client := newTestClient(t)

	if _, err := client.Acquire(context.Background(), "evt-123"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := client.Release(context.Background(), "evt-123"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if server.Exists(IdempotencyKey("evt-123")) {
		t.Fatal("expected key to be deleted")
	}
}

func TestProcessingLeaseExpirationAllowsAcquire(t *testing.T) {
	server, client := newTestClient(t)

	if _, err := client.Acquire(context.Background(), "evt-123"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	server.FastForward(5 * time.Minute)

	result, err := client.Acquire(context.Background(), "evt-123")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if result != events.IdempotencyAcquired {
		t.Fatalf("expected acquire after ttl expiration, got %q", result)
	}
}

func newTestClient(t *testing.T) (*miniredis.Miniredis, *Client) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("local listener unavailable for miniredis: %v", err)
		}
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(server.Close)
	client := NewClient(config.RedisConfig{
		Addr:                     server.Addr(),
		DB:                       0,
		ConnectTimeout:           time.Second,
		OperationTimeout:         time.Second,
		IdempotencyProcessingTTL: 5 * time.Minute,
		IdempotencyCompletedTTL:  24 * time.Hour,
	}, slog.Default())
	t.Cleanup(client.Close)
	return server, client
}
