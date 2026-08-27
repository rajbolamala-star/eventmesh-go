package redis

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/events"
	"eventmesh/internal/observability"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	processingValue = "PROCESSING"
	completedValue  = "COMPLETED"
	keyPrefix       = "eventmesh:idempotency:"
)

type Client struct {
	client        *goredis.Client
	operationTTL  time.Duration
	processingTTL time.Duration
	completedTTL  time.Duration
	log           *slog.Logger
}

func NewClient(cfg config.RedisConfig, log *slog.Logger) *Client {
	client := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.ConnectTimeout,
		ReadTimeout:  cfg.OperationTimeout,
		WriteTimeout: cfg.OperationTimeout,
	})

	return &Client{
		client:        client,
		operationTTL:  cfg.OperationTimeout,
		processingTTL: cfg.IdempotencyProcessingTTL,
		completedTTL:  cfg.IdempotencyCompletedTTL,
		log:           log,
	}
}

func (c *Client) Ping(ctx context.Context) error {
	ctx, span := c.startSpan(ctx, "redis.ping")
	defer span.End()
	opCtx, cancel := c.operationContext(ctx)
	defer cancel()
	err := c.client.Ping(opCtx).Err()
	recordSpanError(span, err)
	return err
}

func (c *Client) Acquire(ctx context.Context, eventID string) (events.IdempotencyAcquireResult, error) {
	ctx, span := c.startSpan(ctx, "idempotency.acquire")
	defer span.End()
	key := IdempotencyKey(eventID)

	for attempt := 0; attempt < 2; attempt++ {
		opCtx, cancel := c.operationContext(ctx)
		acquired, err := c.client.SetNX(opCtx, key, processingValue, c.processingTTL).Result()
		cancel()
		if err != nil {
			recordSpanError(span, err)
			return "", err
		}
		if acquired {
			span.SetAttributes(attribute.String("eventmesh.idempotency_result", string(events.IdempotencyAcquired)))
			return events.IdempotencyAcquired, nil
		}

		state, err := c.getState(ctx, key)
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			recordSpanError(span, err)
			return "", err
		}

		switch state {
		case completedValue:
			span.SetAttributes(attribute.String("eventmesh.idempotency_result", string(events.IdempotencyCompleted)))
			return events.IdempotencyCompleted, nil
		case processingValue:
			span.SetAttributes(attribute.String("eventmesh.idempotency_result", string(events.IdempotencyAlreadyProcessing)))
			return events.IdempotencyAlreadyProcessing, nil
		default:
			err := errors.New("unknown idempotency state")
			recordSpanError(span, err)
			return "", err
		}
	}

	err := errors.New("idempotency state changed during acquire")
	recordSpanError(span, err)
	return "", err
}

func (c *Client) MarkCompleted(ctx context.Context, eventID string) error {
	ctx, span := c.startSpan(ctx, "idempotency.mark_completed")
	defer span.End()
	opCtx, cancel := c.operationContext(ctx)
	defer cancel()
	err := c.client.Set(opCtx, IdempotencyKey(eventID), completedValue, c.completedTTL).Err()
	recordSpanError(span, err)
	return err
}

func (c *Client) Release(ctx context.Context, eventID string) error {
	ctx, span := c.startSpan(ctx, "idempotency.release")
	defer span.End()
	opCtx, cancel := c.operationContext(ctx)
	defer cancel()
	err := c.client.Del(opCtx, IdempotencyKey(eventID)).Err()
	recordSpanError(span, err)
	return err
}

func (c *Client) IsCompleted(ctx context.Context, eventID string) (bool, error) {
	state, err := c.State(ctx, eventID)
	if err != nil {
		return false, err
	}
	return state == events.IdempotencyStateCompleted, nil
}

func (c *Client) State(ctx context.Context, eventID string) (events.IdempotencyState, error) {
	ctx, span := c.startSpan(ctx, "idempotency.get_state")
	defer span.End()
	state, err := c.getState(ctx, IdempotencyKey(eventID))
	if errors.Is(err, goredis.Nil) {
		span.SetAttributes(attribute.String("eventmesh.idempotency_state", string(events.IdempotencyStateAbsent)))
		return events.IdempotencyStateAbsent, nil
	}
	if err != nil {
		recordSpanError(span, err)
		return "", err
	}
	switch state {
	case processingValue:
		span.SetAttributes(attribute.String("eventmesh.idempotency_state", string(events.IdempotencyStateProcessing)))
		return events.IdempotencyStateProcessing, nil
	case completedValue:
		span.SetAttributes(attribute.String("eventmesh.idempotency_state", string(events.IdempotencyStateCompleted)))
		return events.IdempotencyStateCompleted, nil
	default:
		err := errors.New("unknown idempotency state")
		recordSpanError(span, err)
		return "", err
	}
}

func (c *Client) Close() {
	if err := c.client.Close(); err != nil {
		c.log.Error("redis client close failed", "error", err)
		return
	}
	c.log.Debug("redis client closed")
}

func IdempotencyKey(eventID string) string {
	return keyPrefix + strings.TrimSpace(eventID)
}

func (c *Client) getState(ctx context.Context, key string) (string, error) {
	opCtx, cancel := c.operationContext(ctx)
	defer cancel()
	return c.client.Get(opCtx, key).Result()
}

func (c *Client) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.operationTTL <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.operationTTL)
}

func (c *Client) startSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return observability.Tracer().Start(ctx, name, trace.WithAttributes(attribute.String("db.system.name", "redis")))
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, "operation failed")
}
