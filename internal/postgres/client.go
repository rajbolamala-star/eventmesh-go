package postgres

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"unicode/utf8"

	"eventmesh/internal/config"
	"eventmesh/internal/events"
	"eventmesh/internal/observability"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const maxLastErrorRunes = 1000

type Client struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewClient(ctx context.Context, cfg config.PostgresConfig, log *slog.Logger) (*Client, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	return &Client{pool: pool, log: log}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	ctx, span := c.startSpan(ctx, "postgres.ping")
	defer span.End()
	err := c.pool.Ping(ctx)
	recordSpanError(span, err)
	return err
}

func (c *Client) Close() {
	c.pool.Close()
	c.log.Debug("postgres client closed")
}

func (c *Client) Create(ctx context.Context, event events.Event) error {
	ctx, span := c.startSpan(ctx, "events.create")
	span.SetAttributes(attribute.String("event.type", event.Type), attribute.String("event.source", event.Source))
	defer span.End()
	_, err := c.pool.Exec(ctx, `
		INSERT INTO events (
			event_id,
			event_type,
			source,
			payload,
			status,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`, event.EventID, event.Type, event.Source, event.Payload, events.StatusReceived, event.CreatedAt)
	recordSpanError(span, err)
	return err
}

func (c *Client) MarkProcessing(ctx context.Context, eventID string) error {
	ctx, span := c.startSpan(ctx, "events.mark_processing")
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			attempt_count = attempt_count + 1,
			processing_started_at = NOW(),
			updated_at = NOW()
		WHERE event_id = $1
	`, eventID, events.StatusProcessing)
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) MarkProcessed(ctx context.Context, eventID string) error {
	ctx, span := c.startSpan(ctx, "events.mark_processed")
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			processed_at = NOW(),
			last_error = NULL,
			updated_at = NOW()
		WHERE event_id = $1
	`, eventID, events.StatusProcessed)
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) MarkFailed(ctx context.Context, eventID string, reason string) error {
	ctx, span := c.startSpan(ctx, "events.mark_failed")
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			last_error = $3,
			last_failed_at = NOW(),
			updated_at = NOW()
		WHERE event_id = $1
	`, eventID, events.StatusFailed, SanitizeLastError(reason))
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) MarkRetrying(ctx context.Context, eventID string, reason string) error {
	ctx, span := c.startSpan(ctx, "events.mark_retrying")
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			last_error = $3,
			first_failed_at = COALESCE(first_failed_at, NOW()),
			last_failed_at = NOW(),
			updated_at = NOW()
		WHERE event_id = $1
	`, eventID, events.StatusRetrying, SanitizeLastError(reason))
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) MarkDLQ(ctx context.Context, eventID string, reason string, metadata events.RetryMetadata) error {
	ctx, span := c.startSpan(ctx, "events.mark_dlq")
	span.SetAttributes(
		attribute.Int("eventmesh.attempt", metadata.Attempt),
		attribute.Int("eventmesh.retry_stage", metadata.RetryStage),
		attribute.String("messaging.destination.name", metadata.OriginalTopic),
	)
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			last_error = $3,
			first_failed_at = COALESCE(first_failed_at, NOW()),
			last_failed_at = NOW(),
			dlq_at = NOW(),
			original_topic = COALESCE(NULLIF($4, ''), original_topic),
			original_partition = COALESCE($5, original_partition),
			original_offset = COALESCE($6, original_offset),
			updated_at = NOW()
		WHERE event_id = $1
	`, eventID, events.StatusDLQ, SanitizeLastError(reason), metadata.OriginalTopic, metadata.OriginalPartition, metadata.OriginalOffset)
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) ListDLQ(ctx context.Context, limit int, offset int) ([]events.DLQRecord, error) {
	ctx, span := c.startSpan(ctx, "events.list_dlq")
	span.SetAttributes(attribute.Int("eventmesh.page_limit", limit), attribute.Int("eventmesh.page_offset", offset))
	defer span.End()
	rows, err := c.pool.Query(ctx, dlqSelectSQL+`
		WHERE status = $1
		ORDER BY COALESCE(dlq_at, updated_at) DESC, id DESC
		LIMIT $2 OFFSET $3
	`, events.StatusDLQ, limit, offset)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer rows.Close()

	records := []events.DLQRecord{}
	for rows.Next() {
		record, err := scanDLQRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	err = rows.Err()
	recordSpanError(span, err)
	return records, err
}

func (c *Client) GetDLQ(ctx context.Context, eventID string) (events.DLQRecord, error) {
	ctx, span := c.startSpan(ctx, "events.get_dlq")
	defer span.End()
	record, err := c.getDLQByStatus(ctx, eventID, events.StatusDLQ)
	if err != nil {
		recordSpanError(span, err)
		return events.DLQRecord{}, err
	}
	return record, nil
}

func (c *Client) BeginReplay(ctx context.Context, eventID string) (events.DLQRecord, error) {
	ctx, span := c.startSpan(ctx, "events.begin_replay")
	defer span.End()
	var record events.DLQRecord
	err := c.pool.QueryRow(ctx, `
		UPDATE events
		SET
			status = $2,
			updated_at = NOW()
		WHERE event_id = $1
		  AND status = $3
		RETURNING
			event_id,
			event_type,
			source,
			payload,
			status,
			attempt_count,
			COALESCE(last_error, ''),
			first_failed_at,
			last_failed_at,
			dlq_at,
			replayed_at,
			COALESCE(original_topic, ''),
			original_partition,
			original_offset,
			created_at,
			processing_started_at,
			processed_at,
			updated_at
	`, eventID, events.StatusReplayPending, events.StatusDLQ).Scan(
		&record.EventID,
		&record.Type,
		&record.Source,
		&record.Payload,
		&record.Status,
		&record.AttemptCount,
		&record.LastError,
		&record.FirstFailedAt,
		&record.LastFailedAt,
		&record.DLQAt,
		&record.ReplayedAt,
		&record.OriginalTopic,
		&record.OriginalPartition,
		&record.OriginalOffset,
		&record.CreatedAt,
		&record.ProcessingStartedAt,
		&record.ProcessedAt,
		&record.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		recordSpanError(span, events.ErrReplayNotAllowed)
		return events.DLQRecord{}, events.ErrReplayNotAllowed
	}
	if err != nil {
		recordSpanError(span, err)
		return events.DLQRecord{}, err
	}
	return record, nil
}

func (c *Client) MarkReplayPublished(ctx context.Context, eventID string) error {
	ctx, span := c.startSpan(ctx, "events.mark_replay_published")
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			replayed_at = NOW(),
			updated_at = NOW()
		WHERE event_id = $1
		  AND status = $3
	`, eventID, events.StatusReplayed, events.StatusReplayPending)
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) MarkReplayFailed(ctx context.Context, eventID string, reason string) error {
	ctx, span := c.startSpan(ctx, "events.mark_replay_failed")
	defer span.End()
	tag, err := c.pool.Exec(ctx, `
		UPDATE events
		SET
			status = $2,
			last_error = $3,
			updated_at = NOW()
		WHERE event_id = $1
		  AND status = $4
	`, eventID, events.StatusDLQ, SanitizeLastError(reason), events.StatusReplayPending)
	err = requireOneRow(tag.RowsAffected(), err)
	recordSpanError(span, err)
	return err
}

func (c *Client) GetByEventID(ctx context.Context, eventID string) (events.Record, error) {
	ctx, span := c.startSpan(ctx, "events.get")
	defer span.End()
	var record events.Record
	err := c.pool.QueryRow(ctx, `
		SELECT
			event_id,
			event_type,
			source,
			status,
			attempt_count,
			created_at,
			processing_started_at,
			processed_at,
			updated_at
		FROM events
		WHERE event_id = $1
	`, eventID).Scan(
		&record.EventID,
		&record.Type,
		&record.Source,
		&record.Status,
		&record.AttemptCount,
		&record.CreatedAt,
		&record.ProcessingStartedAt,
		&record.ProcessedAt,
		&record.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		recordSpanError(span, events.ErrEventNotFound)
		return events.Record{}, events.ErrEventNotFound
	}
	if err != nil {
		recordSpanError(span, err)
		return events.Record{}, err
	}
	return record, nil
}

func (c *Client) startSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return observability.Tracer().Start(ctx, name, trace.WithAttributes(attribute.String("db.system.name", "postgresql")))
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, "operation failed")
}

const dlqSelectSQL = `
	SELECT
		event_id,
		event_type,
		source,
		payload,
		status,
		attempt_count,
		COALESCE(last_error, ''),
		first_failed_at,
		last_failed_at,
		dlq_at,
		replayed_at,
		COALESCE(original_topic, ''),
		original_partition,
		original_offset,
		created_at,
		processing_started_at,
		processed_at,
		updated_at
	FROM events
`

type dlqScanner interface {
	Scan(dest ...any) error
}

func scanDLQRecord(scanner dlqScanner) (events.DLQRecord, error) {
	var record events.DLQRecord
	err := scanner.Scan(
		&record.EventID,
		&record.Type,
		&record.Source,
		&record.Payload,
		&record.Status,
		&record.AttemptCount,
		&record.LastError,
		&record.FirstFailedAt,
		&record.LastFailedAt,
		&record.DLQAt,
		&record.ReplayedAt,
		&record.OriginalTopic,
		&record.OriginalPartition,
		&record.OriginalOffset,
		&record.CreatedAt,
		&record.ProcessingStartedAt,
		&record.ProcessedAt,
		&record.UpdatedAt,
	)
	return record, err
}

func (c *Client) getDLQByStatus(ctx context.Context, eventID string, status events.Status) (events.DLQRecord, error) {
	record, err := scanDLQRecord(c.pool.QueryRow(ctx, dlqSelectSQL+`
		WHERE event_id = $1
		  AND status = $2
	`, eventID, status))
	if errors.Is(err, pgx.ErrNoRows) {
		return events.DLQRecord{}, events.ErrEventNotFound
	}
	if err != nil {
		return events.DLQRecord{}, err
	}
	return record, nil
}

func SanitizeLastError(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "event processing failed"
	}

	if utf8.RuneCountInString(reason) <= maxLastErrorRunes {
		return reason
	}

	runes := []rune(reason)
	return string(runes[:maxLastErrorRunes])
}

func requireOneRow(rowsAffected int64, err error) error {
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return events.ErrEventNotFound
	}
	return nil
}
