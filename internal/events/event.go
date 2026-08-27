package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type contextKey string

const metadataKey contextKey = "event_metadata"

type Event struct {
	EventID   string          `json:"event_id"`
	Type      string          `json:"type"`
	Source    string          `json:"source"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type Status string

const (
	StatusReceived      Status = "RECEIVED"
	StatusProcessing    Status = "PROCESSING"
	StatusProcessed     Status = "PROCESSED"
	StatusFailed        Status = "FAILED"
	StatusRetrying      Status = "RETRYING"
	StatusDLQ           Status = "DLQ"
	StatusReplayPending Status = "REPLAY_PENDING"
	StatusReplayed      Status = "REPLAYED"
)

var ErrEventNotFound = errors.New("event not found")
var ErrReplayNotAllowed = errors.New("event replay is not allowed")

type EventPublisher interface {
	Publish(ctx context.Context, event Event) error
}

type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

type EventRepository interface {
	Create(ctx context.Context, event Event) error
	MarkProcessing(ctx context.Context, eventID string) error
	MarkProcessed(ctx context.Context, eventID string) error
	MarkFailed(ctx context.Context, eventID string, reason string) error
	MarkRetrying(ctx context.Context, eventID string, reason string) error
	MarkDLQ(ctx context.Context, eventID string, reason string, metadata RetryMetadata) error
	ListDLQ(ctx context.Context, limit int, offset int) ([]DLQRecord, error)
	GetDLQ(ctx context.Context, eventID string) (DLQRecord, error)
	BeginReplay(ctx context.Context, eventID string) (DLQRecord, error)
	MarkReplayPublished(ctx context.Context, eventID string) error
	MarkReplayFailed(ctx context.Context, eventID string, reason string) error
	GetByEventID(ctx context.Context, eventID string) (Record, error)
}

type FailureKind string

const (
	FailureRetryable    FailureKind = "RETRYABLE"
	FailureNonRetryable FailureKind = "NON_RETRYABLE"
)

type ClassifiedError struct {
	Kind FailureKind
	Err  error
}

func (e ClassifiedError) Error() string {
	if e.Err == nil {
		return string(e.Kind)
	}
	return e.Err.Error()
}

func (e ClassifiedError) Unwrap() error {
	return e.Err
}

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return ClassifiedError{Kind: FailureRetryable, Err: err}
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return ClassifiedError{Kind: FailureNonRetryable, Err: err}
}

func ClassifyFailure(err error) FailureKind {
	var classified ClassifiedError
	if errors.As(err, &classified) {
		return classified.Kind
	}
	return FailureRetryable
}

type RetryMetadata struct {
	Attempt           int
	RetryStage        int
	OriginalTopic     string
	OriginalPartition int
	OriginalOffset    int64
	FirstFailedAt     time.Time
	LastFailedAt      time.Time
	LastError         string
	NotBefore         time.Time
}

type FailurePublisher interface {
	PublishRetry(ctx context.Context, event Event, metadata RetryMetadata) error
	PublishDLQ(ctx context.Context, message DeadLetterMessage) error
}

type DeadLetterMessage struct {
	Event      *Event        `json:"event,omitempty"`
	RawPayload []byte        `json:"raw_payload,omitempty"`
	Failure    FailureDetail `json:"failure"`
	Metadata   RetryMetadata `json:"metadata"`
}

type EventReplayer interface {
	Replay(ctx context.Context, event Event, metadata ReplayMetadata) error
}

type ReplayMetadata struct {
	ReplayAt             time.Time
	OriginalAttemptCount int
}

type FailureDetail struct {
	Kind   FailureKind `json:"kind"`
	Reason string      `json:"reason"`
}

type IdempotencyAcquireResult string

const (
	IdempotencyAcquired          IdempotencyAcquireResult = "ACQUIRED"
	IdempotencyCompleted         IdempotencyAcquireResult = "COMPLETED"
	IdempotencyAlreadyProcessing IdempotencyAcquireResult = "ALREADY_PROCESSING"
)

type IdempotencyState string

const (
	IdempotencyStateAbsent     IdempotencyState = "ABSENT"
	IdempotencyStateProcessing IdempotencyState = "PROCESSING"
	IdempotencyStateCompleted  IdempotencyState = "COMPLETED"
)

type IdempotencyStore interface {
	Acquire(ctx context.Context, eventID string) (IdempotencyAcquireResult, error)
	MarkCompleted(ctx context.Context, eventID string) error
	Release(ctx context.Context, eventID string) error
	IsCompleted(ctx context.Context, eventID string) (bool, error)
	State(ctx context.Context, eventID string) (IdempotencyState, error)
}

type Record struct {
	EventID             string     `json:"event_id"`
	Type                string     `json:"type"`
	Source              string     `json:"source"`
	Status              Status     `json:"status"`
	AttemptCount        int        `json:"attempt_count"`
	CreatedAt           time.Time  `json:"created_at"`
	ProcessingStartedAt *time.Time `json:"processing_started_at,omitempty"`
	ProcessedAt         *time.Time `json:"processed_at,omitempty"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

type DLQRecord struct {
	EventID             string          `json:"event_id"`
	Type                string          `json:"type"`
	Source              string          `json:"source"`
	Payload             json.RawMessage `json:"payload,omitempty"`
	Status              Status          `json:"status"`
	AttemptCount        int             `json:"attempt_count"`
	LastError           string          `json:"last_error,omitempty"`
	FirstFailedAt       *time.Time      `json:"first_failed_at,omitempty"`
	LastFailedAt        *time.Time      `json:"last_failed_at,omitempty"`
	DLQAt               *time.Time      `json:"dlq_at,omitempty"`
	ReplayedAt          *time.Time      `json:"replayed_at,omitempty"`
	OriginalTopic       string          `json:"original_topic,omitempty"`
	OriginalPartition   *int            `json:"original_partition,omitempty"`
	OriginalOffset      *int64          `json:"original_offset,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	ProcessingStartedAt *time.Time      `json:"processing_started_at,omitempty"`
	ProcessedAt         *time.Time      `json:"processed_at,omitempty"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

type Metadata struct {
	Topic     string
	Partition int
	Offset    int64
}

func ContextWithMetadata(ctx context.Context, metadata Metadata) context.Context {
	return context.WithValue(ctx, metadataKey, metadata)
}

func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	metadata, ok := ctx.Value(metadataKey).(Metadata)
	return metadata, ok
}

const retryMetadataKey contextKey = "retry_metadata"

func ContextWithRetryMetadata(ctx context.Context, metadata RetryMetadata) context.Context {
	return context.WithValue(ctx, retryMetadataKey, metadata)
}

func RetryMetadataFromContext(ctx context.Context) (RetryMetadata, bool) {
	metadata, ok := ctx.Value(retryMetadataKey).(RetryMetadata)
	return metadata, ok
}

func ValidateRetryMetadata(metadata RetryMetadata) error {
	if metadata.Attempt < 1 {
		return fmt.Errorf("attempt must be positive")
	}
	if metadata.RetryStage < 0 {
		return fmt.Errorf("retry stage cannot be negative")
	}
	if metadata.OriginalTopic == "" {
		return fmt.Errorf("original topic is required")
	}
	if metadata.FirstFailedAt.IsZero() {
		return fmt.Errorf("first_failed_at is required")
	}
	if metadata.LastFailedAt.IsZero() {
		return fmt.Errorf("last_failed_at is required")
	}
	return nil
}

func NewEventID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])

	return string(dst), nil
}
