package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"eventmesh/internal/events"
	"eventmesh/internal/metrics"
	"eventmesh/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultDLQLimit = 20
	maxDLQLimit     = 100
)

type DLQHandler struct {
	repository  events.EventRepository
	replayer    events.EventReplayer
	idempotency events.IdempotencyStore
	metrics     *metrics.Recorder
	log         *slog.Logger
	now         func() time.Time
}

type dlqListResponse struct {
	Items  []dlqListItem `json:"items"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

type dlqListItem struct {
	EventID           string     `json:"event_id"`
	Type              string     `json:"type"`
	Source            string     `json:"source"`
	AttemptCount      int        `json:"attempt_count"`
	LastError         string     `json:"last_error,omitempty"`
	DLQAt             *time.Time `json:"dlq_at,omitempty"`
	OriginalTopic     string     `json:"original_topic,omitempty"`
	OriginalPartition *int       `json:"original_partition,omitempty"`
	OriginalOffset    *int64     `json:"original_offset,omitempty"`
}

func NewDLQHandler(repository events.EventRepository, replayer events.EventReplayer, idempotency events.IdempotencyStore, recorder *metrics.Recorder, log *slog.Logger) *DLQHandler {
	if recorder == nil {
		recorder = metrics.NewRecorder()
	}
	return &DLQHandler{repository: repository, replayer: replayer, idempotency: idempotency, metrics: recorder, log: log, now: time.Now}
}

func (h *DLQHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/admin/dlq")
	switch {
	case r.Method == http.MethodGet && path == "":
		h.list(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/"):
		h.get(w, r, strings.TrimPrefix(path, "/"))
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/") && strings.HasSuffix(path, "/replay"):
		eventID := strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/replay")
		h.replay(w, r, eventID)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (h *DLQHandler) list(w http.ResponseWriter, r *http.Request) {
	log := observability.WithTrace(r.Context(), h.log)
	limit, offset, ok := parsePagination(w, r)
	if !ok {
		return
	}
	records, err := h.repository.ListDLQ(r.Context(), limit, offset)
	if err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "dlq list failed")
		log.Error("dlq_list_failed", "request_id", RequestIDFromContext(r.Context()), "error", err)
		writeJSONError(w, http.StatusInternalServerError, "dlq_lookup_failed", "DLQ events could not be retrieved")
		return
	}

	items := make([]dlqListItem, 0, len(records))
	for _, record := range records {
		items = append(items, dlqListItem{
			EventID:           record.EventID,
			Type:              record.Type,
			Source:            record.Source,
			AttemptCount:      record.AttemptCount,
			LastError:         record.LastError,
			DLQAt:             record.DLQAt,
			OriginalTopic:     record.OriginalTopic,
			OriginalPartition: record.OriginalPartition,
			OriginalOffset:    record.OriginalOffset,
		})
	}

	writeJSON(w, http.StatusOK, dlqListResponse{Items: items, Limit: limit, Offset: offset})
}

func (h *DLQHandler) get(w http.ResponseWriter, r *http.Request, eventID string) {
	log := observability.WithTrace(r.Context(), h.log)
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || strings.Contains(eventID, "/") {
		writeJSONError(w, http.StatusNotFound, "dlq_event_not_found", "DLQ event not found")
		return
	}

	record, err := h.repository.GetDLQ(r.Context(), eventID)
	if errors.Is(err, events.ErrEventNotFound) {
		writeJSONError(w, http.StatusNotFound, "dlq_event_not_found", "DLQ event not found")
		return
	}
	if err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "dlq get failed")
		log.Error("dlq_get_failed", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "dlq_lookup_failed", "DLQ event could not be retrieved")
		return
	}

	trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("event.type", record.Type), attribute.String("event.source", record.Source))
	log.Info("dlq_event_viewed", "request_id", RequestIDFromContext(r.Context()), "event_id", record.EventID, "event_type", record.Type)
	writeJSON(w, http.StatusOK, record)
}

func (h *DLQHandler) replay(w http.ResponseWriter, r *http.Request, eventID string) {
	log := observability.WithTrace(r.Context(), h.log)
	eventID = strings.TrimSpace(eventID)
	h.metrics.RecordDLQReplayRequested()
	trace.SpanFromContext(r.Context()).SetAttributes(attribute.Bool("eventmesh.replayed", true))
	log.Info("dlq_replay_requested", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID)
	if eventID == "" || strings.Contains(eventID, "/") {
		h.rejectReplay(w, r, eventID, "invalid_event_id", "DLQ event not found")
		return
	}

	record, err := h.repository.GetByEventID(r.Context(), eventID)
	if errors.Is(err, events.ErrEventNotFound) {
		writeJSONError(w, http.StatusNotFound, "dlq_event_not_found", "DLQ event not found")
		return
	}
	if err != nil {
		h.metrics.RecordDLQReplayFailed()
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "event lookup failed")
		log.Error("dlq_replay_failed", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "dlq_replay_failed", "DLQ event could not be replayed")
		return
	}
	if record.Status != events.StatusDLQ {
		h.rejectReplay(w, r, eventID, "dlq_replay_conflict", "event is not in DLQ status")
		return
	}

	state, err := h.idempotency.State(r.Context(), eventID)
	if err != nil {
		h.metrics.RecordDLQReplayFailed()
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "idempotency unavailable")
		log.Error("dlq_replay_failed", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "error", err)
		writeJSONError(w, http.StatusServiceUnavailable, "idempotency_unavailable", "DLQ event could not be replayed")
		return
	}
	if state == events.IdempotencyStateCompleted {
		h.rejectReplay(w, r, eventID, "dlq_replay_conflict", "event is marked completed in idempotency store")
		return
	}
	if state == events.IdempotencyStateProcessing {
		h.rejectReplay(w, r, eventID, "dlq_replay_conflict", "event is currently processing")
		return
	}

	dlqRecord, err := h.repository.BeginReplay(r.Context(), eventID)
	if errors.Is(err, events.ErrReplayNotAllowed) {
		h.rejectReplay(w, r, eventID, "dlq_replay_conflict", "event is already replaying or not in DLQ status")
		return
	}
	if err != nil {
		h.metrics.RecordDLQReplayFailed()
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "begin replay failed")
		log.Error("dlq_replay_failed", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "dlq_replay_failed", "DLQ event could not be replayed")
		return
	}
	trace.SpanFromContext(r.Context()).SetAttributes(
		attribute.String("event.type", dlqRecord.Type),
		attribute.String("event.source", dlqRecord.Source),
		attribute.Int("eventmesh.original_attempt_count", dlqRecord.AttemptCount),
	)

	event := events.Event{
		EventID:   dlqRecord.EventID,
		Type:      dlqRecord.Type,
		Source:    dlqRecord.Source,
		Payload:   dlqRecord.Payload,
		CreatedAt: dlqRecord.CreatedAt,
	}
	replayAt := h.now().UTC()
	if err := h.replayer.Replay(r.Context(), event, events.ReplayMetadata{ReplayAt: replayAt, OriginalAttemptCount: dlqRecord.AttemptCount}); err != nil {
		h.metrics.RecordDLQReplayFailed()
		_ = h.repository.MarkReplayFailed(context.WithoutCancel(r.Context()), eventID, "kafka replay publish failed")
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "kafka replay publish failed")
		log.Error("dlq_replay_failed", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "event_type", dlqRecord.Type, "error", err)
		writeJSONError(w, http.StatusServiceUnavailable, "dlq_replay_publish_failed", "DLQ event could not be replayed")
		return
	}

	if err := h.repository.MarkReplayPublished(r.Context(), eventID); err != nil {
		h.metrics.RecordDLQReplayFailed()
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "replay state update failed")
		log.Error("dlq_replay_failed", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "event_type", dlqRecord.Type, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "dlq_replay_state_update_failed", "DLQ event was published but state could not be updated")
		return
	}

	h.metrics.RecordDLQReplaySuccess()
	log.Info("dlq_replay_published", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "event_type", dlqRecord.Type, "previous_status", events.StatusDLQ, "new_status", events.StatusReplayed)
	writeJSON(w, http.StatusAccepted, map[string]string{"event_id": eventID, "status": "replay_accepted"})
}

func (h *DLQHandler) rejectReplay(w http.ResponseWriter, r *http.Request, eventID string, code string, message string) {
	h.metrics.RecordDLQReplayRejected()
	trace.SpanFromContext(r.Context()).AddEvent("dlq_replay_rejected", trace.WithAttributes(attribute.String("eventmesh.replay_rejection", code)))
	observability.WithTrace(r.Context(), h.log).Info("dlq_replay_rejected", "request_id", RequestIDFromContext(r.Context()), "event_id", eventID, "error", code)
	status := http.StatusConflict
	if code == "invalid_event_id" {
		status = http.StatusNotFound
	}
	writeJSONError(w, status, code, message)
}

func parsePagination(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	limit := defaultDLQLimit
	offset := 0

	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxDLQLimit {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100")
			return 0, 0, false
		}
		limit = parsed
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "offset must be greater than or equal to 0")
			return 0, 0, false
		}
		offset = parsed
	}
	return limit, offset, true
}
