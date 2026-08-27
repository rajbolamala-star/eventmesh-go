package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"eventmesh/internal/events"
	"eventmesh/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const maxEventRequestBytes = 1 << 20

type EventsHandler struct {
	publisher      events.EventPublisher
	repository     events.EventRepository
	log            *slog.Logger
	publishTimeout time.Duration
	now            func() time.Time
	newEventID     func() (string, error)
}

type EventRequest struct {
	Type    string          `json:"type"`
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload"`
}

func NewEventsHandler(publisher events.EventPublisher, repository events.EventRepository, log *slog.Logger, publishTimeout time.Duration) *EventsHandler {
	return &EventsHandler{
		publisher:      publisher,
		repository:     repository,
		log:            log,
		publishTimeout: publishTimeout,
		now:            time.Now,
		newEventID:     events.NewEventID,
	}
}

func (h *EventsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/events":
		h.createEvent(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/events/"):
		h.getEvent(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (h *EventsHandler) createEvent(w http.ResponseWriter, r *http.Request) {
	log := observability.WithTrace(r.Context(), h.log)
	var req EventRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEventRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "invalid request")
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid JSON request body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "invalid request")
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid JSON request body")
		return
	}

	req.Type = strings.TrimSpace(req.Type)
	req.Source = strings.TrimSpace(req.Source)
	trace.SpanFromContext(r.Context()).SetAttributes(
		attribute.String("event.type", req.Type),
		attribute.String("event.source", req.Source),
	)
	if req.Type == "" {
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "missing type")
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "type is required")
		return
	}
	if req.Source == "" {
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "missing source")
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "source is required")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	eventID, err := h.newEventID()
	if err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "event id generation failed")
		log.Error("event id generation failed", "request_id", RequestIDFromContext(r.Context()), "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal_error", "event could not be accepted")
		return
	}

	event := events.Event{
		EventID:   eventID,
		Type:      req.Type,
		Source:    req.Source,
		Payload:   req.Payload,
		CreatedAt: h.now().UTC(),
	}

	if err := h.repository.Create(r.Context(), event); err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "event persistence failed")
		log.Error(
			"event persistence failed",
			"request_id", RequestIDFromContext(r.Context()),
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"error", err,
		)
		writeJSONError(w, http.StatusInternalServerError, "event_persistence_failed", "event could not be accepted")
		return
	}

	publishCtx, cancel := context.WithTimeout(r.Context(), h.publishTimeout)
	defer cancel()

	if err := h.publisher.Publish(publishCtx, event); err != nil {
		if markErr := h.repository.MarkFailed(r.Context(), event.EventID, "kafka publish failed"); markErr != nil {
			log.Error(
				"event publish failure state update failed",
				"request_id", RequestIDFromContext(r.Context()),
				"event_id", event.EventID,
				"event_type", event.Type,
				"source", event.Source,
				"error", markErr,
			)
		}
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "event publish failed")
		log.Error(
			"event publish failed",
			"request_id", RequestIDFromContext(r.Context()),
			"event_id", event.EventID,
			"event_type", event.Type,
			"source", event.Source,
			"publish_result", "failure",
			"error", err,
		)
		writeJSONError(w, http.StatusServiceUnavailable, "event_publish_failed", "event could not be accepted")
		return
	}

	log.Info(
		"event accepted",
		"request_id", RequestIDFromContext(r.Context()),
		"event_id", event.EventID,
		"event_type", event.Type,
		"source", event.Source,
		"publish_result", "success",
	)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"event_id": event.EventID,
		"status":   "accepted",
	})
}

func (h *EventsHandler) getEvent(w http.ResponseWriter, r *http.Request) {
	log := observability.WithTrace(r.Context(), h.log)
	eventID := strings.TrimPrefix(r.URL.Path, "/v1/events/")
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || strings.Contains(eventID, "/") {
		writeJSONError(w, http.StatusNotFound, "event_not_found", "event not found")
		return
	}

	record, err := h.repository.GetByEventID(r.Context(), eventID)
	if errors.Is(err, events.ErrEventNotFound) {
		writeJSONError(w, http.StatusNotFound, "event_not_found", "event not found")
		return
	}
	if err != nil {
		trace.SpanFromContext(r.Context()).RecordError(err)
		trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "event lookup failed")
		log.Error(
			"event lookup failed",
			"request_id", RequestIDFromContext(r.Context()),
			"event_id", eventID,
			"error", err,
		)
		writeJSONError(w, http.StatusInternalServerError, "event_lookup_failed", "event could not be retrieved")
		return
	}

	writeJSON(w, http.StatusOK, record)
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
