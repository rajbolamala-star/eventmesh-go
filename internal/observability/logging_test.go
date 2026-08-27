package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"eventmesh/internal/config"

	"go.opentelemetry.io/otel/trace"
)

func TestLogAttrsExposeTraceAndSpanIDs(t *testing.T) {
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	WithTrace(ctx, log).Info("hello")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("decode log entry: %v", err)
	}
	if entry["trace_id"] != traceID.String() {
		t.Fatalf("expected trace_id %q, got %v", traceID.String(), entry["trace_id"])
	}
	if entry["span_id"] != spanID.String() {
		t.Fatalf("expected span_id %q, got %v", spanID.String(), entry["span_id"])
	}
}

func TestInitDisabledWorksAndShutdownIsSafe(t *testing.T) {
	shutdown, err := Init(context.Background(), config.TracingConfig{
		Enabled:      false,
		ServiceName:  "eventmesh-test",
		OTLPEndpoint: "http://localhost:4318/v1/traces",
		Environment:  "test",
	}, "eventmesh-test")
	if err != nil {
		t.Fatalf("init disabled tracing: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown disabled tracing: %v", err)
	}
}

func TestClampSampleRatio(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  float64
	}{
		{name: "below zero", value: -1, want: 0},
		{name: "within range", value: 0.25, want: 0.25},
		{name: "above one", value: 2, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clampSampleRatio(tt.value); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}
