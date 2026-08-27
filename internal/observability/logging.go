package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

func LogAttrs(ctx context.Context) []any {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return nil
	}
	return []any{
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	}
}

func WithTrace(ctx context.Context, log *slog.Logger) *slog.Logger {
	if log == nil {
		log = slog.Default()
	}
	attrs := LogAttrs(ctx)
	if len(attrs) == 0 {
		return log
	}
	return log.With(attrs...)
}
