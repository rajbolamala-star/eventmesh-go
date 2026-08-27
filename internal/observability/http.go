package observability

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := routeName(r)
		ctx, span := Tracer().Start(
			r.Context(),
			"HTTP "+r.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("http.route", route),
			),
		)
		defer span.End()

		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r.WithContext(ctx))
		span.SetAttributes(attribute.Int("http.response.status_code", recorder.status))
		if recorder.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(recorder.status))
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func routeName(r *http.Request) string {
	path := strings.TrimRight(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}
	switch {
	case path == "/healthz", path == "/readyz", path == "/metrics":
		return path
	case r.Method == http.MethodPost && path == "/v1/events":
		return "/v1/events"
	case strings.HasPrefix(path, "/v1/events/"):
		return "/v1/events/{event_id}"
	case path == "/v1/admin/dlq":
		return "/v1/admin/dlq"
	case strings.HasPrefix(path, "/v1/admin/dlq/") && strings.HasSuffix(path, "/replay"):
		return "/v1/admin/dlq/{event_id}/replay"
	case strings.HasPrefix(path, "/v1/admin/dlq/"):
		return "/v1/admin/dlq/{event_id}"
	default:
		return "unknown"
	}
}
