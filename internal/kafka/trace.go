package kafka

import (
	"context"
	"strings"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

type kafkaHeaderCarrier struct {
	headers *[]kafkago.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, header := range *c.headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key string, value string) {
	for i, header := range *c.headers {
		if strings.EqualFold(header.Key, key) {
			(*c.headers)[i].Key = key
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafkago.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

func injectTraceHeaders(ctx context.Context, headers []kafkago.Header) []kafkago.Header {
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: &headers})
	return headers
}

func extractTraceContext(ctx context.Context, headers []kafkago.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &headers})
}
