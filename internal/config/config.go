package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env               string
	HTTPAddr          string
	WorkerMetricsAddr string
	LogLevel          string
	ShutdownTimeout   time.Duration
	Kafka             KafkaConfig
	Postgres          PostgresConfig
	Redis             RedisConfig
	Tracing           TracingConfig
}

type KafkaConfig struct {
	Brokers               []string
	Topic                 string
	RetryTopics           []string
	DLQTopic              string
	RetryDelays           []time.Duration
	ConsumerGroup         string
	ProducerTimeout       time.Duration
	ConsumerMinBytes      int
	ConsumerMaxBytes      int
	ConsumerMaxWait       time.Duration
	ProcessingConcurrency int
}

type PostgresConfig struct {
	URL            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
}

type RedisConfig struct {
	Addr                     string
	Password                 string
	DB                       int
	ConnectTimeout           time.Duration
	OperationTimeout         time.Duration
	IdempotencyProcessingTTL time.Duration
	IdempotencyCompletedTTL  time.Duration
}

type TracingConfig struct {
	Enabled      bool
	ServiceName  string
	OTLPEndpoint string
	Environment  string
	SampleRatio  float64
}

func Load() Config {
	return Config{
		Env:               getEnv("APP_ENV", "local"),
		HTTPAddr:          getEnv("HTTP_ADDR", ":8080"),
		WorkerMetricsAddr: getEnv("WORKER_METRICS_ADDR", ":9091"),
		LogLevel:          getEnv("LOG_LEVEL", "info"),
		ShutdownTimeout:   getDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
		Kafka: KafkaConfig{
			Brokers:               splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
			Topic:                 getEnv("KAFKA_TOPIC", "eventmesh.events"),
			RetryTopics:           splitCSV(getEnv("KAFKA_RETRY_TOPICS", "eventmesh.retry.1,eventmesh.retry.2,eventmesh.retry.3")),
			DLQTopic:              getEnv("KAFKA_DLQ_TOPIC", "eventmesh.dlq"),
			RetryDelays:           getDurations("KAFKA_RETRY_DELAYS", []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}),
			ConsumerGroup:         getEnv("KAFKA_CONSUMER_GROUP", "eventmesh-processors"),
			ProducerTimeout:       getDuration("KAFKA_PRODUCER_TIMEOUT", 5*time.Second),
			ConsumerMinBytes:      getInt("KAFKA_CONSUMER_MIN_BYTES", 1),
			ConsumerMaxBytes:      getInt("KAFKA_CONSUMER_MAX_BYTES", 10e6),
			ConsumerMaxWait:       getDuration("KAFKA_CONSUMER_MAX_WAIT", 1*time.Second),
			ProcessingConcurrency: getInt("KAFKA_PROCESSING_CONCURRENCY", 1),
		},
		Postgres: PostgresConfig{
			URL:            getEnv("POSTGRES_URL", getEnv("POSTGRES_DSN", "postgres://eventmesh:eventmesh@localhost:5432/eventmesh?sslmode=disable")),
			MaxConns:       int32(getInt("POSTGRES_MAX_CONNS", 10)),
			MinConns:       int32(getInt("POSTGRES_MIN_CONNS", 1)),
			ConnectTimeout: getDuration("POSTGRES_CONNECT_TIMEOUT", 5*time.Second),
		},
		Redis: RedisConfig{
			Addr:                     getEnv("REDIS_ADDR", "localhost:6379"),
			Password:                 os.Getenv("REDIS_PASSWORD"),
			DB:                       getInt("REDIS_DB", 0),
			ConnectTimeout:           getDuration("REDIS_CONNECT_TIMEOUT", 5*time.Second),
			OperationTimeout:         getDuration("REDIS_OPERATION_TIMEOUT", 2*time.Second),
			IdempotencyProcessingTTL: getDuration("IDEMPOTENCY_PROCESSING_TTL", 5*time.Minute),
			IdempotencyCompletedTTL:  getDuration("IDEMPOTENCY_COMPLETED_TTL", 24*time.Hour),
		},
		Tracing: TracingConfig{
			Enabled:      getBool("OTEL_TRACES_ENABLED", false),
			ServiceName:  getEnv("OTEL_SERVICE_NAME", ""),
			OTLPEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces"),
			Environment:  getEnv("APP_ENV", "local"),
			SampleRatio:  getFloat("OTEL_TRACE_SAMPLE_RATIO", 1),
		},
	}
}

func getDurations(key string, fallback []time.Duration) []time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parts := splitCSV(value)
	durations := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		duration, err := time.ParseDuration(part)
		if err != nil {
			return fallback
		}
		durations = append(durations, duration)
	}
	if len(durations) == 0 {
		return fallback
	}
	return durations
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}

func getInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "true", "1", "yes", "y", "on":
		return true
	case "false", "0", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func getFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
