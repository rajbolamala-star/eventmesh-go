package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"eventmesh/internal/config"
	"eventmesh/internal/kafka"
	"eventmesh/internal/logger"
	"eventmesh/internal/metrics"
	"eventmesh/internal/observability"
	"eventmesh/internal/postgres"
	"eventmesh/internal/redis"
	"eventmesh/internal/workers"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tracingShutdown, err := observability.Init(ctx, cfg.Tracing, "eventmesh-worker")
	if err != nil {
		log.Error("tracing initialization failed", "error", err)
		os.Exit(1)
	}
	defer shutdownTracing(tracingShutdown, cfg.ShutdownTimeout, log)

	failurePublisher := kafka.NewFailureProducer(cfg.Kafka, log)
	consumerGroup := kafka.NewConsumerGroup(cfg.Kafka, log, failurePublisher)
	postgresClient, err := postgres.NewClient(ctx, cfg.Postgres, log)
	if err != nil {
		log.Error("postgres client initialization failed", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(cfg.Redis, log)
	worker := workers.NewEventWorker(postgresClient, redisClient, failurePublisher, cfg.Kafka.RetryDelays, metrics.NewRecorder(), log)
	metricsServer := startMetricsServer(cfg.WorkerMetricsAddr, log)

	defer consumerGroup.Close()
	defer failurePublisher.Close()
	defer postgresClient.Close()
	defer redisClient.Close()
	defer shutdownMetricsServer(metricsServer, cfg.ShutdownTimeout, log)

	log.Info("worker service starting", "group_id", cfg.Kafka.ConsumerGroup, "topic", cfg.Kafka.Topic)
	if err := consumerGroup.Run(ctx, worker); err != nil {
		log.Error("worker service failed", "error", err)
		os.Exit(1)
	}

	log.Info("worker service stopped")
}

func startMetricsServer(addr string, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", metrics.Handler())

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("worker metrics server starting", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("worker metrics server failed", "error", err)
		}
	}()

	return server
}

func shutdownMetricsServer(server *http.Server, timeout time.Duration, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Error("worker metrics server shutdown failed", "error", err)
	}
}

func shutdownTracing(shutdown observability.ShutdownFunc, timeout time.Duration, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Error("tracing shutdown failed", "error", err)
	}
}
