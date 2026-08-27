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
	"eventmesh/internal/httpapi"
	"eventmesh/internal/kafka"
	"eventmesh/internal/logger"
	"eventmesh/internal/metrics"
	"eventmesh/internal/observability"
	"eventmesh/internal/postgres"
	"eventmesh/internal/redis"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tracingShutdown, err := observability.Init(ctx, cfg.Tracing, "eventmesh-api")
	if err != nil {
		log.Error("tracing initialization failed", "error", err)
		os.Exit(1)
	}
	defer shutdownTracing(tracingShutdown, cfg.ShutdownTimeout, log)

	kafkaProducer := kafka.NewProducer(cfg.Kafka, log)
	postgresClient, err := postgres.NewClient(ctx, cfg.Postgres, log)
	if err != nil {
		log.Error("postgres client initialization failed", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(cfg.Redis, log)

	defer kafkaProducer.Close()
	defer postgresClient.Close()
	defer redisClient.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := postgresClient.Ping(r.Context()); err != nil {
			log.Error("readiness check failed", "dependency", "postgres", "error", err)
			writeServiceUnavailable(w)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("/metrics", metrics.Handler())
	eventsHandler := httpapi.NewEventsHandler(kafkaProducer, postgresClient, log, cfg.Kafka.ProducerTimeout)
	mux.Handle("/v1/events", eventsHandler)
	mux.Handle("/v1/events/", eventsHandler)
	dlqHandler := httpapi.NewDLQHandler(postgresClient, kafkaProducer, redisClient, metrics.NewRecorder(), log)
	mux.Handle("/v1/admin/dlq", dlqHandler)
	mux.Handle("/v1/admin/dlq/", dlqHandler)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.RequestIDMiddleware(observability.HTTPMiddleware(mux)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("api service starting", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("api service failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("api service shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("api service shutdown failed", "error", err)
		os.Exit(1)
	}

	log.Info("api service stopped")
}

func shutdownTracing(shutdown observability.ShutdownFunc, timeout time.Duration, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Error("tracing shutdown failed", "error", err)
	}
}

func writeServiceUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":"service_unavailable","message":"service is not ready"}` + "\n"))
}
